#lang racket/base

(require racket/contract/base
         racket/format
         racket/match
         racket/port
         "private/batch.rkt"
         "private/client.rkt"
         "private/common.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (contract-out
  [producer? (-> any/c boolean?)]
  [make-producer (->* (client?)
                      (#:acks (or/c 'none 'leader 'full)
                       #:compression (or/c 'none 'gzip)
                       #:flush-interval exact-positive-integer?
                       #:max-batch-bytes exact-positive-integer?
                       #:max-batch-size exact-positive-integer?)
                      producer?)]
  [produce (->* (producer? string? (or/c #f bytes?) (or/c #f bytes?))
                (#:partition exact-nonnegative-integer?
                 #:headers (hash/c string? (or/c #f bytes?)))
                evt?)]
  [producer-flush (-> producer? void?)]
  [producer-stop (-> producer? void?)]))

(module+ unsafe
  (provide
   producer?
   make-producer
   produce
   producer-flush
   producer-stop))

(define-logger kafka-producer)

(struct producer (ch batcher)
  #:transparent)

(define (make-producer
         client
         #:acks [acks 'leader]
         #:compression [compression 'gzip]
         #:flush-interval [flush-interval-ms 60000]
         #:max-batch-bytes [max-batch-bytes (* 100 1024 1024)]
         #:max-batch-size [max-batch-size 1000])
  (define ch (make-channel))
  (define batcher
    (thread/suspend-to-kill
     (lambda ()
       (define st (make-state (λ () (make-batch #:compression compression))))
       (define (process-message msg)
         (cond
           [(state-stopped? st)
            (match-define `(,_ ,nack ,res-ch ,_ ...) msg)
            (add-state-reqs! st (FailReq nack res-ch (client-error "stop in progress")))]

           [else
            (match msg
              [`(produce ,nack ,res-ch ,topic ,pid ,key ,value ,headers)
               (add-state-message! st topic pid key value headers)
               (define-values (fut fut-evt)
                 (make-Future topic pid))
               (add-state-req! st (ProduceReq nack res-ch fut-evt))
               (add-state-fut! st fut)]

              [`(stop ,nack ,res-ch)
               (add-state-pending-req! st (StopReq nack res-ch))
               (set-state-force-flush?! st #t)
               (set-state-stopped?! st #t)]

              [`(flush ,nack ,res-ch)
               (add-state-pending-req! st (FlushReq nack res-ch))
               (set-state-force-flush?! st #t)]

              [msg
               (log-kafka-producer-error "invalid message: ~e" msg)])]))
       (add-state-evt! st (handle-evt ch process-message))
       (reset-state-deadline-evt! st flush-interval-ms)

       (let loop ()
         (cond
           [(or (state-force-flush? st)
                (>= (state-pending-bytes st) max-batch-bytes)
                (>= (state-pending-count st) max-batch-size))
            (log-kafka-producer-debug
             "flushing ~a messages (~a bytes)"
             (state-pending-count st)
             (state-pending-bytes st))
            (define start-time (current-inexact-monotonic-milliseconds))
            (define batches (pop-state-batches! st))
            (define pending-reqs (pop-state-pending-reqs! st))
            (define pending-futs (pop-state-pending-futs! st))
            (with-handlers ([exn:fail?
                             (lambda (err)
                               (for ([fut (in-list pending-futs)])
                                 (resolve fut err)))])
              (define evts
                (make-produce-evts
                 client batches
                 #:acks acks
                 #:timeout-ms 30000))
              (define results-by-topic&pid
                (for*/hash ([evt (in-list evts)]
                            [res (in-value (sync evt))]
                            [t (in-list (ProduceResponse-topics res))]
                            [p (in-list (ProduceResponseTopic-partitions t))])
                  (define topic (ProduceResponseTopic-name t))
                  (define pid (ProduceResponsePartition-id p))
                  (define topic&pid (cons topic pid))
                  (define error-code (ProduceResponsePartition-error-code p))
                  (values topic&pid (if (zero? error-code)
                                        (make-RecordResult
                                         #:topic topic
                                         #:partition p)
                                        (server-error error-code)))))
              (for ([fut (in-list pending-futs)])
                (define topic (Future-topic fut))
                (define pid (Future-pid fut))
                (define topic&pid (cons topic pid))
                (define partition-res (hash-ref results-by-topic&pid topic&pid #f))
                (define produce-res
                  (or partition-res
                      (make-RecordResult
                       #:topic topic
                       #:partition (make-ProduceResponsePartition
                                    #:id pid
                                    #:error-code 0
                                    #:offset -1))))
                (resolve fut produce-res)))
            (log-kafka-producer-debug
             "flush duration: ~a"
             (~ms (- (current-inexact-monotonic-milliseconds) start-time)))
            (add-state-reqs! st pending-reqs)
            (set-state-force-flush?! st #f)
            (reset-state-deadline-evt! st flush-interval-ms)
            (loop)]

           [else
            (apply sync (state-evts st))
            (unless (and (state-stopped? st)
                         (not (state-force-flush? st))
                         (null? (state-pending-reqs st)))
              (loop))])))))
  (producer ch batcher))

(define (produce p topic key value
                 #:partition [pid 0]
                 #:headers [headers (hash)])
  (sync (make-producer-evt p 'produce topic pid key value headers)))

(define (producer-flush p)
  (sync (make-producer-evt p 'flush)))

(define (producer-stop p)
  (sync (make-producer-evt p 'stop)))

(define (make-producer-evt p command . args)
  (define ch (producer-ch p))
  (define thd (producer-batcher p))
  (define res-ch (make-channel))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume thd (current-thread))
      (begin0 res-ch
        (channel-put ch (list* command nack res-ch args)))))
   (lambda (res-or-exn)
     (begin0 res-or-exn
       (when (exn:fail? res-or-exn)
         (raise res-or-exn))))))


;; State ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct state
  (stopped?
   force-flush?
   deadline-evt
   pending-reqs
   pending-futs
   evts
   batch-proc
   batches
   pending-bytes
   pending-count)
  #:transparent
  #:mutable)

(define (make-state make-batch-proc)
  (state #f #f #f null null null make-batch-proc (make-hash) 0 0))

(define (add-state-pending-req! st req)
  (set-state-pending-reqs! st (cons req (state-pending-reqs st))))

(define (pop-state-pending-reqs! st)
  (define pending-reqs (state-pending-reqs st))
  (begin0 pending-reqs
    (set-state-pending-reqs! st null)))

(define (add-state-req! st req)
  (define nack-evt
    (handle-evt
     (Req-nack req)
     (lambda (_)
       (remove-state-evt! st nack-evt)
       (remove-state-evt! st req-evt))))
  (define req-evt
    (handle-evt
     (match req
       [(ProduceReq _ res-ch evt) (channel-put-evt res-ch evt)]
       [(FlushReq   _ res-ch)     (channel-put-evt res-ch (void))]
       [(StopReq    _ res-ch)     (channel-put-evt res-ch (void))]
       [(FailReq    _ res-ch err) (channel-put-evt res-ch err)])
     (lambda (_)
       (remove-state-evt! st nack-evt)
       (remove-state-evt! st req-evt))))
  (set-state-evts! st (cons req-evt (cons nack-evt (state-evts st)))))

(define (add-state-reqs! st reqs)
  (for ([req (in-list reqs)])
    (add-state-req! st req)))

(define (add-state-evt! st evt)
  (set-state-evts! st (cons evt (state-evts st))))

(define (remove-state-evt! st evt)
  (set-state-evts! st (remq evt (state-evts st))))

(define (pop-state-pending-futs! st)
  (define pending-futs (state-pending-futs st))
  (begin0 pending-futs
    (set-state-pending-futs! st null)))

(define (add-state-fut! st fut)
  (set-state-pending-futs! st (cons fut (state-pending-futs st))))

(define (add-state-message! st topic pid key value headers)
  (define t (hash-ref! (state-batches st) topic make-hasheqv))
  (define b (hash-ref! t pid (state-batch-proc st)))
  (batch-append! b key value #:headers headers)
  (define size
    (+ (if key (bytes-length key) 0)
       (if value (bytes-length value) 0)
       (for/sum ([(k v) (in-hash headers)])
         (+ (string-length k)
            (if v (bytes-length v) 0)))))
  (set-state-pending-bytes! st (+ (state-pending-bytes st) size))
  (set-state-pending-count! st (add1 (state-pending-count st))))

(define (pop-state-batches! st)
  (define batches (state-batches st))
  (begin0 batches
    (set-state-batches! st (make-hash))
    (set-state-pending-bytes! st 0)
    (set-state-pending-count! st 0)))

(define (reset-state-deadline-evt! st interval-ms)
  (define start-time (current-inexact-monotonic-milliseconds))
  (remove-state-evt! st (state-deadline-evt st))
  (define evt
    (handle-evt
     (alarm-evt (+ start-time interval-ms) #t)
     (lambda (_)
       (log-kafka-producer-debug
        "flush deadline: ~a"
        (~ms (- (current-inexact-monotonic-milliseconds) start-time)))
       (remove-state-evt! st evt)
       (set-state-force-flush?! st #t))))
  (set-state-deadline-evt! st evt)
  (add-state-evt! st evt))


;; Req ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Req ([nack #:mutable] res-ch) #:transparent)
(struct ProduceReq Req (evt) #:transparent)
(struct FlushReq Req () #:transparent)
(struct StopReq Req () #:transparent)
(struct FailReq Req (err) #:transparent)


;; Future ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; state : (or/c 'ready 'guarded 'resolved)
;; On `produce', the event associated with a Future is returned to the
;; user.  If the user discards that event, then we race GC against
;; future resolution, such that if the event is garbage-collected
;; before the future is resolved, we avoid trying to send it a result
;; because we're guaranteed that nobody will be listening for one.
(struct Future (ch topic pid [state #:mutable])
  #:transparent
  #:property prop:evt (struct-field-index ch))

(define (make-Future topic pid)
  (define fut
    (Future (make-channel) topic pid 'ready))
  (define evt
    (guard-evt
     (lambda ()
       ;; The guard-evt may be garbage-collected at the end of this
       ;; procedure's dynamic extent, so we have to mark the Future as
       ;; having been guarded to avoid a deadlock.
       (begin0 (Future-ch fut)
         (set-Future-state! fut 'guarded)))))
  (will-register
   executor evt
   (lambda (_)
     (unless (eq? (Future-state fut) 'guarded)
       (set-Future-state! fut 'garbage))))
  (values fut evt))

(define (resolve fut res)
  (unless (eq? (Future-state fut) 'garbage)
    (void (thread (λ () (channel-put (Future-ch fut) res))))))

(define executor
  (make-will-executor))

(void
 (thread
  (parameterize ([current-namespace (make-base-empty-namespace)])
    (lambda ()
      (let loop ()
        (with-handlers ([exn:fail?
                         (lambda (e)
                           (log-kafka-producer-error "will execution failed: ~a" (exn-message e)))])
          (will-execute executor))
        (loop))))))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (~ms n)
  (~a (~r #:precision '(= 2) n) "ms"))

(define (make-produce-evts
         client batches
         #:acks acks
         #:timeout-ms timeout-ms)
  (let/ec esc
    (let retry ([metadata (client-metadata client)]
                [reloaded? #f])
      (define nodes-by-topic&pid
        (collect-nodes-by-topic&pid metadata (hash-keys batches)))
      (define topics-by-node
        (for*/fold ([data-by-node (hasheqv)])
                   ([(topic parts) (in-hash batches)]
                    [(pid b) (in-hash parts)])
          (define topic&pid (cons topic pid))
          (define node-id
            (hash-ref
             nodes-by-topic&pid
             topic&pid
             (lambda ()
               (and (not reloaded?)
                    (esc (retry (reload-metadata client) #t))))))
          (define node-topics (hash-ref data-by-node node-id hash))
          (define partition-data
            (make-PartitionData
             #:id pid
             #:batch (call-with-output-bytes
                      (lambda (out)
                        (write-batch b out)))))
          (hash-set
           data-by-node node-id
           (hash-update
            node-topics topic
            (λ (partitions)
              (cons partition-data partitions))
            null))))
      ;; node-id may be #f when we couldn't find a node for a
      ;; particular topic&pid combination, in which case we fake an
      ;; error response.
      (for/list ([(node-id topics) (in-hash topics-by-node)])
        (cond
          [node-id
           (define conn (get-node-connection client node-id))
           (define data
             (for/list ([(topic partitions) (in-hash topics)])
               (make-TopicData
                #:name topic
                #:partitions partitions)))
           (make-Produce-evt conn data acks timeout-ms)]

          [else
           (pure-evt
            (make-ProduceResponse
             #:topics (for/list ([(topic partitions) (in-hash topics)])
                        (make-ProduceResponseTopic
                         #:name topic
                         #:partitions (for/list ([p (in-list partitions)])
                                        (make-ProduceResponsePartition
                                         #:id (PartitionData-id p)
                                         #:error-code 3
                                         #:offset -1))))))])))))
