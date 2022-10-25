#lang racket/base

(require racket/contract
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
  [produce (->* (producer? string? bytes? bytes?)
                (#:partition exact-nonnegative-integer?)
                evt?)]
  [producer-flush (-> producer? void?)]
  [producer-stop (-> producer? void?)]))

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
       (define st (make-state (make-deadline-evt flush-interval-ms)))
       (define batches (make-hash))
       (define (append! topic pid key value)
         (define t (hash-ref! batches topic make-hasheqv))
         (define b (hash-ref! t pid (λ () (make-batch #:compression compression))))
         (batch-append! b key value))
       (let loop ()
         (cond
           [(or (state-force-flush? st)
                (> (state-pending-bytes st) max-batch-bytes)
                (> (state-pending-count st) max-batch-size))
            (log-kafka-producer-debug
             "flushing ~a messages (~a bytes)"
             (state-pending-count st)
             (state-pending-bytes st))
            (define start-time (current-inexact-monotonic-milliseconds))
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
            (define duration
              (- (current-inexact-monotonic-milliseconds) start-time))
            (log-kafka-producer-debug "flush took ~ams" (~r #:precision '(= 2) duration))
            (hash-clear! batches)
            (add-state-reqs! st pending-reqs)
            (set-state-deadline-evt! st (make-deadline-evt flush-interval-ms))
            (set-state-force-flush?! st #f)
            (reset-state-pending-bytes&count! st)
            (loop)]

           [else
            (apply
             sync
             (handle-evt
              ch
              (lambda (msg)
                (cond
                  [(state-stopped? st)
                   (match-define `(,_ ,_ ... ,nack ,res-ch) msg)
                   (add-state-reqs! st (FailReq nack res-ch (client-error "stop in progress")))]

                  [else
                   (match msg
                     [`(produce ,topic ,pid ,key, value ,nack ,req-ch)
                      (append! topic pid key value)
                      (define bytes-size
                        (+ (bytes-length key)
                           (bytes-length value)))
                      (define-values (fut fut-evt)
                        (make-Future topic pid))
                      (incr-state-pending-bytes&count! st bytes-size)
                      (add-state-req! st (ProduceReq nack req-ch fut-evt))
                      (add-state-fut! st fut)]

                     [`(stop ,nack ,res-ch)
                      (add-state-pending-req! st (StopReq nack res-ch))
                      (set-state-force-flush?! st #t)
                      (set-state-stopped?! st #t)]

                     [`(flush ,nack ,res-ch)
                      (add-state-pending-req! st (FlushReq nack res-ch))
                      (set-state-force-flush?! st #t)]

                     [msg
                      (log-kafka-producer-error "invalid message: ~e" msg)])])))
             (handle-evt
              (state-deadline-evt st)
              (λ (_) (set-state-force-flush?! st #t)))
             (state-evts st))
            (loop)])))))
  (producer ch batcher))

(define (produce p topic key value #:partition [pid 0])
  (sync (make-producer-evt p `(produce ,topic ,pid ,key ,value))))

(define (producer-flush p)
  (sync (make-producer-evt p `(flush))))

(define (producer-stop p)
  (sync (make-producer-evt p `(stop))))

(define (make-producer-evt p msg)
  (define ch (producer-ch p))
  (define thd (producer-batcher p))
  (define res-ch (make-channel))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume thd (current-thread))
      (begin0 res-ch
        (sync
         (thread-dead-evt thd)
         (channel-put-evt ch (append msg `(,nack ,res-ch)))))))
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
   pending-bytes
   pending-count)
  #:transparent
  #:mutable)

(define (make-state deadline-evt)
  (state #f #f deadline-evt null null null 0 0))

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

(define (remove-state-evt! st evt)
  (set-state-evts! st (remq evt (state-evts st))))

(define (pop-state-pending-futs! st)
  (define pending-futs (state-pending-futs st))
  (begin0 pending-futs
    (set-state-pending-futs! st null)))

(define (add-state-fut! st fut)
  (set-state-pending-futs! st (cons fut (state-pending-futs st))))

(define (incr-state-pending-bytes&count! st bytes-amt [count-amt 1])
  (set-state-pending-bytes! st (+ (state-pending-bytes st) bytes-amt))
  (set-state-pending-count! st (+ (state-pending-count st) count-amt)))

(define (reset-state-pending-bytes&count! st)
  (set-state-pending-bytes! st 0)
  (set-state-pending-count! st 0))


;; Req ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Req ([nack #:mutable] res-ch) #:transparent)
(struct ProduceReq Req (evt) #:transparent)
(struct FlushReq Req () #:transparent)
(struct StopReq Req () #:transparent)
(struct FailReq Req (err) #:transparent)


;; Future ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Future (ch topic pid [garbage? #:mutable])
  #:transparent
  #:property prop:evt (struct-field-index ch))

(define (make-Future topic pid)
  (define fut (Future (make-channel) topic pid #f))
  (define evt (guard-evt (λ () (Future-ch fut))))
  (begin0 (values fut evt)
    (will-register executor evt (λ (_) (set-Future-garbage?! fut #t)))))

(define (resolve fut res)
  (unless (Future-garbage? fut)
    (void (thread (λ () (channel-put (Future-ch fut) res))))))

(define executor
  (make-will-executor))

(void
 (thread
  (parameterize ([current-namespace (make-base-empty-namespace)])
    (lambda ()
      (let loop ()
        (with-handlers ([exn:fail? (λ (e) (log-kafka-producer-warning "will execution failed: ~a" (exn-message e)))])
          (will-execute executor))
        (loop))))))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (make-deadline-evt ms)
  (alarm-evt (+ (current-inexact-milliseconds) ms)))

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
