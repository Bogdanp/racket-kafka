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
       (define batches (make-hash))
       (define (append! topic pid key value)
         (define t (hash-ref! batches topic make-hasheqv))
         (define b (hash-ref! t pid (λ () (make-batch #:compression compression))))
         (batch-append! b key value))
       (let loop ([st (make-state (make-deadline-evt flush-interval-ms))])
         (cond
           [(or (state-force-flush? st)
                (> (state-pending-bytes st) max-batch-bytes)
                (> (state-pending-count st) max-batch-size))
            (log-kafka-producer-debug
             "flushing ~a messages (~a bytes)"
             (state-pending-count st)
             (state-pending-bytes st))
            (define start-time (current-inexact-monotonic-milliseconds))
            (define-values (next-st pending-reqs)
              (pop-state-pending-reqs st))
            (define ready-reqs
              (with-handlers ([exn:fail?
                               (λ (err)
                                 (for/list ([r (in-list pending-reqs)])
                                   (if (ProduceRes? r)
                                       (struct-copy ProduceRes r [res err])
                                       r)))])
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
                (for/list ([r (in-list pending-reqs)])
                  (cond
                    [(ProduceRes? r)
                     (define topic (ProduceRes-topic r))
                     (define pid (ProduceRes-pid r))
                     (define topic&pid (cons topic pid))
                     (define partition-res (hash-ref results-by-topic&pid topic&pid #f))
                     (struct-copy ProduceRes r [res (or partition-res (make-RecordResult
                                                                       #:topic topic
                                                                       #:partition (make-ProduceResponsePartition
                                                                                    #:id pid
                                                                                    #:error-code 0
                                                                                    #:offset -1)))])]
                    [else r]))))
            (define duration
              (- (current-inexact-monotonic-milliseconds) start-time))
            (log-kafka-producer-debug "flush took ~ams" (~r #:precision '(= 2) duration))
            (hash-clear! batches)
            (loop
             (reset-state-pending-bytes&count
              (set-state-deadline
               (state-unforce-flush
                (add-state-reqs next-st ready-reqs))
               (make-deadline-evt flush-interval-ms))))]

           [else
            (apply
             sync
             (handle-evt
              ch
              (lambda (msg)
                (cond
                  [(state-stopped? st)
                   (match-define `(,_ ,_ ... ,nack ,res-ch) msg)
                   (loop (add-state-req st (FailReq nack res-ch (client-error "stop in progress"))))]

                  [else
                   (match msg
                     [`(produce ,topic ,pid ,key, value ,nack ,req-ch)
                      (append! topic pid key value)
                      (define bytes-size
                        (+ (bytes-length key)
                           (bytes-length value)))
                      (define-values (res res-evt)
                        (make-ProduceRes topic pid))
                      (loop
                       (add-state-pending-req
                        (add-state-req
                         (incr-state-pending-bytes&count st bytes-size)
                         (ProduceReq nack req-ch res-evt))
                        res))]

                     [`(stop ,nack ,res-ch)
                      (loop (state-force-flush (add-state-pending-req st (StopReq nack res-ch))))]

                     [`(flush ,nack ,res-ch)
                      (loop (state-force-flush (add-state-pending-req st (FlushReq nack res-ch))))]

                     [msg
                      (log-kafka-producer-error "invalid message: ~e" msg)
                      (loop st)])])))
             (handle-evt
              (state-deadline-evt st)
              (lambda (_)
                (loop
                 (state-force-flush
                  (set-state-deadline st (make-deadline-evt flush-interval-ms))))))
             (append
              (for/list ([r (in-list (state-reqs st))])
                (define req-evt
                  (match r
                    [(ProduceReq _ res-ch evt)     (channel-put-evt res-ch evt)]
                    [(ProduceRes _ res-ch _ _ res) (channel-put-evt res-ch res)]
                    [(FlushReq   _ res-ch)         (channel-put-evt res-ch (void))]
                    [(StopReq    _ res-ch)         (channel-put-evt res-ch (void))]
                    [(FailReq    _ res-ch err)     (channel-put-evt res-ch err)]))
                (handle-evt
                 req-evt
                 (lambda (_)
                   (loop (remove-state-req st r)))))
              (for/list ([r (in-list (state-reqs st))] #:when (Req-nack r))
                (handle-evt
                 (Req-nack r)
                 (lambda (_)
                   (loop (remove-state-req st r)))))))])))))
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
   reqs
   pending-bytes
   pending-count)
  #:transparent)

(define (make-state deadline-evt)
  (state #f #f deadline-evt null null 0 0))

(define (set-state-deadline st deadline-evt)
  (struct-copy state st [deadline-evt deadline-evt]))

(define (add-state-pending-req st req)
  (struct-copy state st [pending-reqs (cons req (state-pending-reqs st))]))

(define (pop-state-pending-reqs st)
  (values
   (struct-copy state st [pending-reqs null])
   (state-pending-reqs st)))

(define (add-state-req st req)
  (struct-copy state st [reqs (cons req (state-reqs st))]))

(define (add-state-reqs st reqs)
  (struct-copy state st [reqs (append reqs (state-reqs st))]))

(define (remove-state-req st req)
  (struct-copy state st [reqs (remq req (state-reqs st))]))

(define (state-force-flush st)
  (struct-copy state st [force-flush? #t]))

(define (state-unforce-flush st)
  (struct-copy state st [force-flush? #f]))

(define (incr-state-pending-bytes&count st bytes-amt [count-amt 1])
  (struct-copy state st
               [pending-bytes (+ (state-pending-bytes st) bytes-amt)]
               [pending-count (+ (state-pending-count st) count-amt)]))

(define (reset-state-pending-bytes&count st)
  (struct-copy state st
               [pending-bytes 0]
               [pending-count 0]))


;; Req ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Req ([nack #:mutable] res-ch) #:transparent)
(struct ProduceReq Req (evt) #:transparent)
(struct ProduceRes Req (topic pid res) #:transparent)
(struct FlushReq Req () #:transparent)
(struct StopReq Req () #:transparent)
(struct FailReq Req (err) #:transparent)

(define (make-ProduceRes topic pid)
  (define ch (make-channel))
  (define res (ProduceRes #f ch topic pid (void)))
  (define res-evt
    (handle-evt
     (nack-guard-evt
      (lambda (nack)
        (begin0 ch
          (set-Req-nack! res nack))))
     (lambda (res-or-exn)
       (begin0 res-or-exn
         (when (exn:fail? res-or-exn)
           (raise res-or-exn))))))
  (will-register
   executor
   res-evt
   (lambda (_)
     ;; The `handle-evt' may be GC'd as soon as its handler procedure
     ;; finishes, so we have to take care not to mutate the nack in
     ;; that case, lest we cause a deadlock.
     (unless (Req-nack res)
       #;(log-kafka-debug "ProduceRes GC: ~e" res)
       (set-Req-nack! res always-evt))))
  (values res res-evt))

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
