#lang racket/base

(require racket/contract
         racket/match
         racket/port
         "private/batch.rkt"
         "private/client.rkt"
         "private/connection.rkt"
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
       (define (flush?)
         (define-values (bs sz)
           (batch-stats batches))
         (or (> bs max-batch-bytes)
             (> sz max-batch-size)))
       ; (listof Req?) -> (evt/c (cons/c (or/c exn? ProduceResponse?) (listof Req?)))
       (define (make-flush-evt pending-reqs)
         (define-values (_bs sz)
           (batch-stats batches))
         (define evt
           (handle-evt
            ; (evt/c (or/c exn? ProduceResponse?))
            (if (zero? sz)
                (pure-evt (make-ProduceResponse #:topics null))
                (with-handlers ([exn:fail? pure-evt])
                  (define conn
                    (get-connection client))
                  (make-produce-evt
                   conn batches
                   #:acks acks
                   #:timeout-ms 30000)))
            (lambda (res)
              (cons res pending-reqs))))
         (begin0 evt
           (hash-clear! batches)))
       (let loop ([st (make-state (make-deadline-evt flush-interval-ms))])
         (cond
           [(or (state-force-flush? st) (flush?))
            (define-values (next-st pending-reqs)
              (pop-state-pending-reqs st))
            (define flush-evt
              (make-flush-evt pending-reqs))
            (define ready-reqs
              (match (sync flush-evt)
                [(cons (? exn:fail? err) reqs)
                 (for/list ([r (in-list reqs)])
                   (if (ProduceRes? r)
                       (struct-copy ProduceRes r [res err])
                       r))]

                [(cons res reqs)
                 (define results-by-topic&pid
                   (for*/hash ([t (in-list (ProduceResponse-topics res))]
                               [p (in-list (ProduceResponseTopic-partitions t))])
                     (define topic (ProduceResponseTopic-name t))
                     (define pid (ProduceResponsePartition-id p))
                     (define topic&pid (cons topic pid))
                     (define error-code (ProduceResponsePartition-error-code p))
                     (values topic&pid (if (not (zero? error-code))
                                           (server-error error-code)
                                           (make-RecordResult
                                            #:topic topic
                                            #:partition p)))))
                 (for/list ([r (in-list reqs)])
                   (cond
                     [(ProduceRes? r)
                      (define topic (ProduceRes-topic r))
                      (define pid (ProduceRes-pid r))
                      (define topic&pid (cons topic pid))
                      (define partition-res
                        (hash-ref
                         results-by-topic&pid
                         topic&pid
                         (λ ()
                           (make-RecordResult
                            #:topic topic
                            #:partition (make-ProduceResponsePartition
                                         #:id pid
                                         #:error-code 0
                                         #:offset -1)))))
                      (struct-copy ProduceRes r [res partition-res])]
                     [else r]))]))
            (loop
             (set-state-deadline
              (state-unforce-flush
               (add-state-reqs next-st ready-reqs))
              (make-deadline-evt flush-interval-ms)))]

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
                      (define-values (res res-evt)
                        (make-ProduceRes topic pid))
                      (loop
                       (add-state-pending-req
                        (add-state-req st (ProduceReq nack req-ch res-evt))
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

(define (pure-evt v)
  (handle-evt always-evt (λ (_) v)))


;; State ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct state
  (stopped?
   force-flush?
   deadline-evt
   pending-reqs
   reqs)
  #:transparent)

(define (make-state deadline-evt)
  (state #f #f deadline-evt null null))

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
       (log-kafka-debug "ProduceRes GC: ~e" res)
       (set-Req-nack! res always-evt))))
  (values res res-evt))

(define executor
  (make-will-executor))

(void
 (thread
  (parameterize ([current-namespace (make-base-empty-namespace)])
    (lambda ()
      (let loop ()
        (with-handlers ([exn:fail?
                         (λ (e)
                           (log-warning "will execution failed: ~a" (exn-message e))
                           (loop))])
          (will-execute executor)
          (loop)))))))

;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (batch-stats batches)
  (for*/fold ([bs 0]
              [sz 0])
             ([t (in-hash-values batches)]
              [b (in-hash-values t)])
    (values
     (+ bs (batch-len b))
     (+ sz (batch-size b)))))

(define (make-deadline-evt ms)
  (alarm-evt (+ (current-inexact-milliseconds) ms)))

(define (make-produce-evt
         conn batches
         #:acks acks
         #:timeout-ms timeout-ms)
  (define data
    (for/list ([(topic parts) (in-hash batches)])
      (make-TopicData
       #:name topic
       #:partitions (for/list ([(pid b) (in-hash parts)])
                      (make-PartitionData
                       #:id pid
                       #:batch (call-with-output-bytes
                                (lambda (out)
                                  (write-batch b out))))))))
  (make-Produce-evt conn data acks timeout-ms))
