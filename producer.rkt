#lang racket/base

(require racket/contract
         racket/match
         racket/port
         "private/batch.rkt"
         "private/connection.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (contract-out
  [producer? (-> any/c boolean?)]
  [make-producer (->* (connection? string?)
                      (#:acks (or/c 'none 'leader 'full)
                       #:compression (or/c 'none 'gzip)
                       #:flush-interval exact-positive-integer?
                       #:max-batch-bytes exact-positive-integer?
                       #:max-batch-size exact-positive-integer?)
                      producer?)]
  [produce (->* (producer? bytes? bytes?)
                (#:partition exact-nonnegative-integer?)
                evt?)]
  [producer-flush (-> producer? void?)]
  [producer-stop (-> producer? void?)]))

(define-logger kafka-producer)

(struct producer (conn topic ch batcher)
  #:transparent)

(define (make-producer
         conn topic
         #:acks [acks 'leader]
         #:compression [compression 'gzip]
         #:flush-interval [flush-interval-ms 60000]
         #:max-batch-bytes [max-batch-len (* 100 1024 1024)]
         #:max-batch-size [max-batch-size 1000])
  (define ch (make-channel))
  (define batcher
    (thread/suspend-to-kill
     (lambda ()
       (define batches
         (make-hasheqv))
       (define (append! pid key value)
         (define b (hash-ref! batches pid (λ () (make-batch #:compression compression))))
         (batch-append! b key value))
       (define (flush?)
         (define-values (bs sz)
           (batch-stats batches))
         (or (> bs max-batch-len)
             (> sz max-batch-size)))
       (define (make-flush-evt pending-reqs)
         (define-values (_bs sz)
           (batch-stats batches))
         (define evt
           (cond
             [(zero? sz) always-evt]
             [else
              (begin0 (make-produce-evt
                       conn topic batches
                       #:acks acks
                       #:timeout-ms 30000)
                (hash-clear! batches))]))
         (handle-evt evt (λ (res) (cons res pending-reqs))))
       (let loop ([st (make-state (make-deadline-evt flush-interval-ms))])
         (cond
           [(or (state-force-flush? st) (flush?))
            (define-values (next-st pending-reqs)
              (pop-state-pending-reqs st))
            (define flush-evt
              (make-flush-evt pending-reqs))
            (loop
             (state-unforce-flush
              (add-state-pending-evt next-st flush-evt)))]

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
                     [`(produce ,pid ,key, value ,nack ,req-ch)
                      (append! pid key value)
                      (define-values (res res-evt)
                        (make-ProduceRes))
                      (loop
                       (add-state-pending-req
                        (add-state-req st (ProduceReq nack req-ch res-evt))
                        res))]

                     [`(stop ,nack ,res-ch)
                      (loop (state-force-flush (add-state-pending-req st (StopReq nack res-ch))))]

                     [`(flush ,nack ,res-ch)
                      (loop (state-force-flush (add-state-pending-req st (FlushReq nack res-ch))))]

                     [msg
                      (log-kafka-producer-error "invalid message ~e" msg)
                      (loop st)])])))
             (handle-evt
              (state-deadline-evt st)
              (lambda (_)
                (loop
                 (state-force-flush
                  (set-state-deadline st (make-deadline-evt flush-interval-ms))))))
             (append
              (for/list ([pending-evt (in-list (state-pending-evts st))])
                (handle-evt
                 pending-evt
                 (lambda (res&reqs)
                   (match-define (cons res reqs) res&reqs)
                   (loop
                    (remove-state-pending-evt
                     (add-state-reqs st (for/list ([r (in-list reqs)])
                                          ;; FIXME
                                          (if (ProduceRes? r)
                                              (struct-copy ProduceRes r [res res])
                                              r)))
                     pending-evt)))))
              (for/list ([r (in-list (state-reqs st))])
                (define req-evt
                  (match r
                    [(ProduceReq _ res-ch evt) (channel-put-evt res-ch evt)]
                    [(ProduceRes _ res-ch res) (channel-put-evt res-ch res)]
                    [(FlushReq   _ res-ch)     (channel-put-evt res-ch (void))]
                    [(StopReq    _ res-ch)     (channel-put-evt res-ch (void))]
                    [(FailReq    _ res-ch err) (channel-put-evt res-ch err)]))
                (handle-evt
                 req-evt
                 (lambda (_)
                   (loop (remove-state-req st r)))))
              (for/list ([r (in-list (state-reqs st))] #:when (Req-nack r))
                (handle-evt
                 (Req-nack r)
                 (lambda (_)
                   (loop (remove-state-req st r)))))))])))))
  (producer conn topic ch batcher))

(define (produce p key value #:partition [pid -1])
  (sync (make-producer-evt p `(produce ,pid ,key ,value))))

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
      (thread-resume thd)
      (begin0 res-ch
        (channel-put ch (append msg `(,nack ,res-ch))))))
   (lambda (res-or-exn)
     (begin0 res-or-exn
       (when (exn:fail? res-or-exn)
         (raise res-or-exn))))))


;; State ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct state
  (deadline-evt
   stopped?
   pending-evts
   pending-reqs
   reqs
   force-flush?)
  #:transparent)

(define (make-state deadline-evt)
  (state deadline-evt #f null null null #f))

(define (set-state-deadline st deadline-evt)
  (struct-copy state st [deadline-evt deadline-evt]))

(define (add-state-pending-evt st evt)
  (struct-copy state st [pending-evts (cons evt (state-pending-evts st))]))

(define (remove-state-pending-evt st evt)
  (struct-copy state st [pending-evts (remq evt (state-pending-evts st))]))

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
(struct ProduceRes Req (res) #:transparent)
(struct FlushReq Req () #:transparent)
(struct StopReq Req () #:transparent)
(struct FailReq Req (err) #:transparent)

(define (make-ProduceRes)
  (define ch (make-channel))
  (define res (ProduceRes #f ch (void)))
  (define res-evt
    (nack-guard-evt
     (lambda (nack)
       (begin0 ch
         (set-Req-nack! res nack)))))
  (will-register
   executor res-evt
   (λ (_) (set-Req-nack! res always-evt)))
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
  (for/fold ([bs 0]
             [sz 0])
            ([b (in-hash-values batches)])
    (values
     (+ bs (batch-len b))
     (+ sz (batch-size b)))))

(define (make-deadline-evt ms)
  (alarm-evt (+ (current-inexact-milliseconds) ms)))

(require racket/pretty)
(define (make-produce-evt
         conn topic batches
         #:acks acks
         #:timeout-ms timeout-ms)
  (define parts
    (for/list ([(pid b) (in-hash batches)])
      (PartitionData pid (call-with-output-bytes
                          (lambda (out)
                            (write-batch b out))))))
  (define data
    (TopicData topic parts))
  (pretty-print data)
  (make-Produce-evt conn (list data) acks timeout-ms))
