#lang racket/base

(require actor
         racket/contract/base
         racket/format
         racket/match
         racket/port
         racket/promise
         "private/batch.rkt"
         "private/client.rkt"
         "private/common.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (contract-out
  [producer?
   (-> any/c boolean?)]
  [make-producer
   (->* [client?]
        [#:acks (or/c 'none 'leader 'full)
         #:compression (or/c 'none 'gzip)
         #:flush-interval exact-positive-integer?
         #:max-batch-bytes exact-positive-integer?
         #:max-batch-size exact-positive-integer?]
        producer?)]
  [produce
   (->* [producer? string? (or/c #f bytes?) (or/c #f bytes?)]
        [#:partition exact-nonnegative-integer?
         #:headers (hash/c string? (or/c #f bytes?))]
        evt?)]
  [producer-flush
   (-> producer? void?)]
  [producer-stop
   (-> producer? void?)]))

(module+ unsafe
  (provide
   producer?
   make-producer
   produce
   producer-flush
   producer-stop))

(define-logger kafka-producer)

(struct producer (batcher)
  #:transparent)

(define (make-producer
         client
         #:acks [acks 'leader]
         #:compression [compression 'gzip]
         #:flush-interval [interval-ms 60000]
         #:max-batch-bytes [max-batch-bytes (* 100 1024 1024)]
         #:max-batch-size [max-batch-size 1000])
  (producer (batcher client acks compression interval-ms max-batch-bytes max-batch-size)))

(define (produce p topic key value
                 #:partition [pid 0]
                 #:headers [headers (hash)])
  (-produce (producer-batcher p) topic pid key value headers))

(define (producer-flush p)
  (-flush (producer-batcher p)))

(define (producer-stop p)
  (-stop (producer-batcher p)))

(define-actor (batcher client acks compression interval-ms max-batch-bytes max-batch-size)
  #:state
  (make-state
   (make-deadline-evt interval-ms)
   (lambda ()
     (make-batch #:compression compression)))
  #:receive?
  (compose1 not state-stopped?)
  #:stopped?
  (lambda (st)
    (and (state-stopped? st)
         (not (state-flush? st))))
  #:event
  (lambda (st)
    (choice-evt
     (handle-evt
      (state-deadline-evt st)
      (lambda (_)
        (log-kafka-producer-debug "flush deadline")
        (struct-copy
         state st
         [flush? #t]
         [deadline-evt never-evt])))
     (handle-evt
      (if (or (state-flush? st)
              (>= (state-pending-bytes st) max-batch-bytes)
              (>= (state-pending-count st) max-batch-size))
          always-evt
          never-evt)
      (lambda (_)
        (log-kafka-producer-debug
         "flushing ~a messages (~a bytes)"
         (state-pending-count st)
         (state-pending-bytes st))
        (define t0 (current-inexact-monotonic-milliseconds))
        (define batches (state-batches st))
        (define waiters (state-waiters st))
        (with-handlers ([exn:fail?
                         (lambda (err)
                           (for ([w (in-list waiters)])
                             (resolve w err)))])
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
          (for ([w (in-list waiters)])
            (match-define (Waiter _ topic pid) w)
            (define topic&pid
              (cons topic pid))
            (define partition-res
              (hash-ref results-by-topic&pid topic&pid #f))
            (define produce-res
              (or partition-res
                  (make-RecordResult
                   #:topic topic
                   #:partition
                   (make-ProduceResponsePartition
                    #:id pid
                    #:error-code 0
                    #:offset -1))))
            (resolve w produce-res)))
        (log-kafka-producer-debug
         "flush duration: ~a"
         (~ms (- (current-inexact-monotonic-milliseconds) t0)))
        (struct-copy
         state st
         [flush? #f]
         [deadline-evt (make-deadline-evt interval-ms)]
         [batches (make-hash)]
         [waiters null]
         [pending-bytes 0]
         [pending-count 0])))))

  (define (-produce st topic pid key value headers)
    (define t (hash-ref! (state-batches st) topic make-hasheqv))
    (define b (hash-ref! t pid (state-batch-proc st)))
    (batch-append! b key value #:headers headers)
    (define size
      (+ (if key (bytes-length key) 0)
         (if value (bytes-length value) 0)
         (for/sum ([(k v) (in-hash headers)])
           (+ (string-length k)
              (if v (bytes-length v) 0)))))
    (define-values (waiter waiter-evt)
      (make-Waiter topic pid))
    (values
     (struct-copy
      state st
      [waiters (cons waiter (state-waiters st))]
      [pending-bytes (+ (state-pending-bytes st) size)]
      [pending-count (add1 (state-pending-count st))])
     waiter-evt))

  (define (-flush st)
    (values
     (struct-copy
      state st
      [flush? #t])
     (void)))

  (define (-stop st)
    (values
     (struct-copy
      state st
      [flush? #t]
      [stopped? #t])
     (void))))


;; State ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct state
  (stopped?
   flush?
   deadline-evt
   batch-proc
   batches
   waiters
   pending-bytes
   pending-count))

(define (make-state
         deadline-evt
         make-batch-proc)
  (state
   #;stopped? #f
   #;flush? #f
   #;deadline-evt deadline-evt
   #;batch-proc make-batch-proc
   #;batches (make-hash)
   #;waiters null
   #;pending-bytes 0
   #;pending-count 0))

(define (make-deadline-evt interval-ms)
  (alarm-evt
   #;msesc (+ (current-inexact-monotonic-milliseconds) interval-ms)
   #;monotonic? #t))


;; Waiter ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Waiter (res-ch topic pid))

(define (make-Waiter topic pid)
  (define res-ch
    (make-channel))
  (define promise
    (delay/thread
     (define res-or-exn
       (channel-get res-ch))
     (when (exn:fail? res-or-exn)
       (raise res-or-exn))
     res-or-exn))
  (define waiter
    (Waiter res-ch topic pid))
  (values waiter (handle-evt promise (λ (_) (force promise)))))

(define (resolve w res)
  (channel-put (Waiter-res-ch w) res))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (~ms n)
  (~a (~r #:precision '(= 2) n) "ms"))

(define (make-produce-evts
         client batches
         #:acks acks
         #:timeout-ms timeout-ms)
  (let/ec esc
    (let loop ([metadata (client-metadata client)]
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
                    (esc (loop (reload-metadata client) #t))))))
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
             #:topics
             (for/list ([(topic partitions) (in-hash topics)])
               (make-ProduceResponseTopic
                #:name topic
                #:partitions
                (for/list ([p (in-list partitions)])
                  (make-ProduceResponsePartition
                   #:id (PartitionData-id p)
                   #:error-code 3
                   #:offset -1))))))])))))
