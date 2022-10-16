#lang racket/base

(require racket/contract
         racket/promise
         "private/batch.rkt"
         "private/client.rkt"
         "private/record.rkt"
         "private/serde.rkt")

(provide
 internal-events?
 (contract-out
  [make-internal-events (-> client? internal-events?)]
  [stop-internal-events (-> internal-events? void?)]
  [internal-events-supported? (-> client? boolean?)]))

(define consumer-offsets-topic
  "__consumer_offsets")

(struct internal-events (ch [thd #:mutable])
  #:transparent
  #:property prop:evt (struct-field-index ch))

(define (make-internal-events c)
  (define metadata
    (get-offsets-topic c))
  (define ch (make-channel))
  (define thd
    (thread
     (lambda ()
       (define offsets (make-hasheqv))
       (let loop ([deadline 0])
         (define next-deadline
           (sync
            (handle-evt
             (thread-receive-evt)
             (λ (_) #f))
            (replace-evt
             (alarm-evt deadline #t)
             (lambda (_)
               (define events
                 (get-internal-events c metadata offsets))
               (handle-evt
                (channel-put-evt ch events)
                (λ (_)
                  (if (null? events)
                      (+ (current-inexact-monotonic-milliseconds) 1000)
                      (current-inexact-monotonic-milliseconds))))))))
         (when next-deadline
           (loop next-deadline))))))

  (internal-events ch thd))

(define (stop-internal-events i)
  (thread-send (internal-events-thd i) '(stop)))

(define (internal-events-supported? c)
  (and (get-offsets-topic c) #t))

(define (get-internal-events c metadata offsets)
  (define partitions-by-node
    (for/fold ([nodes (hasheqv)])
              ([p (in-list (TopicMetadata-partitions metadata))])
      (hash-update
       nodes
       (PartitionMetadata-leader-id p)
       (λ (parts)
         (define pid (PartitionMetadata-id p))
         (define part
           (make-TopicPartition
            #:id pid
            #:offset (hash-ref offsets pid 0)))
         (cons part parts))
       null)))
  (define response-promises
    (for/list ([(node-id partitions) (in-hash partitions-by-node)])
      (delay/thread
       (define topic-partitions (hash consumer-offsets-topic partitions))
       (sync (make-Fetch-evt (get-node-connection c node-id) topic-partitions 1000)))))
  (define responses
    (for/list ([promise (in-list response-promises)])
      (with-handlers ([exn:fail? (λ (e)
                                   (begin0 #f
                                     ((error-display-handler)
                                      (format "get-internal-events: ~a" (exn-message e))
                                      e)))])
        (define response (force promise))
        (begin0 response
          (for* ([parts (in-hash-values (FetchResponse-topics response))]
                 [part (in-list parts)]
                 [b (in-list (FetchResponsePartition-batches part))])
            (define pid (FetchResponsePartition-id part))
            (define size (batch-size b))
            (unless (zero? size)
              (define last-record (vector-ref (batch-records b) (sub1 size)))
              (hash-set! offsets pid (add1 (record-offset last-record)))))))))
  (for*/list ([response (in-list responses)]
              [parts (in-hash-values (FetchResponse-topics response))]
              [part (in-list parts)]
              [batch (in-list (FetchResponsePartition-batches part))]
              [r (in-vector (batch-records batch))]
              #:when (record-value r)
              [event (in-value
                      (parse-Internal
                       (record-key r)
                       (record-value r)))]
              #:when event)
    event))

(define (get-offsets-topic c)
  (for/first ([topic (in-list (Metadata-topics (client-metadata c)))]
              #:when (equal? (TopicMetadata-name topic) consumer-offsets-topic))
    topic))
