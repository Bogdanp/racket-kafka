#lang racket/base

(require racket/contract/base
         "iterator.rkt"
         "private/client.rkt"
         "private/serde.rkt")

(provide
 internal-events?
 (contract-out
  [internal-events-supported? (-> client? boolean?)]
  [make-internal-events (-> client? internal-events?)]
  [get-events (-> internal-events? vector?)]))

(define consumer-offsets-topic
  "__consumer_offsets")

(struct internal-events (iter))

(define (make-internal-events c)
  (internal-events (make-topic-iterator c consumer-offsets-topic 'earliest)))

(define (get-events ie)
  (parse-events (get-records (internal-events-iter ie))))

(define (internal-events-supported? c)
  (and (get-offsets-topic c) #t))

(define (get-offsets-topic c)
  (findf
   (λ (t) (equal? (TopicMetadata-name t) consumer-offsets-topic))
   (Metadata-topics (client-metadata c))))

(define (parse-events records)
  (for*/vector ([record (in-vector records)]
                #:when (record-value record)
                [event (in-value
                        (parse-Internal
                         (record-key record)
                         (record-value record)))]
                #:when event)
    event))
