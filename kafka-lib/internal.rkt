#lang racket/base

(require racket/contract
         "iterator.rkt"
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

(struct internal-events (iter)
  #:property prop:evt (lambda (self)
                        (handle-evt (internal-events-iter self) parse-events)))

(define (make-internal-events c)
  (internal-events (make-topic-iterator c consumer-offsets-topic #:initial-offset 'earliest)))

(define (stop-internal-events it)
  (stop-topic-iterator (internal-events-iter it)))

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
