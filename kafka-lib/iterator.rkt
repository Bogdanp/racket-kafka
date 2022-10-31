#lang racket/base

(require racket/contract
         racket/match
         racket/promise
         "private/batch.rkt"
         "private/client.rkt"
         "private/common.rkt"
         "private/error.rkt"
         "private/record.rkt"
         "private/serde.rkt")

;; iterator ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 record?
 record-partition-id
 record-offset
 record-timestamp
 record-key
 record-value

 topic-iterator?
 (contract-out
  [make-topic-iterator (->* (client? string?)
                            (offset/c)
                            topic-iterator?)]
  [reset-topic-iterator! (-> topic-iterator? offset/c void?)]
  [get-records (->* (topic-iterator?)
                    (#:max-bytes exact-positive-integer?)
                    (vectorof record?))]))

(define offset/c
  (or/c 'earliest 'latest exact-nonnegative-integer?))

(struct topic-iterator
  (client topic [metadata #:mutable] [offsets #:mutable]))

(define (make-topic-iterator c topic [offset 'latest])
  (define metadata
    (get-topic c topic))
  (unless metadata
    (error 'make-topic-iterator "topic not found"))
  (define offsets
    (get-offsets c topic offset))
  (topic-iterator c topic metadata offsets))

(define (reset-topic-iterator! it offset)
  (match-define (topic-iterator c topic _ _) it)
  (define metadata (get-topic c topic))
  (unless metadata
    (error 'reset-topic-iterator! "topic not found"))
  (define offsets
    (get-offsets c topic offset))
  (set-topic-iterator-metadata! it metadata)
  (set-topic-iterator-offsets! it offsets))

(define (get-records it #:max-bytes [max-bytes (* 1 1024 1024)])
  (match-define (topic-iterator c topic metadata offsets) it)
  (define-values (records updated-offsets)
    (get-records* c topic metadata offsets max-bytes))
  (begin0 records
    (set-topic-iterator-offsets! it updated-offsets)))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (get-topic c name)
  (findf
   (λ (t) (equal? (TopicMetadata-name t) name))
   (Metadata-topics (reload-metadata c))))

(define (get-offsets c topic-name offset)
  (case offset
    [(earliest latest)
     (define nodes-by-t&p (collect-nodes-by-topic&pid (client-metadata c) (list topic-name)))
     (define pids-by-node
       (for/fold ([nodes (hasheqv)])
                 ([(t&p node-id) (in-hash nodes-by-t&p)])
         (define pid (cdr t&p))
         (hash-update nodes node-id (λ (pids) (cons pid pids)) null)))
     (define promises
       (for/list ([(node-id pids) (in-hash pids-by-node)])
         (delay/thread
          (define conn (get-node-connection c node-id))
          (sync (make-ListOffsets-evt conn (hash topic-name (for/hasheqv ([pid (in-list pids)])
                                                              (values pid offset))))))))
     (for*/hasheqv ([promise (in-list promises)]
                    [res (in-value (force promise))]
                    [parts (in-hash-values res)]
                    [part (in-list parts)])
       (define pid (PartitionOffset-id part))
       (unless (zero? (PartitionOffset-error-code part))
         (raise-server-error (PartitionOffset-error-code part)))
       (values pid (PartitionOffset-offset part)))]
    [else
     (unless (exact-nonnegative-integer? offset)
       (raise-argument-error 'get-offsets "(or/c 'earliest 'latest exact-nonnegative-integer?)" offset))
     (for*/hasheqv ([part (TopicMetadata-partitions (get-topic c topic-name))])
       (define pid (PartitionMetadata-id part))
       (values pid offset))]))

(define (get-records* c topic-name metadata offsets max-bytes)
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
       (define topic-partitions (hash topic-name partitions))
       (sync (make-Fetch-evt (get-node-connection c node-id) topic-partitions 1000 0 max-bytes)))))
  (define responses
    (for/list ([promise (in-list response-promises)])
      (with-handlers ([exn:fail? (λ (e)
                                   (begin0 #f
                                     ((error-display-handler)
                                      (format "get-internal-events: ~a" (exn-message e))
                                      e)))])
        (force promise))))
  (define records
    (for*/vector ([response (in-list responses)]
                  #:when response
                  [parts (in-hash-values (FetchResponse-topics response))]
                  [part (in-list parts)]
                  [pid (in-value (FetchResponsePartition-id part))]
                  [batch (in-list (FetchResponsePartition-batches part))]
                  [record (in-vector (batch-records batch))]
                  #:when (>= (record-offset record)
                             (hash-ref offsets pid 0)))
      (begin0 record
        (set-record-partition-id! record pid))))
  (define updated-offsets
    (for*/fold ([offsets offsets])
               ([response (in-list responses)]
                #:when response
                [parts (in-hash-values (FetchResponse-topics response))]
                [part (in-list parts)]
                [b (in-list (FetchResponsePartition-batches part))]
                #:unless (zero? (batch-size b)))
      (define pid (FetchResponsePartition-id part))
      (define size (batch-size b))
      (define last-record (vector-ref (batch-records b) (sub1 size)))
      (hash-set offsets pid (add1 (record-offset last-record)))))
  (values records updated-offsets))
