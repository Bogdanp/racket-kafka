#lang racket/base

(require racket/contract
         racket/match
         racket/promise
         "private/batch.rkt"
         "private/client.rkt"
         "private/common.rkt"
         "private/error.rkt"
         "private/logger.rkt"
         "private/record.rkt"
         "private/serde.rkt")

;; iterator ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (contract-out
  [struct record ([partition-id (or/c #f exact-integer?)]
                  [offset exact-integer?]
                  [timestamp exact-integer?]
                  [key (or/c #f bytes?)]
                  [value (or/c #f bytes?)]
                  [headers (hash/c string? bytes?)])]

  [topic-iterator? (-> any/c boolean?)]
  [make-topic-iterator (->* (client? string?)
                            (offset/c)
                            topic-iterator?)]
  [reset-topic-iterator! (-> topic-iterator? offset/c void?)]
  [get-records (->* (topic-iterator?)
                    (#:max-bytes exact-positive-integer?)
                    (vectorof record?))]))

(define offset/c
  (or/c 'earliest 'latest
        (list/c 'timestamp exact-nonnegative-integer?)
        (list/c 'exact exact-nonnegative-integer?)
        (list/c 'recent exact-positive-integer?)))

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
  (match offset
    [(or 'earliest 'latest)
     (find-offsets c topic-name offset)]
    [`(timestamp ,timestamp)
     (find-offsets c topic-name timestamp)]
    [`(recent ,n)
     (for/hasheq ([(pid offset) (in-hash (find-offsets c topic-name 'latest))])
       (values pid (max 0 (- offset n))))]
    [`(exact ,offset)
     (for/hasheqv ([part (TopicMetadata-partitions (get-topic c topic-name))])
       (define pid (PartitionMetadata-id part))
       (values pid offset))]))

(define (find-offsets c topic-name timestamp)
  (define nodes-by-t&p
    (collect-nodes-by-topic&pid
     (client-metadata c)
     (list topic-name)))
  (define pids-by-node
    (for/fold ([nodes (hasheqv)])
              ([(t&p node-id) (in-hash nodes-by-t&p)])
      (define pid (cdr t&p))
      (hash-update nodes node-id (λ (pids) (cons pid pids)) null)))
  (define promises
    (for/list ([(node-id pids) (in-hash pids-by-node)])
      (delay/thread
       (define conn (get-node-connection c node-id))
       (define topics
         (hash topic-name
               (for/hasheqv ([pid (in-list pids)])
                 (values pid timestamp))))
       (sync (make-ListOffsets-evt conn topics)))))
  (define offsets
    (for*/hasheqv ([promise (in-list promises)]
                   [res (in-value (force promise))]
                   [parts (in-hash-values res)]
                   [part (in-list parts)])
      (define pid (PartitionOffset-id part))
      (unless (zero? (PartitionOffset-error-code part))
        (raise-server-error (PartitionOffset-error-code part)))
      (values pid (PartitionOffset-offset part))))
  (log-kafka-debug
   "found offsets for topic ~s~n timestamp: ~s~n offsets: ~s"
   topic-name timestamp offsets)
  (cond
    ;; When searching by timestamp, if the timestamp exceeds the
    ;; timestamp of the latest record, or if the partition has no
    ;; data, then -1 is returned.  In those cases, perform another set
    ;; of requests to get the latest offsets for those partitions and
    ;; fill in the gaps.
    [(and (exact-integer? timestamp)
          (ormap negative? (hash-values offsets)))
     (define latest-offsets
       (find-offsets c topic-name 'latest))
     (for/hasheq ([(pid offset) (in-hash offsets)])
       (values pid (if (negative? offset)
                       (hash-ref latest-offsets pid 0)
                       offset)))]
    [else
     offsets]))

(define (get-records* c topic-name metadata offsets max-bytes)
  ;; While it is possible to batch requests per node, it's a bad idea
  ;; because one partition may starve out multiple smaller partitions
  ;; on the same node. Instead, just make one request per (node,
  ;; topic, partition) tuple.
  (define response-promises
    (for/list ([p (in-list (TopicMetadata-partitions metadata))]
               #:when (>= (PartitionMetadata-leader-id p) 0))
      (define node-id (PartitionMetadata-leader-id p))
      (define partition-id (PartitionMetadata-id p))
      (define topic-partition
        (make-TopicPartition
         #:id partition-id
         #:offset (hash-ref offsets partition-id 0)))
      (define topic-partitions
        (hash topic-name (list topic-partition)))
      (delay/thread
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
