#lang racket/base

(require racket/contract
         racket/promise
         "private/batch.rkt"
         "private/client.rkt"
         "private/common.rkt"
         "private/error.rkt"
         "private/record.rkt"
         "private/serde.rkt")

;; iterator ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 topic-iterator?
 (contract-out
  [make-topic-iterator (->* (client? string?)
                            (#:offset (or/c 'earliest 'latest exact-nonnegative-integer?)
                             #:max-bytes exact-positive-integer?)
                            topic-iterator?)]
  [stop-topic-iterator (-> topic-iterator? void?)]))

(struct topic-iterator (ch [thd #:mutable])
  #:transparent
  #:property prop:evt (struct-field-index ch))

(define (make-topic-iterator c topic-name
                             #:offset [offset 'latest]
                             #:max-bytes [max-bytes (* 1 1024 1024)])
  (define topic-metadata
    (get-topic c topic-name))
  (unless topic-metadata
    (error 'make-topic-iterator "topic not found"))
  (define offsets
    (get-offsets c topic-name offset))
  (define ch (make-channel))
  (define thd
    (thread
     (lambda ()
       (let loop ([deadline 0])
         (define next-deadline
           (with-handlers ([exn:break?
                            (lambda (e)
                              ((error-display-handler) (format "iterator: ~a" (exn-message e)) e)
                              (+ (current-inexact-monotonic-milliseconds) 1000))])
             (sync
              (handle-evt
               (thread-receive-evt)
               (λ (_) #f))
              (replace-evt
               (alarm-evt deadline #t)
               (lambda (_)
                 (define records
                   (get-records c topic-name topic-metadata offsets max-bytes))
                 (if (zero? (vector-length records))
                     (pure-evt (+ (current-inexact-monotonic-milliseconds) 1000))
                     (handle-evt
                      (channel-put-evt ch records)
                      (λ (_) (current-inexact-monotonic-milliseconds)))))))))
         (when next-deadline
           (loop next-deadline))))))
  (topic-iterator ch thd))

(define (stop-topic-iterator i)
  (thread-send (topic-iterator-thd i) '(stop)))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (get-topic c name)
  (findf
   (λ (t) (equal? (TopicMetadata-name t) name))
   (Metadata-topics (client-metadata c))))

(define (get-offsets c topic-name offset)
  (define offsets (make-hasheqv))
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
  (begin0 offsets
    (for* ([promise (in-list promises)]
           [res (in-value (force promise))]
           [parts (in-hash-values res)]
           [part (in-list parts)])
      (define pid (PartitionOffset-id part))
      (unless (zero? (PartitionOffset-error-code part))
        (raise-server-error (PartitionOffset-error-code part)))
      (hash-set! offsets pid (PartitionOffset-offset part)))))

(define (get-records c topic-name metadata offsets max-bytes)
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
  (define num-records
    (for*/sum ([response (in-list responses)]
               [parts (in-hash-values (FetchResponse-topics response))]
               [part (in-list parts)]
               [batch (in-list (FetchResponsePartition-batches part))])
      (vector-length (batch-records batch))))
  (for*/vector #:length num-records
               ([response (in-list responses)]
                [parts (in-hash-values (FetchResponse-topics response))]
                [part (in-list parts)]
                [batch (in-list (FetchResponsePartition-batches part))]
                [record (in-vector (batch-records batch))])
    record))
