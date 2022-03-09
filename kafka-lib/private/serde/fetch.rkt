#lang racket/base

(require racket/contract
         "core.rkt")

(define-record FetchResponsePartition
  ([id exact-nonnegative-integer?]
   [(error-code 0) error-code/c]
   [high-watermark exact-nonnegative-integer?]
   [last-stable-offset exact-nonnegative-integer?]
   [batches list?]))

(define-record FetchResponse
  ([throttle-time-ms exact-nonnegative-integer?]
   [topics (hash/c string? (listof FetchResponsePartition?))]))

(define-record TopicPartition
  ([id exact-nonnegative-integer?]
   [(offset 0) exact-nonnegative-integer?]
   [(max-bytes (* 100 1024 1024)) exact-nonnegative-integer?]))

(define-request Fetch
  (topic-partitions
   max-wait-ms
   [min-bytes 0]
   [max-bytes (* 100 1024 1024)]
   [isolation-level 'read-committed])
  #:code 1

  #:version 4
  #:response proto:FetchResponseV4
  (lambda (topic-partitions max-wait-ms min-bytes max-bytes isolation-level)
    (with-output-bytes
      (proto:un-FetchRequestV4
       `((ReplicaID_1 . -1)
         (MaxWaitMs_1 . ,max-wait-ms)
         (MinBytes_1 . ,min-bytes)
         (MaxBytes_1 . ,max-bytes)
         (IsolationLevel_1 . ,(case isolation-level
                                [(read-uncommitted) 0]
                                [(read-committed) 1]))
         (ArrayLen_1 . ,(hash-count topic-partitions))
         (FetchTopicV4_1 . ,(for/list ([(topic partitions) (in-hash topic-partitions)])
                              `((TopicName_1 . ,topic)
                                (ArrayLen_1 . ,(length partitions))
                                (FetchPartitionV4_1 . ,(for/list ([p (in-list partitions)])
                                                         `((PartitionID_1 . ,(TopicPartition-id p))
                                                           (FetchOffset_1 . ,(TopicPartition-offset p))
                                                           (MaxBytes_1 . ,(TopicPartition-max-bytes p))))))))))))
  (lambda (res)
    (make-FetchResponse
     #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
     #:topics (for/hash ([t (in-list (ref 'FetchResponseDataV4_1 res))])
                (define topic (ref 'TopicName_1 t))
                (define parts
                  (for/list ([p (in-list (ref 'FetchResponsePartitionV4_1 t))])
                    (make-FetchResponsePartition
                     #:id (ref 'PartitionID_1 p)
                     #:error-code (ref 'ErrorCode_1 p)
                     #:high-watermark (ref 'HighWatermark_1 p)
                     #:last-stable-offset (ref 'LastStableOffset_1 p)
                     #:batches (ref 'Records_1 p))))
                (values topic parts)))))
