#lang racket/base

(require "core.rkt")

(define-record TopicPartition
  ([id exact-nonnegative-integer?]
   [(offset 0) exact-nonnegative-integer?]
   [(max-bytes (* 1 1024 1024 1024)) exact-nonnegative-integer?]))

(define-request Fetch
  (topic-partitions max-wait-ms [min-bytes 0])
  #:code 1

  #:version 0
  #:response proto:FetchResponseV0
  (lambda (topic-partitions max-wait-ms min-bytes)
    (with-output-bytes
      (proto:un-FetchRequestV0
       `((ReplicaID_1 . -1)
         (MaxWaitMs_1 . ,max-wait-ms)
         (MinBytes_1 . ,min-bytes)
         (ArrayLen_1 . ,(hash-count topic-partitions))
         (FetchTopic_1 . ,(for/list ([(topic partitions) (in-hash topic-partitions)])
                            `((TopicName_1 . ,topic)
                              (ArrayLen_1 . ,(length partitions))
                              (FetchPartition_1 . ,(for/list ([p (in-list partitions)])
                                                     `((PartitionID_1 . ,(TopicPartition-id p))
                                                       (FetchOffset_1 . ,(TopicPartition-offset p))
                                                       (MaxBytes_1 . ,(TopicPartition-max-bytes p))))))))))))
  (lambda (res)
    res))
