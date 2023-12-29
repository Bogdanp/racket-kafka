#lang racket/base

(require racket/contract/base
         "core.rkt")

(define-record Producer
  ([id exact-nonnegative-integer?]
   [epoch exact-integer?]
   [last-sequence exact-integer?]
   [last-timestamp exact-integer?]
   [coordinator-epoch exact-integer?]
   [current-txn-start-offset exact-integer?]))

(define-record PartitionProducers
  ([id exact-nonnegative-integer?]
   [error-code error-code/c]
   [error-message (or/c #f string?)]
   [producers (listof Producer?)]))

(define-record TopicProducers
  ([name string?]
   [partitions (listof PartitionProducers?)]))

(define-record DescribedProducers
  ([throttle-time-ms (or/c #f exact-nonnegative-integer?)]
   [topics (listof TopicProducers?)]))

(define-request DescribeProducers
  (topics)
  #:code 61
  #:version 0
  #:flexible #t
  #:response proto:DescribeProducersResponseV0
  (lambda (topics)
    (define topics-data
      (for/list ([(topic partitions) (in-hash topics)])
        `((CompactTopicName_1 . ,topic)
          (CompactArrayLen_1 . ,(length partitions))
          (PartitionID_1 . ,partitions)
          (Tags_1 . ,(hash)))))

    (with-output-bytes
      (proto:un-DescribeProducersRequestV0
       `((CompactArrayLen_1 . ,(hash-count topics))
         (DescribeProducersRequestTopicV0_1 . ,topics-data)
         (Tags_1 . ,(hash))))))
  (lambda (res)
    (make-DescribedProducers
     #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
     #:topics (for/list ([topic (in-list (ref 'DescribeProducersResponseTopicV0_1 res))])
                (make-TopicProducers
                 #:name (ref 'CompactTopicName_1 topic)
                 #:partitions (for/list ([part (in-list (ref 'DescribeProducersResponseTopicPartitionV0_1 topic))])
                                (make-PartitionProducers
                                 #:id (ref 'PartitionID_1 part)
                                 #:error-code (ref 'ErrorCode_1 part)
                                 #:error-message (ref 'CompactErrorMessage_1 part)
                                 #:producers (for/list ([prod (in-list (ref 'DescribeProducersResponseProducerV0_1 part))])
                                               (make-Producer
                                                #:id (ref 'ProducerID_1 prod)
                                                #:epoch (ref 'ProducerEpoch_1 prod)
                                                #:last-sequence (ref 'LastSequence_1 prod)
                                                #:last-timestamp (ref 'LastTimestamp_1 prod)
                                                #:coordinator-epoch (ref 'CoordinatorEpoch_1 prod)
                                                #:current-txn-start-offset (ref 'CurrentTxnStartOffset_1 prod))))))))))
