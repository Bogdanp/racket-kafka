#lang racket/base

(require racket/contract
         "core.rkt")

(define-record PartitionOffset/Group
  ([id exact-nonnegative-integer?]
   [error-code error-code/c]
   [offset (or/c -1 exact-nonnegative-integer?)]
   [metadata string?]))

(define-request FetchOffsets
  (group-id topics)
  #:code 9

  #:version 1
  #:response proto:OffsetFetchResponseV1
  (lambda (group-id topics)
    (with-output-bytes
      (proto:un-OffsetFetchRequestV1
       `((GroupID_1 . ,group-id)
         (ArrayLen_1 . ,(hash-count topics))
         (OffsetFetchRequestTopicV1_1 . ,(for/list ([(topic partitions) (in-hash topics)])
                                           `((TopicName_1 . ,topic)
                                             (ArrayLen_1 . ,(length partitions))
                                             (OffsetFetchRequestPartitionV1_1 . ,partitions))))))))
  (lambda (res)
    (for/hash ([t (in-list (ref 'OffsetFetchResponseTopicV1_1 res))])
      (define topic (ref 'TopicName_1 t))
      (values topic (for/list ([p (in-list (ref 'OffsetFetchResponsePartitionV1_1 t))])
                      (make-PartitionOffset/Group
                       #:id (ref 'PartitionID_1 p)
                       #:error-code (ref 'ErrorCode_1 p)
                       #:offset (ref 'CommittedOffset_1 p)
                       #:metadata (ref 'OffsetFetchMetadata_1 p)))))))
