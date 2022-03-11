#lang racket/base

(require "core.rkt")

(define-record CommitPartition
  ([id exact-nonnegative-integer?]
   [offset exact-nonnegative-integer?]
   [(timestamp (current-milliseconds)) exact-nonnegative-integer?]
   [(metadata "") string?]))

(define-record CommitPartitionResult
  ([id exact-nonnegative-integer?]
   [error-code error-code/c]))

(define-request Commit
  (group-id
   generation-id
   member-id
   topics)
  #:code 8

  #:version 1
  #:response proto:OffsetCommitResponseV1
  (lambda (group-id generation-id member-id topics)
    (with-output-bytes
      (proto:un-OffsetCommitRequestV1
       `((GroupID_1 . ,group-id)
         (GenerationID_1 . ,generation-id)
         (MemberID_1 . ,member-id)
         (ArrayLen_1 . ,(hash-count topics))
         (OffsetCommitRequestTopicV1_1 . ,(for/list ([(topic partitions) (in-hash topics)])
                                            (define partition-data
                                              (for/list ([part (in-list partitions)])
                                                `((PartitionID_1 . ,(CommitPartition-id part))
                                                  (CommittedOffset_1 . ,(CommitPartition-offset part))
                                                  (CommitTimestamp_1 . ,(CommitPartition-timestamp part))
                                                  (CommittedMetadata_1 . ,(CommitPartition-metadata part)))))

                                            `((TopicName_1 . ,topic)
                                              (ArrayLen_1 . ,(length partitions))
                                              (OffsetCommitRequestPartitionV1_1 . ,partition-data))))))))
  (lambda (res)
    (for/hash ([t (in-list (ref 'OffsetCommitResponseTopicV1_1 res))])
      (define topic (ref 'TopicName_1 t))
      (values topic (for/list ([p (in-list (ref 'OffsetCommitResponsePartitionV1_1 t))])
                      (make-CommitPartitionResult
                       #:id (ref 'PartitionID_1 p)
                       #:error-code (ref 'ErrorCode_1 p)))))))
