#lang racket/base

(require "core.rkt")

(define-record PartitionOffset
  ([id exact-nonnegative-integer?]
   [error-code error-code/c]
   [timestamp exact-nonnegative-integer?]
   [offset exact-nonnegative-integer?]))

(define-request ListOffsets
  (topics [isolation-level 'read-committed])
  #:code 2

  #:version 2
  #:response proto:ListOffsetsResponseV2
  (lambda (topics isolation-level)
    (with-output-bytes
      (proto:un-ListOffsetsRequestV2
       `((ReplicaID_1 . -1)
         (IsolationLevel_1 . ,(case isolation-level
                                [(read-committed) 1]
                                [(read-uncommitted) 0]
                                [else (raise-argument-error 'ListOffsets "(or/c 'read-committed 'read-uncommitted)" isolation-level)]))
         (ArrayLen_1 . ,(hash-count topics))
         (ListOffsetsRequestTopicV2_1 . ,(for/list ([(topic partitions) (in-hash topics)])
                                           (define parts
                                             (for/list ([(pid timestamp) (in-hash partitions)])
                                               `((PartitionID_1 . ,pid)
                                                 (Timestamp_1 . ,(case timestamp
                                                                   [(latest) -1]
                                                                   [(earliest) -2]
                                                                   [else timestamp])))))

                                           `((TopicName_1 . ,topic)
                                             (ArrayLen_1 . ,(hash-count partitions))
                                             (ListOffsetsRequestPartitionV2_1 . ,parts))))))))
  (lambda (res)
    (for/hash ([t (in-list (ref 'ListOffsetsResponseTopicV2_1 res))])
      (define topic (ref 'TopicName_1 t))
      (values topic (for/list ([p (in-list (ref 'ListOffsetsResponsePartitionV2_1 t))])
                      (make-PartitionOffset
                       #:id (ref 'PartitionID_1 p)
                       #:error-code (ref 'ErrorCode_1 p)
                       #:timestamp (ref 'Timestamp_1 p)
                       #:offset (ref 'Offset_1 p)))))))
