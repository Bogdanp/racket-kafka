#lang racket/base

(require "core.rkt")

(define-request FetchOffsets
  (group-id topics)
  #:code 9

  #:version 0
  #:response proto:OffsetFetchResponseV0
  (lambda (group-id topics)
    (with-output-bytes
      (proto:un-OffsetFetchRequestV0
       `((GroupID_1 . ,group-id)
         (ArrayLen_1 . ,(hash-count topics))
         (OffsetFetchRequestTopicV0_1 . (for/list ([(topic partitions) (in-hash topics)])
                                          `((TopicName_1 . ,topic)
                                            (ArrayLen_1 . ,(length partitions))
                                            (OffsetFetchRequestPartitionV0_1 . ,partitions))))))))
  (lambda (res)
    (for/hash ([t (in-list (ref 'OffsetFetchRequestTopicV0_1 res))])
      (define topic (ref 'TopicName_1 t))
      (values topic (for/hash ([p (in-list (ref 'OffsetFetchRequestPartitionV0_1 t))])
                      (define pid (ref 'PartitionID_1 p))
                      (values pid (ref 'CommittedOffset_1 p)))))))
