#lang racket/base

(require "core.rkt")

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
    res))
