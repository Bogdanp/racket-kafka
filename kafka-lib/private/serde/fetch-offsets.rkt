#lang racket/base

(require racket/contract
         "core.rkt")

(define-record GroupPartitionOffset
  ([id exact-nonnegative-integer?]
   [error-code error-code/c]
   [offset (or/c -1 exact-nonnegative-integer?)]
   [metadata string?]))

(define-record GroupOffsets
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [(error-code 0) error-code/c]
   [topics (hash/c string? (listof GroupPartitionOffset?))]))

(define-request FetchOffsets
  (group-id
   [topics (hash)])
  #:code 9

  #:version 1
  #:response proto:OffsetFetchResponseV1
  (make-enc-offset-fetch-v1 #f)
  dec-offset-fetch-v1

  #:version 2
  #:response proto:OffsetFetchResponseV2
  (make-enc-offset-fetch-v1)
  dec-offset-fetch-v2

  #:version 3
  #:response proto:OffsetFetchResponseV3
  (make-enc-offset-fetch-v1)
  dec-offset-fetch-v3

  #:version 4
  #:response proto:OffsetFetchResponseV4
  (make-enc-offset-fetch-v1)
  dec-offset-fetch-v3)

(define ((make-enc-offset-fetch-v1 [supports-null? #t]) group-id partitions-by-topic)
  (define topics
    (for/list ([(topic partitions) (in-hash partitions-by-topic)])
      `((TopicName_1 . ,topic)
        (ArrayLen_1 . ,(length partitions))
        (OffsetFetchRequestPartitionV1_1 . ,partitions))))
  (with-output-bytes
    (proto:un-OffsetFetchRequestV1
     `((GroupID_1 . ,group-id)
       (ArrayLen_1 . ,(if (and supports-null? (null? topics))
                          -1
                          (hash-count partitions-by-topic)))
       (OffsetFetchRequestTopicV1_1 . ,topics)))))

(define (dec-offset-fetch-v1 res)
  (make-GroupOffsets
   #:topics (dec-offset-fetch-v1-topics res)))

(define (dec-offset-fetch-v2 res)
  (make-GroupOffsets
   #:error-code (ref 'ErrorCode_1 res)
   #:topics (dec-offset-fetch-v1-topics res)))

(define (dec-offset-fetch-v3 res)
  (make-GroupOffsets
   #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
   #:error-code (ref 'ErrorCode_1 res)
   #:topics (dec-offset-fetch-v1-topics res)))

(define (dec-offset-fetch-v1-topics res)
  (for/hash ([t (in-list (ref 'OffsetFetchResponseTopicV1_1 res))])
    (define topic (ref 'TopicName_1 t))
    (values topic (for/list ([p (in-list (ref 'OffsetFetchResponsePartitionV1_1 t))])
                    (make-GroupPartitionOffset
                     #:id (ref 'PartitionID_1 p)
                     #:error-code (ref 'ErrorCode_1 p)
                     #:offset (ref 'CommittedOffset_1 p)
                     #:metadata (ref 'OffsetFetchMetadata_1 p))))))
