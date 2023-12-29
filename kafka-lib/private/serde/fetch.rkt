#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         racket/contract/base
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
  #:response read-FetchResponseV4
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

;; Kafka simply lifts batches off disk, so it's likely that responses
;; will be truncated.  This procedure has to treat parse failures as
;; signals that all the data that could have been read, has been.
;;
;; xref: https://kafka.apache.org/documentation/#impl_reads
(define (read-FetchResponseV4 in)
  ;; ThrottleTimeMs
  (res-bind
   (parse-i32be in)
   (lambda (throttle-time-ms)
     ;; ArrayLen
     (res-bind
      (parse-i32be in)
      (lambda (len)
        ;; FetchResponseDataV4{ArrayLen_1}
        (define topics
          (let loop ([remaining len] [topics null])
            (cond
              [(> remaining 0)
               (define topic
                 (with-handlers ([exn:fail? (λ (_) #f)])
                   (proto:FetchResponseDataV4 in)))
               (if topic
                   (loop
                    (sub1 remaining)
                    (cons topic topics))
                   (reverse topics))]
              [else
               (reverse topics)])))
        `((ThrottleTimeMs_1 . ,throttle-time-ms)
          (ArrayLen_1 . ,len)
          (FetchResponseDataV4_1 . ,topics)))))))
