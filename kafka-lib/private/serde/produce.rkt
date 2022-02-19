#lang racket/base

(require racket/contract
         "core.rkt")

(define-record ProduceResponsePartition
  ([error-code error-code/c]
   [index exact-nonnegative-integer?]
   [offset exact-integer?]))

(define-record RecordResult
  ([topic string?]
   [partition ProduceResponsePartition?]))

(define-record ProduceResponseTopic
  ([name string?]
   [partitions (listof ProduceResponsePartition?)]))

(define-record ProduceResponse
  ([topics (listof ProduceResponseTopic?)]))

(define-record PartitionData
  ([index exact-integer?]
   [batch bytes?]))

(define-record TopicData
  ([name string?]
   [partitions (listof PartitionData?)]))

(define-request Produce
  (data
   [acks 'none]
   [timeout-ms 30000])
  #:code 0
  #:version 3
  #:response proto:ProduceResponseV3
  #:immed-response (λ (_data acks _timeout-ms)
                     (and (eq? acks 'none)
                          (ProduceResponse null)))
  enc-producev3
  dec-producev2)

(define (acks->integer acks)
  (case acks
    [(none) 0]
    [(leader) 1]
    [(all) -1]
    [else (raise-argument-error 'acks->integer "(or/c -1 0 1)" acks)]))

(define (enc-producev3 data acks timeout-ms)
  (with-output-bytes
    (proto:un-ProduceRequestV3
     `((TransactionalID_1 . nil)
       (Acks_1 . ,(acks->integer acks))
       (TimeoutMs_1 . ,timeout-ms)
       (ArrayLen_1 . ,(length data))
       (TopicData_1 . ,(for/list ([d (in-list data)])
                         (define parts (TopicData-partitions d))
                         `((TopicName_1 . ,(TopicData-name d))
                           (ArrayLen_1 . ,(length parts))
                           (PartitionData_1 . ,(for/list ([p (in-list parts)])
                                                 `((PartitionIndex_1 . ,(PartitionData-index p))
                                                   (Records_1 . ,(PartitionData-batch p))))))))))))

(define (dec-producev2 res)
  (ProduceResponse
   (for/list ([t (in-list (ref 'ProduceResponseDataV2_1 res))])
     (ProduceResponseTopic
      (ref 'TopicName_1 t)
      (for/list ([p (in-list (ref 'PartitionResponseV2_1 t))])
        (ProduceResponsePartition
         (ref 'ErrorCode_1 p)
         (ref 'PartitionIndex_1 p)
         (ref 'Offset_1 p)))))))
