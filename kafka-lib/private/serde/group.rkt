#lang racket/base

(require racket/contract/base
         racket/port
         (prefix-in consumer: "../protocol-consumer.bnf")
         "authorized-operation.rkt"
         "core.rkt")

(provide
 (contract-out
  [Group-topics (-> Group? (listof string?))]
  [GroupMember-topics (-> GroupMember? (listof string?))]
  [GroupMember-assignments (-> GroupMember? (hash/c string? (listof exact-integer?)))]))

(define-record GroupMember
  ([id string?]
   [client-id string?]
   [client-host string?]
   [metadata bytes?]
   [assignment bytes?]))

(define-record Group
  ([(error-code 0) error-code/c]
   [id string?]
   [(state #f) (or/c #f string?)]
   [protocol-type string?]
   [(protocol-data #f) (or/c #f string?)]
   [(members null) (listof GroupMember?)]
   [(authorized-operations null) (listof authorized-operation/c)]))

(define (Group-topics g)
  (case (Group-protocol-type g)
    [("consumer")
     (for*/fold ([topics (hash)] #:result (sort (hash-keys topics) string<?))
                ([m (in-list (Group-members g))]
                 [t (in-list (GroupMember-topics m))])
       (hash-set topics t #t))]
    [else null]))

(define (GroupMember-topics m)
  (with-handlers ([exn:fail? (λ (_) null)])
    (define res
      (call-with-input-bytes (GroupMember-metadata m)
        consumer:MemberMetadata))
    (ref 'TopicName_1 res)))

(define (GroupMember-assignments m)
  (with-handlers ([exn:fail? (λ (_) (hash))])
    (define res
      (call-with-input-bytes (GroupMember-assignment m)
        consumer:MemberAssignment))
    (for/hash ([topic (in-list (ref 'Assignment_1 res))])
      (values
       (ref 'TopicName_1 topic)
       (ref 'PartitionID_1 topic)))))
