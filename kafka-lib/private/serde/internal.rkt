#lang racket/base

(require racket/contract/base
         racket/port
         (prefix-in consumer: "../protocol-consumer.bnf")
         (prefix-in internal: "../protocol-internal.bnf")
         "core.rkt")

(provide
 Internal/c
 (contract-out
  [parse-Internal (-> bytes? bytes? (or/c #f Internal/c))]
  [InternalGroupMember-topics (-> InternalGroupMember? (listof string?))]
  [InternalGroupMember-assignments (-> InternalGroupMember? (hash/c string? (listof exact-nonnegative-integer?)))]))

(define-record InternalOffsetCommit
  ([group string?]
   [topic string?]
   [partition-id exact-nonnegative-integer?]
   [offset exact-integer?]))

(define-record InternalGroupMember
  ([id string?]
   [client-id string?]
   [client-host string?]
   [(rebalance-timeout #f) (or/c #f exact-integer?)]
   [session-timeout exact-integer?]
   [(subscription #f) (or/c #f bytes?)]
   [(assignment #f) (or/c #f bytes?)]))

(define (InternalGroupMember-topics m)
  (with-handlers ([exn:fail? (λ (_) null)])
    (define res
      (call-with-input-bytes (InternalGroupMember-subscription m)
        consumer:MemberMetadata))
    (ref 'TopicName_1 res)))

(define (InternalGroupMember-assignments m)
  (with-handlers ([exn:fail? (λ (_) (hash))])
    (define res
      (call-with-input-bytes (InternalGroupMember-assignment m)
        consumer:MemberAssignment))
    (for/hash ([topic (in-list (ref 'Assignment_1 res))])
      (values
       (ref 'TopicName_1 topic)
       (ref 'PartitionID_1 topic)))))

(define-record InternalGroupMetadata
  ([group string?]
   [generation exact-integer?]
   [protocol-type (or/c #f string?)]
   [protocol-data (or/c #f string?)]
   [leader (or/c #f string?)]
   [members (listof InternalGroupMember?)]))

(define Internal/c
  (or/c InternalOffsetCommit? InternalGroupMetadata?))

(define (parse-Internal key-bs value-bs)
  (define-values (type key)
    (call-with-input-bytes key-bs
      (lambda (in)
        (case (read-bytes 2 in)
          [(#"\0\0" #"\0\1")
           (values 'commit (internal:OffsetCommitKeyV01 in))]
          [(#"\0\2")
           (values 'group (internal:GroupMetadataKeyV2 in))]
          [else
           (values #f #f)]))))
  (case type
    [(commit)
     (define value
       (call-with-input-bytes value-bs
         (lambda (in)
           (case (read-bytes 2 in)
             [(#"\0\0" #"\0\1" #"\0\2" #"\0\3")
              (internal:OffsetCommitValueV0 in)]
             [else #f]))))
     (and value
          (make-InternalOffsetCommit
           #:group (ref 'Group_1 key)
           #:topic (ref 'Topic_1 key)
           #:partition-id (ref 'PartitionID_1 key)
           #:offset value))]
    [(group)
     (define-values (members-key value)
       (call-with-input-bytes value-bs
         (lambda (in)
           (case (read-bytes 2 in)
             [(#"\0\0") (values 'MemberMetadataV0_1 (internal:GroupMetadataValueV0 in))]
             [(#"\0\1") (values 'MemberMetadataV1_1 (internal:GroupMetadataValueV1 in))]
             [(#"\0\2") (values 'MemberMetadataV2_1 (internal:GroupMetadataValueV2 in))]
             [(#"\0\3") (values 'MemberMetadataV3_1 (internal:GroupMetadataValueV3 in))]
             [else (values #f #f)]))))
     (and value
          (make-InternalGroupMetadata
           #:group key
           #:generation (ref 'Generation_1 value)
           #:protocol-type (ref 'ProtocolType_1 value)
           #:protocol-data (ref 'Protocol_1 value)
           #:leader (ref 'Leader_1 value)
           #:members (for/list ([m (in-list (ref members-key value))])
                       (make-InternalGroupMember
                        #:id (ref 'MemberID_1 m)
                        #:client-id (ref 'ClientID_1 m)
                        #:client-host (ref 'ClientHost_1 m)
                        #:rebalance-timeout (ref 'RebalanceTimeout_1 m)
                        #:session-timeout (ref 'SessionTimeout_1 m)
                        #:subscription (ref 'Subscription_1 m)
                        #:assignment (ref 'Assignment_1 m)))))]
    [else
     #f]))

(module+ test
  (require rackunit)

  (test-case "invalid key & value"
    (check-false (parse-Internal #"" #"")))

  (test-case "valid key, bad value"
    (check-false (parse-Internal #"\0\2\0\rexample-group" #"")))

  (test-case "valid offset commit"
    (check-equal?
     (parse-Internal
      #"\0\1\0\rexample-group\0\rexample-topic\0\0\0\1"
      #"\0\3\0\0\0\0\0\0\b\0\377\377\377\377\0\0\0\0\1\203\340\243\341\f")
     (make-InternalOffsetCommit
      #:group "example-group"
      #:topic "example-topic"
      #:partition-id 1
      #:offset 2048)))

  (test-case "valid group metadata"
    (check-equal?
     (parse-Internal
      #"\0\2\0\rexample-group"
      #"\0\3\0\bconsumer\0\0\0\27\0\5range\0001racket-kafka-35d27953-bcfa-4be9-a5b0-58ae5cebe04a\0\0\1\203\340\235'\21\0\0\0\2\0001racket-kafka-35d27953-bcfa-4be9-a5b0-58ae5cebe04a\377\377\0\fracket-kafka\0\v/172.18.0.1\0\0u0\0\0u0\0\0\0\31\0\0\0\0\0\1\0\rexample-topic\0\0\0\0\0\0\0!\0\0\0\0\0\1\0\rexample-topic\0\0\0\1\0\0\0\0\0\0\0\0\0001racket-kafka-1f34c0ba-75b8-4d68-a4f5-63afcaa16522\377\377\0\fracket-kafka\0\v/172.18.0.1\0\0u0\0\0u0\0\0\0\31\0\0\0\0\0\1\0\rexample-topic\0\0\0\0\0\0\0!\0\0\0\0\0\1\0\rexample-topic\0\0\0\1\0\0\0\1\0\0\0\0")
     (make-InternalGroupMetadata
      #:group "example-group"
      #:generation 23
      #:protocol-type "consumer"
      #:protocol-data "range"
      #:leader "racket-kafka-35d27953-bcfa-4be9-a5b0-58ae5cebe04a"
      #:members (list
                 (make-InternalGroupMember
                  #:id "racket-kafka-35d27953-bcfa-4be9-a5b0-58ae5cebe04a"
                  #:client-id "racket-kafka"
                  #:client-host "/172.18.0.1"
                  #:rebalance-timeout 30000
                  #:session-timeout 30000
                  #:subscription #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\0"
                  #:assignment #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\1\0\0\0\0\0\0\0\0")
                 (make-InternalGroupMember
                  #:id "racket-kafka-1f34c0ba-75b8-4d68-a4f5-63afcaa16522"
                  #:client-id "racket-kafka"
                  #:client-host "/172.18.0.1"
                  #:rebalance-timeout 30000
                  #:session-timeout 30000
                  #:subscription #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\0"
                  #:assignment #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\1\0\0\0\1\0\0\0\0")))))

  (test-case "group member accessors"
    (define m
      (make-InternalGroupMember
       #:id "racket-kafka-35d27953-bcfa-4be9-a5b0-58ae5cebe04a"
       #:client-id "racket-kafka"
       #:client-host "/172.18.0.1"
       #:rebalance-timeout 30000
       #:session-timeout 30000
       #:subscription #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\0"
       #:assignment #"\0\0\0\0\0\1\0\rexample-topic\0\0\0\1\0\0\0\0\0\0\0\0"))
    (check-equal?
     (InternalGroupMember-topics m)
     '("example-topic"))
    (check-equal?
     (InternalGroupMember-assignments m)
     (hash "example-topic" '(0)))))
