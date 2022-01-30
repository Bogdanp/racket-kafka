#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse)
         racket/contract
         "connection.rkt"
         "help.rkt"
         (prefix-in proto: "protocol.bnf"))

(define-syntax (define-response stx)
  (syntax-parse stx
    [(_ id:id ([fld:id ctc:expr] ...))
     #'(begin
         (provide
          (contract-out
           [struct id ([fld ctc] ...)]))
         (struct id (fld ...) #:transparent))]))

(begin-for-syntax
  (define-syntax-class request-arg
    (pattern id:id)
    (pattern [id:id def:expr])))

(define-syntax (define-request stx)
  (syntax-parse stx
    [(_ id:id
        (arg:request-arg ...)
        (~seq #:code key:number)
        (~seq #:version version-num:number parser:expr encoder:expr decoder:expr) ...+)
     #:with make-evt-id (format-id #'id "make-~a-evt" #'id)
     #:with version-rng-id (format-id #'id "~a-supported-versions" #'id)
     #:with version-rng (let ([versions (map syntax->datum (syntax-e #'(version-num ...)))])
                          (with-syntax ([min-v (apply min versions)]
                                        [max-v (apply max versions)])
                            #'(version-range min-v max-v)))
     #'(begin
         (provide make-evt-id)
         (define version-rng-id version-rng)
         (define (make-evt conn v data parser-proc proc)
           (handle-evt
            (make-request-evt
             conn
             #:key key
             #:version v
             #:data data
             #:parser parser-proc)
            (lambda (res)
              (define err-code (or (opt 'ErrorCode_1 res) 0))
              (unless (zero? err-code)
                ;; FIXME: convert codes to messages
                (raise (kafka-err err-code "request failed")))
              (proc res))))
         (define (make-evt-id conn arg ...)
           (case (find-best-version conn key version-rng-id)
             [(version-num) (make-evt conn version-num (encoder arg.id ...) parser decoder)] ...
             [else (error 'make-evt-id "no supported version")])))]))

;; metadata ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define error-code/c exact-integer?)
(define port/c (integer-in 0 65535))

(define-response BrokerMetadata
  ([node-id exact-nonnegative-integer?]
   [host string?]
   [port port/c]
   [rack string?]))

(define-response PartitionMetadata
  ([error-code error-code/c]
   [id exact-nonnegative-integer?]
   [leader? boolean?]))

(define-response TopicMetadata
  ([error-code error-code/c]
   [name string?]
   [internal? boolean?]
   [partitions (listof PartitionMetadata?)]))

(define-response Metadata
  ([brokers (listof BrokerMetadata?)]
   [topics (listof TopicMetadata?)]
   [controller-id exact-integer?]))

(define-request Metadata (topics)
  #:code 3
  #:version 1 proto:MetadataResponseV1
  (lambda (topics)
    (if (null? topics)
        null32
        (with-output-bytes
          (proto:un-MetadataRequestV1
           `((ArrayLen_1   . ,(length topics))
             (TopicName_1  . ,(map kstring topics)))))))
  (lambda (res)
    (Metadata
     (for/list ([broker (in-list (ref 'Broker_1 'Brokers_1 res))])
       (BrokerMetadata
        (ref 'NodeID_1 broker)
        (kunstring (ref 'Host_1 broker))
        (ref 'Port_1 broker)
        (kunstring (ref 'Rack_1 broker))))
     (for/list ([topic (in-list (ref 'TopicMetadata_1 'TopicMetadatas_1 res))])
       (TopicMetadata
        (ref 'TopicErrorCode_1 topic)
        (kunstring (ref 'TopicName_1 topic))
        (not (zero? (ref 'IsInternal_1 topic)))
        (for/list ([part (in-list (ref 'PartitionMetadata_1 'PartitionMetadatas_1 topic))])
          (PartitionMetadata
           (ref 'PartitionErrorCode_1 part)
           (ref 'PartitionID_1 part)
           (not (zero? (ref 'Leader_1 part)))))))
     (ref 'ControllerID_1 res))))


;; heartbeat ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define tags/c hash?)

(define-response Heartbeat
  ([throttle-time-ms (or/c #f exact-integer?)]
   [tags tags/c]))

(define (enc-heartbeatv0 group-id generation-id member-id _group-instance-id _tags)
  (with-output-bytes
    (proto:un-HeartbeatRequestV0
     `((GroupID_1      . ,(kstring group-id))
       (GenerationID_1 . ,generation-id)
       (MemberID_1     . ,(kstring member-id))))))

(define (dec-heartbeatv1 res)
  (Heartbeat (ref 'ThrottleTimeMs_1 res) (hasheqv)))

(define-request Heartbeat
  (group-id
   generation-id
   member-id
   [group-instance-id #f]
   [tags (hasheqv)])
  #:code 12
  #:version 0 proto:HeartbeatResponseV0
  enc-heartbeatv0
  (lambda (_res)
    (Heartbeat #f (hasheqv)))

  #:version 1 proto:HeartbeatResponseV1
  enc-heartbeatv0
  dec-heartbeatv1

  #:version 2 proto:HeartbeatResponseV2
  enc-heartbeatv0
  dec-heartbeatv1

  #:version 3 proto:HeartbeatResponseV3
  (lambda (group-id generation-id member-id group-instance-id _tags)
    (with-output-bytes
      (proto:un-HeartbeatRequestV3
       `((GroupID_1         . ,(kstring group-id))
         (GenerationID_1    . ,generation-id)
         (MemberID_1        . ,(kstring member-id))
         (GroupInstanceID_1 . ,(kstring group-instance-id))))))
  dec-heartbeatv1)


;; delete topics ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-response DeletedTopic
  ([error-code error-code/c]
   [error-message (or/c #f string?)]
   [name string?]
   [uuid (or/c #f bytes?)]
   [tags tags/c]))

(define-response DeletedTopics
  ([throttle-time-ms (or/c #f exact-integer?)]
   [topics (listof DeletedTopic?)]
   [tags tags/c]))

(define-request DeleteTopics
  (topics
   [timeout-ms 30000])
  #:code 20
  #:version 0 proto:DeleteTopicsResponseV0
  (lambda (topics timeout-ms)
    (with-output-bytes
      (proto:un-DeleteTopicsRequestV0
       `((TopicNames_1 . ((ArrayLen_1  . ,(length topics))
                          (TopicName_1 . ,(map kstring topics))))
         (TimeoutMs_1 . ,timeout-ms)))))
  (lambda (res)
    (define topics
      (for/list ([topic (in-list (ref 'DeleteTopicsResponseDataV0_1 res))])
        (define err-code (ref 'ErrorCode_1 topic))
        (define name (kunstring (ref 'TopicName_1 topic)))
        (DeletedTopic err-code #f name #f (hasheqv))))
    (DeletedTopics #f topics (hasheqv))))
