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

(define-syntax (define-request stx)
  (syntax-parse stx
    [(_ id:id (arg-id:id ...) (~seq #:version version-num:number parser:expr encoder:expr decoder:expr) ...)
     #:with make-evt-id (format-id #'id "make-~a-evt" #'id)
     #:with version-rng-id (format-id #'id "~a-supported-versions" #'id)
     #:with version-rng (let ([versions (map syntax->datum (syntax-e #'(version-num ...)))])
                          (with-syntax ([min-v (apply min versions)]
                                        [max-v (apply max versions)])
                            #'(version-range min-v max-v)))
     #'(begin
         (provide make-evt-id)
         (define version-rng-id version-rng)
         (define (make-evt-id conn arg-id ...)
           (case (find-best-version conn 'id version-rng-id)
             [(version-num)
              (handle-evt
               (make-request-evt
                conn
                #:key (request-key 'id)
                #:version version-num
                #:data (encoder arg-id ...)
                #:parser parser)
               decoder)] ...
             [else
              (error 'make-evt-id "no supported version")])))]))

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
