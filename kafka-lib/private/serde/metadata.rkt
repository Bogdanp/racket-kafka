#lang racket/base

(require racket/contract/base
         "core.rkt")

(define-record BrokerMetadata
  ([node-id exact-nonnegative-integer?]
   [host string?]
   [port port/c]
   [rack (or/c #f string?)]))

(define-record PartitionMetadata
  ([error-code error-code/c]
   [id exact-nonnegative-integer?]
   [leader-id exact-integer?]
   [replica-node-ids (listof exact-nonnegative-integer?)]
   [in-sync-replica-node-ids (listof exact-nonnegative-integer?)]))

(define-record TopicMetadata
  ([error-code error-code/c]
   [name string?]
   [internal? boolean?]
   [partitions (listof PartitionMetadata?)]))

(define-record Metadata
  ([brokers (listof BrokerMetadata?)]
   [topics (listof TopicMetadata?)]
   [controller-id exact-integer?]
   [(cluster-id #f) (or/c #f string?)]))

(define-request Metadata (topics)
  #:code 3
  #:version 1
  #:response proto:MetadataResponseV1
  enc-metadata-request
  dec-metadata-response

  #:version 2
  #:response proto:MetadataResponseV2
  enc-metadata-request
  dec-metadata-response)

(define (enc-metadata-request topics)
  (if (null? topics)
      null32
      (with-output-bytes
        (proto:un-MetadataRequestV1
         `((ArrayLen_1   . ,(length topics))
           (TopicName_1  . ,topics))))))

(define (dec-metadata-response res)
  (Metadata
   (for/list ([broker (in-list (ref 'Broker_1 'Brokers_1 res))])
     (BrokerMetadata
      (ref 'NodeID_1 broker)
      (ref 'Host_1 broker)
      (ref 'Port_1 broker)
      (ref 'Rack_1 broker)))
   (for/list ([topic (in-list (ref 'TopicMetadata_1 'TopicMetadatas_1 res))])
     (TopicMetadata
      (ref 'TopicErrorCode_1 topic)
      (ref 'TopicName_1 topic)
      (not (zero? (ref 'IsInternal_1 topic)))
      (for/list ([part (in-list (ref 'PartitionMetadata_1 'PartitionMetadatas_1 topic))])
        (PartitionMetadata
         (ref 'PartitionErrorCode_1 part)
         (ref 'PartitionID_1 part)
         (ref 'Leader_1 part)
         (ref 'Replica_1 part)
         (ref 'ISR_1 part)))))
   (ref 'ControllerID_1 res)
   (opt 'ClusterID_1 res)))
