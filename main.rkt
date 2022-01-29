#lang racket/base

(require racket/contract
         "private/connection.rkt"
         "private/help.rkt"
         (prefix-in proto: "private/protocol.bnf"))

(provide
 (contract-out
  [current-client-id (parameter/c string?)]
  [connection? (-> any/c boolean?)]
  [connect (-> string? (integer-in 0 65535) connection?)]
  [disconnect (-> connection? void)]))


;; topic-metadata ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (contract-out
  [struct metadata ([brokers (listof broker-metadata?)]
                    [controller-id exact-integer?]
                    [topics (listof topic-metadata?)])]
  [struct broker-metadata ([node-id exact-integer?]
                           [host string?]
                           [port (integer-in 0 65535)]
                           [rack string?])]
  [struct topic-metadata ([error-code error-code/c]
                          [topic string?]
                          [internal? boolean?]
                          [partitions (listof partition-metadata?)])]
  [struct partition-metadata ([error-code error-code/c]
                              [partition-id exact-integer?]
                              [leader? boolean?])]
  [get-topic-metadata (-> connection? metadata?)]))

(define error-code/c exact-integer?)

(struct metadata (brokers controller-id topics)
  #:transparent)

(struct broker-metadata (node-id host port rack)
  #:transparent)

(struct topic-metadata (error-code topic internal? partitions)
  #:transparent)

(struct partition-metadata (error-code partition-id leader?)
  #:transparent)

(define (get-topic-metadata conn . topics)
  (sync
   (handle-evt
    (make-request-evt conn 3 1
                      `((ArrayLen_1 . ,(length topics))
                        (TopicName . ,(map kstring topics))))
    (lambda (res)
      (metadata
       (for/list ([broker (in-list (ref 'Broker_1 'Brokers_1 res))])
         (broker-metadata
          (ref 'NodeID_1 broker)
          (kunstring (ref 'Host_1 broker))
          (ref 'Port_1 broker)
          (kunstring (ref 'Rack_1 broker))))
       (ref 'ControllerID_1 res)
       (for/list ([topic (in-list (ref 'TopicMetadata_1 'TopicMetadatas_1 res))])
         (topic-metadata
          (ref 'TopicErrorCode_1 topic)
          (kunstring (ref 'TopicName_1 topic))
          (= (ref 'IsInternal_1 topic) 1)
          null)))))))
