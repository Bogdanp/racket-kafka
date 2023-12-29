#lang racket/base

(require racket/contract/base
         "authorized-operation.rkt"
         "core.rkt"
         "metadata.rkt")

(define-record Cluster
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [error-code error-code/c]
   [error-message (or/c #f string?)]
   [cluster-id string?]
   [controller-id exact-nonnegative-integer?]
   [brokers (listof BrokerMetadata?)]
   [authorized-operations (listof authorized-operation/c)]))

(define-request DescribeCluster
  ([include-authorized-operations? #t])
  #:code 60
  #:version 0
  #:flexible #t
  #:response proto:DescribeClusterResponseV0
  (lambda (include-authorized-operations?)
    (with-output-bytes
      (proto:un-DescribeClusterRequestV0
       `((IncludeClusterAuthorizedOperations_1 . ,include-authorized-operations?)
         (Tags_1 . ,(hash))))))
  (lambda (res)
    (make-Cluster
     #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
     #:error-code (ref 'ErrorCode_1 res)
     #:error-message (ref 'CompactErrorMessage_1 res)
     #:cluster-id (ref 'CompactClusterID_1 res)
     #:controller-id (ref 'ControllerID_1 res)
     #:authorized-operations (integer->authorized-operations
                              (ref 'ClusterAuthorizedOperations_1 res))
     #:brokers (for/list ([b (in-list (ref 'CompactBroker_1 res))])
                 (make-BrokerMetadata
                  #:node-id (ref 'BrokerID_1 b)
                  #:host (ref 'CompactHost_1 b)
                  #:port (ref 'Port_1 b)
                  #:rack (ref 'CompactRack_1 b))))))
