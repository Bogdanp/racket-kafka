#lang racket/base

(require racket/contract
         "core.rkt")

(define resource-type/c
  (or/c 'unknown 'any 'topic 'group 'broker 'cluster 'transaction-id 'delegation-token 'user))

(define-record DescribeResource
  ([type resource-type/c]
   [name string?]
   [(keys null) (listof string?)]))

(define-record ResourceConfig
  ([name string?]
   [value (or/c #f string?)]
   [read-only? boolean?]
   [default? boolean?]
   [sensitive? boolean?]))

(define-record DescribedResource
  ([error-code error-code/c]
   [error-message (or/c #f string?)]
   [type resource-type/c]
   [name string?]
   [configs (listof ResourceConfig?)]))

(define-record DescribedResources
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [resources (listof DescribedResource?)]))

(define-request DescribeConfigs
  (resources)
  #:code 32
  #:version 0
  #:response proto:DescribeConfigsResponseV0
  (lambda (resources)
    (with-output-bytes
      (proto:un-DescribeConfigsRequestV0
       `((ArrayLen_1 . ,(length resources))
         (DescribeConfigsResourceV0_1 . ,(for/list ([res (in-list resources)])
                                           (define len (length (DescribeResource-keys res)))
                                           `((ResourceType_1 . ,(->resource-type (DescribeResource-type res)))
                                             (ResourceName_1 . ,(DescribeResource-name res))
                                             (ArrayLen_1 . ,(if (zero? len) -1 len))
                                             (ConfigName_1 . ,(DescribeResource-keys res)))))))))
  (lambda (res)
    (make-DescribedResources
     #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
     #:resources (for/list ([r (in-list (ref 'DescribeConfigsResultV0_1 res))])
                   (make-DescribedResource
                    #:error-code (ref 'ErrorCode_1 r)
                    #:error-message (ref 'ErrorMessage_1 r)
                    #:type (resource-type-> (ref 'ResourceType_1 r))
                    #:name (ref 'ResourceName_1 r)
                    #:configs (for/list ([c (in-list (ref 'ResourceConfigV0_1 r))])
                                (make-ResourceConfig
                                 #:name (ref 'ConfigName_1 c)
                                 #:value (ref 'ConfigValue_1 c)
                                 #:read-only? (= (ref 'ReadOnly_1 c) 1)
                                 #:default? (= (ref 'IsDefault_1 c) 1)
                                 #:sensitive? (= (ref 'IsSensitive_1 c) 1))))))))

(define (->resource-type type)
  (case type
    [(unknown) 0]
    [(any) 1]
    [(topic) 2]
    [(group) 3]
    [(broker) 4]
    [(cluster) 4]
    [(transactional-id) 5]
    [(delegation-token) 6]
    [(user) 7]
    [else (raise-argument-error '->resource-type "resource-type/c" type)]))

(define (resource-type-> type)
  (case type
    [(0) 'unknown]
    [(1) 'any]
    [(2) 'topic]
    [(3) 'group]
    [(4) 'broker]
    [(4) 'cluster]
    [(5) 'transactional-id]
    [(6) 'delegation-token]
    [(7) 'user]
    [else (raise-argument-error 'resource-type-> "(integer-in 0 7)" type)]))
