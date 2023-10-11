#lang racket/base

(require racket/contract
         "core.rkt"
         "resource.rkt")

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
