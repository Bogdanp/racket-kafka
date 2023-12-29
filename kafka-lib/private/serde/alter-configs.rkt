#lang racket/base

(require racket/contract/base
         "core.rkt"
         "resource.rkt")

(define-record AlterResource
  ([type resource-type/c]
   [name string?]
   [configs (hash/c string? (or/c #f string?))]))

(define-record AlteredResource
  ([error-code error-code/c]
   [error-message (or/c #f string?)]
   [type resource-type/c]
   [name string?]))

(define-record AlteredResources
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [resources (listof AlteredResource?)]))

(define-request AlterConfigs
  (resources validate-only?)
  #:code 33
  #:version 0
  #:response proto:AlterConfigsResponseV0
  (lambda (resources validate-only?)
    (with-output-bytes
      (define data
        (for/list ([res (in-list resources)])
          (define configs (AlterResource-configs res))
          (define len (hash-count configs))
          (define config-data
            (for/list ([(k v) (in-hash configs)])
              `((ConfigName_1 . ,k)
                (ConfigValue_1 . ,v))))
          `((ResourceType_1 . ,(->resource-type (AlterResource-type res)))
            (ResourceName_1 . ,(AlterResource-name res))
            (Configs_1 . ((ArrayLen_1 . ,len)
                          (Config_1 . ,config-data))))))
      (proto:un-AlterConfigsRequestV0
       `((ArrayLen_1 . ,(length resources))
         (AlterConfigsResourceV0_1 . ,data)
         (ValidateOnly_1 . ,validate-only?)))))
  (lambda (res)
    (make-AlteredResources
     #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
     #:resources (for/list ([r (in-list (ref 'AlterConfigsResponseDataV0_1 res))])
                   (make-AlteredResource
                    #:error-code (ref 'ErrorCode_1 r)
                    #:error-message (ref 'ErrorMessage_1 r)
                    #:type (resource-type-> (ref 'ResourceType_1 r))
                    #:name (ref 'ResourceName_1 r))))))
