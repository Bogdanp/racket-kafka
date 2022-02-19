#lang racket/base

(require racket/contract
         "core.rkt")

(define-record Coordinator
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [error-code error-code/c]
   [(error-message #f) (or/c #f string?)]
   [node-id string?]
   [host string?]
   [port (integer-in 0 65535)]))

(define-request FindCoordinator
  (key [key-type 0])
  #:code 10

  #:version 0
  #:response proto:FindCoordinatorResponseV0
  (lambda (key _key-type)
    (with-output-bytes
      (proto:un-FindCoordinatorRequestV0 key)))
  dec-find-coordinator-res

  #:version 1
  #:response proto:FindCoordinatorResponseV1
  enc-find-coordinator-v1
  dec-find-coordinator-res

  #:version 2
  #:response proto:FindCoordinatorResponseV2
  enc-find-coordinator-v1
  dec-find-coordinator-res)

(define (enc-find-coordinator-v1 key type)
  (with-output-bytes
    (proto:un-FindCoordinatorRequestV1
     `((CoordinatorKey_1 . ,key)
       (CoordinatorKeyType_1 . ,type)))))

(define (dec-find-coordinator-res res)
  (make-Coordinator
   #:throttle-time-ms (opt 'ThrottleTimeMs_1 res)
   #:error-code (ref 'ErrorCode_1 res)
   #:error-message (or (opt 'ErrorMessage_1 res)
                       (opt 'CompactErrorMessage_1 res))
   #:node-id (ref 'NodeID_1 res)
   #:host (or (opt 'Host_1 res)
              (ref 'CompactHost_1 res))
   #:port (ref 'Port_1 res)))
