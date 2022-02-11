#lang racket/base

(require racket/contract
         "core.rkt")

(define-record SaslAuthenticateResponse
  ([data bytes?]
   [session-lifetime-ms exact-integer?]))

(define-request SaslAuthenticate
  ([data bytes?])
  #:code 36
  #:version 1
  #:response proto:SaslAuthenticateResponseV1
  (lambda (data)
    (with-output-bytes
      (proto:un-SaslAuthenticateRequestV1 data)))
  (lambda (res)
    (make-SaslAuthenticateResponse
     #:data (ref 'Bytes_1 res)
     #:session-lifetime-ms (ref 'SessionLifetimeMs_1 res))))
