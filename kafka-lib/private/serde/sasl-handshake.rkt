#lang racket/base

(require racket/contract/base
         "core.rkt")

(define-record SaslHandshakeResponse
  ([(mechanisms null) (listof symbol?)]))

(define-request SaslHandshake
  ([mechanism 'plain])
  #:code 17
  #:version 1
  #:response proto:SaslHandshakeResponseV1
  (lambda (mechanism)
    (with-output-bytes
      (proto:un-SaslHandshakeRequestV1
       (string-upcase (symbol->string mechanism)))))
  (lambda (res)
    (make-SaslHandshakeResponse
     #:mechanisms (map (compose1 string->symbol string-downcase)
                       (ref 'SaslHandshakeMechanism_1 res)))))
