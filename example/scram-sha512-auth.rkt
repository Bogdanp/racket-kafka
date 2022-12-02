#lang racket/base

(require kafka
         sasl/scram)

(define (make-auth-ctx _host _port)
  (make-scram-client-ctx
   'sha512
   "client"
   "client-secret"))

(define c
  (make-client
   #:bootstrap-host "127.0.0.1"
   #:bootstrap-port 9092
   #:sasl-mechanism&ctx (list 'SCRAM-SHA-512 make-auth-ctx)))

(get-metadata c)
