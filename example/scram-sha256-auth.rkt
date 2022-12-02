#lang racket/base

(require kafka
         sasl/scram)

(define (make-auth-ctx _host _port)
  (make-scram-client-ctx
   'sha256
   "client"
   "client-secret"))

(define c
  (make-client
   #:bootstrap-host "127.0.0.1"
   #:bootstrap-port 9092
   #:sasl-mechanism&ctx (list 'SCRAM-SHA-256 make-auth-ctx)))

(get-metadata c)
