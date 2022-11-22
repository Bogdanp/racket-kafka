#lang racket/base

(require kafka
         openssl
         sasl/aws-msk-iam)

(define (make-auth-ctx host _port)
  (make-aws-msk-iam-ctx
   #:region "us-east-1"
   #:access-key-id "<ACCESS-KEY-ID>"
   #:secret-access-key "<SECRET-ACCESS-KEY>"
   #:server-name host))

(define c
  (make-client
   #:bootstrap-host "<HOST>"
   #:bootstrap-port 9198
   #:sasl-mechanism&ctx (list 'AWS_MSK_IAM make-auth-ctx)
   #:ssl-ctx (ssl-secure-client-context)))

(get-metadata c)
