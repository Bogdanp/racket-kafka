#lang racket/base

(require (for-syntax racket/base)
         (prefix-in http: net/http-easy)
         racket/string
         "error.rkt")

(provide
 client?
 make-client)

(struct client (session uri auth-proc))

(define (make-client uri [auth-proc (λ (_url headers params)
                                      (values headers params))])
  (client
   (http:make-session)
   (string-trim uri "/")
   auth-proc))

(define ((make-requester method) c path
                                 #:headers [headers (hasheq)]
                                 #:params [params null]
                                 #:data [data #f])
  (define res
    (http:session-request
     #:method method
     #:headers (hash-set headers 'accept "application/vnd.schemaregistry.v1+json")
     #:params params
     #:data data
     #:auth (client-auth-proc c)
     (client-session c)
     (string-append (client-uri c) path)))
  (define status-code
    (http:response-status-code res))
  (cond
    [(>= status-code 500)
     (raise (server-error status-code (http:response-body res)))]
    [(>= status-code 400)
     (define data (http:response-json res))
     (define code (hash-ref data 'error_code status-code))
     (define message (hash-ref data 'message ""))
     (raise (client-error code message))]
    [else
     (http:response-json res)]))

(define-syntax-rule (define-requesters [id ...])
  (begin
    (provide id ...)
    (define id (make-requester 'id)) ...))

(define-requesters
  [delete get post put])
