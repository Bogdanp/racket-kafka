#lang racket/base

(require net/http-client
         racket/generic
         racket/match)

;; generics ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 proxy?
 proxy-connect)

(define-generics proxy
  {proxy-connect proxy target-host target-port ssl-ctx})


;; http proxy ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 make-http-proxy)

(define (make-http-proxy host port)
  (http-proxy host port))

(struct http-proxy (host port)
  #:methods gen:proxy
  [(define (proxy-connect p target-host target-port ssl-ctx)
     (match-define (http-proxy proxy-host proxy-port) p)
     (define-values (_ssl-ctx in out _abandon)
       (http-conn-CONNECT-tunnel
        #:ssl? ssl-ctx
        proxy-host proxy-port
        target-host target-port))
     (values in out))])
