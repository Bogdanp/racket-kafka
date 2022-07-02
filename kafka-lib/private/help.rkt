#lang racket/base

(require racket/port)

(provide
 null8
 null16
 null32
 ref
 ref-string
 opt
 with-output-bytes)

(define null8  #"\xFF")
(define null16 #"\xFF\xFF")
(define null32 #"\xFF\xFF\xFF\xFF")

(define ref
  (case-lambda
    [(id v)
     (define p (assq id v))
     (unless p
       (error 'ref "key not found: ~s~n  have: ~e" id (map car v)))
     (cdr p)]
    [(id . args)
     (ref id (apply ref args))]))

(define (ref-string . args)
  (define res (apply ref args))
  (cond
    [(eq? res 'nil) #f]
    [else res]))

(define opt
  (case-lambda
    [(id v)
     (define p (assq id v))
     (and p (cdr p))]
    [(id . args)
     (opt id (apply ref args))]))

(define-syntax-rule (with-output-bytes e0 e ...)
  (with-output-to-bytes
    (lambda ()
      e0 e ...)))
