#lang racket/base

(require racket/port)

(provide
 ref
 kstring
 kunbytes
 kunstring
 with-output-bytes)

(define ref
  (case-lambda
    [(id v)
     (define p (assq id v))
     (unless p
       (error 'ref "key not found: ~s" id))
     (cdr p)]
    [(id . args)
     (ref id (apply ref args))]))

(define (kstring s)
  (define bs (string->bytes/utf-8 s))
  `((StringLength_1 . ,(bytes-length bs))
    (StringData_1 . ,(bytes->list bs))))

(define (kunbytes d)
  (if (and (assq 'num_1 d)
           (assq 'num_2 d))
      #""
      (apply bytes (ref 'StringData_1 d))))

(define (kunstring d)
  (bytes->string/utf-8 (kunbytes d)))

(define-syntax-rule (with-output-bytes e0 e ...)
  (with-output-to-bytes
    (lambda ()
      e0 e ...)))
