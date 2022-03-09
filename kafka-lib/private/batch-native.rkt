#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         binfmt/runtime/unparser
         racket/port
         (prefix-in proto: "batch.bnf")
         (prefix-in b: "batch.rkt"))

(provide
 (rename-out
  [parse-records Records]
  [unparse-records un-Records]))

(define (parse-records in)
  (res-bind
   (parse-i32be in)
   (lambda (len)
     (with-handlers ([exn:fail? (λ (e) (err (exn-message e)))])
       (define batches-in (make-limited-input-port in len #f))
       (let loop ([batches null])
         (if (eof-object? (peek-byte batches-in))
             (ok (reverse batches))
             (loop (cons (b:read-batch batches-in) batches))))))))

(define (unparse-records out v)
  (unparse-i32be out (bytes-length v))
  (begin0 (ok v)
    (write-bytes v out)))
