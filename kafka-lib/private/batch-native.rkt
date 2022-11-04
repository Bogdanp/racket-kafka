#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         binfmt/runtime/unparser
         racket/port
         (prefix-in b: "batch.rkt"))

(provide
 (rename-out
  [parse-records Records]
  [unparse-records un-Records]))

(define (parse-records in)
  (res-bind
   (parse-i32be in)
   (lambda (len)
     (define batches-in
       (make-limited-input-port in len #f))
     (let loop ([batches null])
       (cond
         [(eof-object? (peek-byte batches-in))
          (ok (reverse batches))]
         [else
          (define batch
            (with-handlers ([exn:fail? (λ (_) #f)])
              (b:read-batch batches-in)))
          (if batch
              (loop (cons batch batches))
              (ok (reverse batches)))])))))

(define (unparse-records out v)
  (unparse-i32be out (bytes-length v))
  (begin0 (ok v)
    (write-bytes v out)))
