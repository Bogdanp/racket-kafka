#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         binfmt/runtime/unparser
         racket/port
         (prefix-in proto: "batch.bnf"))

(provide
 (rename-out
  [parse-records Records]
  [unparse-records un-Records]))

(define (parse-records in)
  (res-bind
   (parse-i32be in)
   (lambda (len)
     (define batch-in (make-limited-input-port in len #f))
     (define b (proto:RecordBatch batch-in))
     (ok `((Batch_1 . ,b)
           (Records_1 . ,(for/list ([_ (in-range (cdr (assq 'RecordCount_1 b)))])
                           (proto:Record batch-in))))))))

(define (unparse-records out v)
  (unparse-i32be out (bytes-length v))
  (begin0 (ok v)
    (write-bytes v out)))
