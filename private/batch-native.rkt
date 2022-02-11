#lang racket/base

(require binfmt/runtime/res
         binfmt/runtime/unparser
         (prefix-in proto: "batch.bnf"))

(provide
 (rename-out
  [parse-records Records]
  [unparse-records un-Records]))

(define (parse-records in)
  (define b
    (proto:RecordBatch in))
  (ok `((Batch_1 . ,b)
        (Records_1 . ,(for/list ([_ (in-range (cdr (assq 'RecordCount_1 b)))])
                        (proto:Record in))))))

(define (unparse-records out v)
  (unparse-i32be out (bytes-length v))
  (begin0 (ok v)
    (write-bytes v out)))
