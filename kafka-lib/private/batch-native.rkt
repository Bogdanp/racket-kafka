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

(define-logger kafka-batch)

;; Clients may set a limit on the size of the data returned by a
;; Fetch.  Kafka doesn't preprocess batches and simply lifts them off
;; disk, so it's likely that a set of records will contain truncated
;; batches.  So, this procedure has to treat parse failures as signals
;; that all the batches that could have been read, have been.
;;
;; xref: https://kafka.apache.org/documentation/#impl_reads
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
            (with-handlers ([exn:fail?
                             (lambda (e)
                               (begin0 #f
                                 (log-kafka-batch-warning "truncated batch: ~a" (exn-message e))))])
              (b:read-batch batches-in)))
          (if batch
              (loop (cons batch batches))
              (ok (reverse batches)))])))))

(define (unparse-records out v)
  (unparse-i32be out (bytes-length v))
  (begin0 (ok v)
    (write-bytes v out)))
