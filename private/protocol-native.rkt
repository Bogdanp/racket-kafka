#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         binfmt/runtime/unparser)

(provide
 (rename-out
  [parse-uuid UUID]
  [unparse-uuid un-UUID]
  [parse-bytes Bytes]
  [unparse-bytes un-Bytes]
  [parse-string String]
  [unparse-string un-String]))

(define (expect what len in)
  (define bs (read-bytes len in))
  (cond
    [(eof-object? bs) (make-err in "unexpected EOF while reading ~a" what)]
    [(< (bytes-length bs) len) (make-err in "input ended before ~a" what)]
    [else (ok bs)]))

(define (parse-uuid in)
  (expect 'UUID 16 in))

(define (unparse-uuid out v)
  (begin0 (ok v)
    (write-bytes v out)))

(define (parse-bytes in)
  (res-bind
   (parse-i32be in)
   (lambda (len)
     (cond
       [(= len -1) (ok 'nil)]
       [else (expect 'bytes len in)]))))

(define (unparse-bytes out bs)
  (cond
    [(equal? bs 'nil)
     (unparse-i32be out -1)]
    [else
     (res-bind
      (unparse-i32be out (bytes-length bs))
      (lambda (_)
        (begin0 (ok bs)
          (write-bytes bs out))))]))

(define (parse-string in)
  (res-bind
   (parse-i16be in)
   (lambda (len)
     (cond
       [(= len -1) (ok 'nil)]
       [else (res-bind
              (expect 'string len in)
              (compose1 ok bytes->string/utf-8))]))))

(define (unparse-string out s)
  (cond
    [(equal? s 'nil)
     (unparse-i16be out -1)]
    [else
     (define bs (string->bytes/utf-8 s))
     (res-bind
      (unparse-i16be out (bytes-length bs))
      (lambda (_)
        (begin0 (ok s)
          (write-bytes bs out))))]))
