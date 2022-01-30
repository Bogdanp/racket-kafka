#lang racket/base

(require binfmt/runtime/parser
         binfmt/runtime/res
         binfmt/runtime/unparser)

(provide
 (rename-out
  [parse-bytes Bytes]
  [unparse-bytes un-Bytes]
  [parse-string String]
  [unparse-string un-String]))

(define (parse-bytes in)
  (res-bind
   (parse-i32be in)
   (lambda (len)
     (cond
       [(= len -1) (ok 'nil)]
       [else
        (define bs
          (read-bytes len in))
        (cond
          [(eof-object? bs) (make-err "unexpected EOF while reading bytes")]
          [(< (bytes-length bs) len) (make-err in "input ended before bytes")]
          [else (ok bs)])]))))

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
       [else
        (define str
          (read-string len in))
        (cond
          [(eof-object? str) (make-err in "unexpected EOF while reading string")]
          [(< (string-length str) len) (make-err in "input ended before string")]
          [else (ok str)])]))))

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
