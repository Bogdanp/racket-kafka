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
  [parse-batch-bytes BatchBytes]
  [unparse-batch-bytes un-BatchBytes]
  [parse-compact-bytes CompactBytes]
  [unparse-compact-bytes un-CompactBytes]
  [parse-string String]
  [unparse-string un-String]
  [parse-batch-string BatchString]
  [unparse-batch-string un-BatchString]
  [parse-compact-string CompactString]
  [unparse-compact-string un-CompactString]
  [parse-compact-array-len CompactArrayLen]
  [unparse-compact-array-len un-CompactArrayLen]
  [parse-tags Tags]
  [unparse-tags un-Tags]))

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

(define (parse-batch-bytes in)
  (res-bind
   (parse-varint32 in)
   (lambda (len)
     (cond
       [(= len -1) (ok 'nil)]
       [else (expect 'batch-bytes len in)]))))

(define (unparse-batch-bytes out bs)
  (cond
    [(equal? bs 'nil)
     (unparse-i32be out -1)]
    [else
     (res-bind
      (unparse-varint32 out (bytes-length bs))
      (lambda (_)
        (begin0 (ok bs)
          (write-bytes bs out))))]))

(define (parse-compact-bytes in)
  (res-bind
   (parse-uvarint32 in)
   (lambda (len)
     (cond
       [(zero? len) (ok 'nil)]
       [else (expect 'compact-bytes (sub1 len) in)]))))

(define (unparse-compact-bytes out bs)
  (cond
    [(equal? bs 'nil) (unparse-uvarint32 out 0)]
    [else
     (res-bind
      (unparse-uvarint32 out (add1 (bytes-length bs)))
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

(define (parse-batch-string in)
  (res-bind
   (parse-varint32 in)
   (lambda (len)
     (res-bind
      (expect 'batch-string len in)
      (compose1 ok bytes->string/utf-8)))))

(define (unparse-batch-string out s)
  (define bs (string->bytes/utf-8 s))
  (res-bind
   (unparse-varint32 out (bytes-length bs))
   (lambda (_)
     (begin0 (ok s)
       (write-bytes bs out)))))

(define (parse-compact-string in)
  (res-bind
   (parse-uvarint32 in)
   (lambda (len)
     (cond
       [(zero? len) (ok 'nil)]
       [else
        (res-bind
         (expect 'compact-string (sub1 len) in)
         (compose1 ok bytes->string/utf-8))]))))

(define (unparse-compact-string out s)
  (cond
    [(equal? s 'nil) (unparse-uvarint32 out 0)]
    [else
     (define bs (string->bytes/utf-8 s))
     (res-bind
      (unparse-uvarint32 out (add1 (bytes-length bs)))
      (lambda (_)
        (begin0 (ok s)
          (write-bytes bs out))))]))

(define (parse-compact-array-len in)
  (res-bind
   (parse-uvarint32 in)
   (lambda (len)
     (cond
       [(zero? len) (ok 0)]
       [else (ok (sub1 len))]))))

(define (unparse-compact-array-len out len)
  (if (zero? len)
      (unparse-uvarint32 out 0)
      (unparse-uvarint32 out (add1 len))))

(define (parse-tags in)
  (res-bind
   (parse-uvarint32 in)
   (lambda (len)
     (let loop ([len len] [tags (hasheqv)])
       (cond
         [(zero? len)
          (ok tags)]
         [else
          (res-bind
           (parse-tag-pair in)
           (lambda (tag-pair)
             (loop (sub1 len)
                   (hash-set tags
                             (car tag-pair)
                             (cdr tag-pair)))))])))))

(define (parse-tag-pair in)
  (res-bind
   (parse-uvarint32 in)
   (lambda (key)
     (res-bind
      (parse-compact-bytes in)
      (lambda (value)
        (ok (cons key value)))))))

(define (unparse-tags out v)
  (res-bind
   (unparse-uvarint32 out (hash-count v))
   (lambda (_)
     (let loop ([tags (sort (hash->list v) #:key car <)])
       (cond
         [(null? tags) (ok v)]
         [else
          (res-bind
           (unparse-tag-pair out (car tags))
           (lambda (_)
             (loop (cdr tags))))])))))

(define (unparse-tag-pair out v)
  (res-bind
   (unparse-uvarint32 out (car v))
   (lambda (_)
     (unparse-compact-bytes out (cdr v)))))
