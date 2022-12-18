#lang racket/base

(require "help.rkt")

(provide
 (struct-out record)
 parse-record)

(struct record
  ([partition-id #:mutable]
   offset
   timestamp
   key
   value
   headers))

(define (parse-record r
                      [base-offset 0]
                      [base-timestamp 0])
  (record
   #f
   (+ base-offset (ref 'OffsetDelta_1 r))
   (+ base-timestamp (ref 'TimestampDelta_1 r))
   (ref 'Key_1 r)
   (ref 'Value_1 r)
   (for*/hash ([e (in-list (or (opt 'Header_1 'Headers_1 r) null))]
               [k (in-value (ref 'HeaderKey_1 e))]
               #:when k
               [v (in-value (ref 'HeaderValue_1 e))])
     (values k v))))
