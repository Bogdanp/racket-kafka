#lang racket/base

(require "help.rkt")

(provide
 (struct-out record)
 parse-record)

(struct record (offset key value))

(define (parse-record r [base-offset 0])
  (record
   (+ base-offset (ref 'OffsetDelta_1 r))
   (ref 'Key_1 r)
   (ref 'Value_1 r)))
