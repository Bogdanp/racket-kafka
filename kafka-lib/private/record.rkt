#lang racket/base

(require "help.rkt")

(provide
 (struct-out record)
 parse-record)

(struct record (offset key value))

(define (parse-record r)
  (record
   (ref 'OffsetDelta_1 r)
   (ref 'Key_1 r)
   (ref 'Value_1 r)))
