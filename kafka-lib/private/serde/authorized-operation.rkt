#lang racket/base

(require racket/contract)

(provide
 authorized-operation/c
 integer->authorized-operations)

(define operations
  #(unknown any all read write create delete alter describe cluster-action describe-configs alter-configs idempotent-write create-tokens describe-tokens))

(define authorized-operation/c
  (apply or/c (vector->list operations)))

(define (integer->authorized-operations n)
  (cond
    [(negative? n) null]
    [else
     (let loop ([i 0] [n n] [ops null])
       (if (zero? n)
           (reverse ops)
           (loop (add1 i)
                 (arithmetic-shift n -1)
                 (if (= (bitwise-and n 1) 1)
                     (cons (get-operation i) ops)
                     ops))))]))

(define (get-operation i)
  (if (> i (vector-length operations))
      'unknown
      (vector-ref operations i)))
