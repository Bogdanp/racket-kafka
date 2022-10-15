#lang racket/base

(require racket/contract
         racket/list)

(provide
 authorized-operation/c
 integer->authorized-operations)

(define operations
  #(unknown any all read write create delete alter describe cluster-action describe-configs alter-configs idempotent-write create-tokens describe-tokens))

(define authorized-operation/c
  (apply or/c (vector->list operations)))

(define (integer->authorized-operations n)
  (let loop ([i 0] [n n] [ops null])
    (if (<= n 0)
        (remove-duplicates (reverse ops))
        (loop (add1 i)
              (arithmetic-shift n -1)
              (if (= (bitwise-and n 1) 1)
                  (cons (get-operation i) ops)
                  ops)))))

(define (get-operation i)
  (if (< i (vector-length operations))
      (vector-ref operations i)
      'unknown))
