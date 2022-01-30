#lang racket/base

(require racket/contract)

(provide
 (all-defined-out))

(define error-code/c exact-integer?)
(define port/c (integer-in 0 65535))
(define tags/c hash?)
