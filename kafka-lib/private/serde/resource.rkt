#lang racket/base

(require racket/contract/base)

(provide
 resource-type/c
 resource-type->
 ->resource-type)

(define resource-type/c
  (or/c 'unknown 'any 'topic 'group 'broker 'cluster 'transaction-id 'delegation-token 'user))

(define (->resource-type type)
  (case type
    [(unknown) 0]
    [(any) 1]
    [(topic) 2]
    [(group) 3]
    [(broker) 4]
    [(cluster) 4]
    [(transactional-id) 5]
    [(delegation-token) 6]
    [(user) 7]
    [else (raise-argument-error '->resource-type "resource-type/c" type)]))

(define (resource-type-> type)
  (case type
    [(0) 'unknown]
    [(1) 'any]
    [(2) 'topic]
    [(3) 'group]
    [(4) 'broker]
    [(4) 'cluster]
    [(5) 'transactional-id]
    [(6) 'delegation-token]
    [(7) 'user]
    [else (raise-argument-error 'resource-type-> "(integer-in 0 7)" type)]))
