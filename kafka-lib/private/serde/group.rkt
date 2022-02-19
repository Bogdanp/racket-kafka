#lang racket/base

(require racket/contract
         "core.rkt")

(define-record GroupMember
  ([id string?]
   [client-id string?]
   [client-host string?]
   [metadata bytes?]
   [assignment bytes?]))

(define-record Group
  ([(error-code 0) error-code/c]
   [id string?]
   [(state #f) (or/c #f string?)]
   [protocol-type string?]
   [(protocol-data #f) (or/c #f string?)]
   [(members null) (listof GroupMember?)]
   [(authorized-operations #f) (or/c #f exact-integer?)]))
