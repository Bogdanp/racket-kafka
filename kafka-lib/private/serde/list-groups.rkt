#lang racket/base

(require "core.rkt")

(define-record Group
  ([name string?]
   [protocol-type string?]))

(define-request ListGroups ()
  #:code 16
  #:version 0
  #:response proto:ListGroupsResponseV0
  (lambda () #"")
  (lambda (res)
    (for/list ([g (in-list (ref 'Group_1 'Groups_1 res))])
      (Group
       (ref 'GroupID_1 g)
       (ref 'ProtocolType_1 g)))))
