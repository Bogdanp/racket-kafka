#lang racket/base

(require "core.rkt"
         "group.rkt")

(define-request ListGroups ()
  #:code 16
  #:version 0
  #:response proto:ListGroupsResponseV0
  (lambda () #"")
  (lambda (res)
    (for/list ([g (in-list (ref 'ListGroupsGroupV0_1 res))])
      (make-Group
       #:id (ref 'GroupID_1 g)
       #:protocol-type (ref 'ProtocolType_1 g)))))
