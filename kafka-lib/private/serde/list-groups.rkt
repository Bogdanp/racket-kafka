#lang racket/base

(require "core.rkt"
         "group.rkt")

(define-request ListGroups ()
  #:code 16
  #:version 0
  #:response proto:ListGroupsResponseV0
  (lambda () #"")
  (dec-list-groups-response-v012 'ListGroupsGroupV0_1)

  #:version 1
  #:response proto:ListGroupsResponseV1
  (lambda () #"")
  (dec-list-groups-response-v012 'ListGroupsGroupV1_1)

  #:version 2
  #:response proto:ListGroupsResponseV2
  (lambda () #"")
  (dec-list-groups-response-v012 'ListGroupsGroupV2_1)

  #:version 3
  #:flexible #t
  #:response proto:ListGroupsResponseV3
  (lambda ()
    (with-output-bytes
      (proto:un-ListGroupsRequestV3 (hash))))
  (lambda (res)
    (for/list ([g (in-list (ref 'ListGroupsGroupV3_1 res))])
      (make-Group
       #:id (ref 'CompactGroupID_1 g)
       #:protocol-type (ref 'CompactProtocolType_1 g)))))

(define ((dec-list-groups-response-v012 list-id) res)
  (for/list ([g (in-list (ref list-id res))])
    (make-Group
     #:id (ref 'GroupID_1 g)
     #:protocol-type (ref 'ProtocolType_1 g))))
