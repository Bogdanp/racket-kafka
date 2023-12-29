#lang racket/base

(require racket/contract/base
         "core.rkt")

(define-record DeletedGroup
  ([id string?]
   [(error-code 0) error-code/c]))

(define-record DeletedGroups
  ([(throttle-time-ms #f) (or/c #f exact-integer?)]
   [groups (listof DeletedGroup?)]))

(define-request DeleteGroups
  (group-ids)
  #:code 42
  #:version 0
  #:response proto:DeleteGroupsResponseV0
  (make-enc-delete-groupsv0 proto:un-DeleteGroupsRequestV0)
  dec-delete-groupsv0

  #:version 1
  #:response proto:DeleteGroupsResponseV1
  (make-enc-delete-groupsv0 proto:un-DeleteGroupsRequestV1)
  dec-delete-groupsv0)

(define ((make-enc-delete-groupsv0 unparser) group-ids)
  (with-output-bytes
    (unparser
     `((ArrayLen_1 . ,(length group-ids))
       (GroupID_1 . ,group-ids)))))


(define (dec-delete-groupsv0 res)
  (define groups
    (for/list ([group (in-list (ref 'DeleteGroupsResponseDataV0_1 res))])
      (make-DeletedGroup
       #:id (ref 'GroupID_1 group)
       #:error-code (ref 'ErrorCode_1 group))))
  (make-DeletedGroups
   #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
   #:groups groups))
