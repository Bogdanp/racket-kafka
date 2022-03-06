#lang racket/base

(require "core.rkt")

(define-record Assignment
  ([member-id string?]
   [data bytes?]))

(define-request SyncGroup
  (group-id
   generation-id
   member-id
   member-assignments)
  #:code 14

  #:version 0
  #:response proto:SyncGroupResponseV0
  (lambda (group-id generation-id member-id member-assignments)
    (with-output-bytes
      (proto:un-SyncGroupRequestV0
       `((GroupID_1 . ,group-id)
         (GenerationID_1 . ,generation-id)
         (MemberID_1 . ,member-id)
         (MemberAssignment_1 ,@(for/list ([a (in-list member-assignments)])
                                 `((MemberID_1 . ,(Assignment-member-id a))
                                   (MemberAssignmentData_1 . ,(Assignment-data a)))))))))
  (lambda (res)
    (ref 'MemberAssignmentData_1 res)))
