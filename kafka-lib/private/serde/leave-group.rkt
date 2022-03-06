#lang racket/base

(require racket/contract
         "core.rkt")

(define-record LeaveGroupResponse
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [error-code error-code/c]))

(define-request LeaveGroup
  (group-id member-id)
  #:code 13

  #:version 0
  #:response proto:LeaveGroupResponseV0
  (make-enc-leave-group proto:un-LeaveGroupRequestV0)
  dec-leave-group

  #:version 1
  #:response proto:LeaveGroupRequestV1
  (make-enc-leave-group proto:un-LeaveGroupRequestV1)
  dec-leave-group

  #:version 2
  #:response proto:LeaveGroupRequestV2
  (make-enc-leave-group proto:un-LeaveGroupRequestV2)
  dec-leave-group)

(define ((make-enc-leave-group unparse) group-id member-id)
  (with-output-bytes
    (unparse
     `((GroupID_1 . ,group-id)
       (MemberID_1 . ,member-id)))))

(define (dec-leave-group res)
  (make-LeaveGroupResponse
   #:throttle-time-ms (opt 'ThrottleTimeMs_1 res)
   #:error-code (res 'ErrorCode_1 res)))
