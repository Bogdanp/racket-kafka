#lang racket/base

(require racket/contract
         "core.rkt")

(define-record Heartbeat
  ([throttle-time-ms (or/c #f exact-integer?)]
   [tags tags/c]))

(define (enc-heartbeatv0 group-id generation-id member-id _group-instance-id _tags)
  (with-output-bytes
    (proto:un-HeartbeatRequestV0
     `((GroupID_1      . ,(kstring group-id))
       (GenerationID_1 . ,generation-id)
       (MemberID_1     . ,(kstring member-id))))))

(define (dec-heartbeatv1 res)
  (Heartbeat (ref 'ThrottleTimeMs_1 res) (hasheqv)))

(define-request Heartbeat
  (group-id
   generation-id
   member-id
   [group-instance-id #f]
   [tags (hasheqv)])
  #:code 12
  #:version 0 proto:HeartbeatResponseV0
  enc-heartbeatv0
  (lambda (_res)
    (Heartbeat #f (hasheqv)))

  #:version 1 proto:HeartbeatResponseV1
  enc-heartbeatv0
  dec-heartbeatv1

  #:version 2 proto:HeartbeatResponseV2
  enc-heartbeatv0
  dec-heartbeatv1

  #:version 3 proto:HeartbeatResponseV3
  (lambda (group-id generation-id member-id group-instance-id _tags)
    (with-output-bytes
      (proto:un-HeartbeatRequestV3
       `((GroupID_1         . ,(kstring group-id))
         (GenerationID_1    . ,generation-id)
         (MemberID_1        . ,(kstring member-id))
         (GroupInstanceID_1 . ,(kstring group-instance-id))))))
  dec-heartbeatv1)
