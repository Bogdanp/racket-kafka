#lang racket/base

(require racket/contract/base
         "core.rkt")

(define-record Heartbeat
  ([throttle-time-ms (or/c #f exact-integer?)]
   [tags tags/c]))

(define-request Heartbeat
  (group-id
   generation-id
   member-id
   [group-instance-id #f]
   [tags (hasheqv)])
  #:code 12
  #:version 0
  #:response proto:HeartbeatResponseV0
  enc-heartbeatv0
  (lambda (_res)
    (Heartbeat #f (hasheqv)))

  #:version 1
  #:response proto:HeartbeatResponseV1
  enc-heartbeatv0
  dec-heartbeatv1

  #:version 2
  #:response proto:HeartbeatResponseV2
  enc-heartbeatv0
  dec-heartbeatv1

  ;; #:version 3
  ;; #:response proto:HeartbeatResponseV3
  ;; (lambda (group-id generation-id member-id group-instance-id _tags)
  ;;   (with-output-bytes
  ;;     (proto:un-HeartbeatRequestV3
  ;;      `((GroupID_1         . ,group-id)
  ;;        (GenerationID_1    . ,generation-id)
  ;;        (MemberID_1        . ,member-id)
  ;;        (GroupInstanceID_1 . ,group-instance-id)))))
  ;; dec-heartbeatv1
  )

(define (enc-heartbeatv0 group-id generation-id member-id _group-instance-id _tags)
  (with-output-bytes
    (proto:un-HeartbeatRequestV0
     `((GroupID_1      . ,group-id)
       (GenerationID_1 . ,generation-id)
       (MemberID_1     . ,member-id)))))

(define (dec-heartbeatv1 res)
  (Heartbeat (ref 'ThrottleTimeMs_1 res) (hasheqv)))
