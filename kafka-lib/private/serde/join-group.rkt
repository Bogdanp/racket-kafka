#lang racket/base

(require racket/contract
         "core.rkt")

(define-record Protocol
  ([name string?]
   [metadata bytes?]))

(define-record Member
  ([id string?]
   [metadata bytes?]))

(define-record JoinGroupResponse
  ([(throttle-time-ms #f) (or/c #f exact-nonnegative-integer?)]
   [error-code error-code/c]
   [generation-id string?]
   [protocol-name string?]
   [leader string?]
   [member-id string?]
   [members (listof Member?)]))

(define-request JoinGroup
  (group-id
   [session-timeout-ms 30000]
   [member-id ""]
   [protocol-type "consumer"]
   [protocols null])
  #:code 11

  #:version 0
  #:response proto:JoinGroupResponseV0
  (lambda (group-id session-timeout-ms member-id protocol-type protocols)
    (with-output-bytes
      (proto:un-JoinGroupRequestV0
       `((GroupID_1 . ,group-id)
         (SessionTimeoutMs_1 . ,session-timeout-ms)
         (MemberID_1 . ,member-id)
         (ProtocolType_1 . ,protocol-type)
         (ArrayLen_1 . ,(length protocols))
         (Protocol_1 ,@(for/list ([p (in-list protocols)])
                         `((ProtocolName_1 . ,(Protocol-name p))
                           (ProtocolMetadata_1 . ,(Protocol-metadata p)))))))))
  (lambda (res)
    (make-JoinGroupResponse
     #:throttle-time-ms (opt 'ThrottleTimeMs_1 res)
     #:error-code (ref 'ErrorCode_1 res)
     #:generation-id (ref 'GenerationID_1 res)
     #:protocol-name (ref 'ProtocolName_1 res)
     #:leader (ref 'LeaderID_1 res)
     #:member-id (ref 'MemberID_1 res)
     #:members (for/list ([m (ref 'Member_1 res)])
                 (make-Member
                  #:id (ref 'MemberID_1 m)
                  #:metadata (ref 'MemberMetadata_1 m))))))
