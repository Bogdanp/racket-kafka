#lang racket/base

(require racket/contract
         "authorized-operation.rkt"
         "core.rkt"
         "group.rkt")

(define-record DescribedGroups
  ([throttle-time-ms (or/c #f exact-nonnegative-integer?)]
   [groups (listof Group?)]))

(define-request DescribeGroups
  ([groups null]
   [include-authorized-operations? #t])
  #:code 15

  #:version 0
  #:response proto:DescribeGroupsResponseV0
  enc-describe-groups-v0
  dec-describe-groups-v0

  #:version 1
  #:response proto:DescribeGroupsResponseV1
  enc-describe-groups-v0
  dec-describe-groups-v1

  #:version 2
  #:response proto:DescribeGroupsResponseV2
  enc-describe-groups-v0
  dec-describe-groups-v2

  #:version 3
  #:response proto:DescribeGroupsResponseV3
  enc-describe-groups-v3
  dec-describe-groups-v3)

(define (enc-describe-groups-v0 groups _include-authorized-operations?)
  (with-output-bytes
    (proto:un-DescribeGroupsRequestV0
     `((ArrayLen_1 . ,(length groups))
       (GroupID_1 . ,groups)))))

(define (enc-describe-groups-v3 groups include-authorized-operations?)
  (with-output-bytes
    (proto:un-DescribeGroupsRequestV3
     `((ArrayLen_1 . ,(length groups))
       (GroupID_1 . ,groups)
       (IncludeAuthorizedOperations_1 . ,include-authorized-operations?)))))

(define ((make-dec-describe-groups [groups-key 'DescribeGroupsGroupV0_1]
                                   [members-key 'DescribeGroupsMemberV0_1]) res)
  (make-DescribedGroups
   #:throttle-time-ms (opt 'ThrottleTimeMs_1 res)
   #:groups (for/list ([g (in-list (ref groups-key res))])
              (make-Group
               #:error-code (ref 'ErrorCode_1 g)
               #:id (ref 'GroupID_1 g)
               #:state (ref 'GroupState_1 g)
               #:protocol-type (ref 'ProtocolType_1 g)
               #:protocol-data (ref 'ProtocolData_1 g)
               #:authorized-operations (integer->authorized-operations
                                        (or (opt 'AuthorizedOperations_1 g) -1))
               #:members (for/list ([m (in-list (ref members-key g))])
                           (make-GroupMember
                            #:id (ref 'MemberID_1 m)
                            #:client-id (ref 'ClientID_1 m)
                            #:client-host (ref 'ClientHost_1 m)
                            #:metadata (ref 'MemberMetadata_1 m)
                            #:assignment (ref 'MemberAssignmentData_1 m)))))))

(define dec-describe-groups-v0 (make-dec-describe-groups))
(define dec-describe-groups-v1 (make-dec-describe-groups))
(define dec-describe-groups-v2 (make-dec-describe-groups))
(define dec-describe-groups-v3 (make-dec-describe-groups 'DescribeGroupsGroupV3_1))
