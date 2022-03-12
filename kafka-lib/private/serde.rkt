#lang racket/base

(define-syntax-rule (reprovide mod ...)
  (begin
    (require mod ...)
    (provide (all-from-out mod ...))))

(reprovide
 "serde/commit.rkt"
 "serde/contract.rkt"
 "serde/create-topics.rkt"
 "serde/delete-topics.rkt"
 "serde/describe-groups.rkt"
 "serde/fetch-offsets.rkt"
 "serde/fetch.rkt"
 "serde/find-coordinator.rkt"
 "serde/group.rkt"
 "serde/heartbeat.rkt"
 "serde/join-group.rkt"
 "serde/leave-group.rkt"
 "serde/list-groups.rkt"
 "serde/list-offsets.rkt"
 "serde/metadata.rkt"
 "serde/produce.rkt"
 "serde/sasl-authenticate.rkt"
 "serde/sasl-handshake.rkt"
 "serde/sync-group.rkt")
