#lang racket/base

(define-syntax-rule (reprovide mod ...)
  (begin
    (require mod ...)
    (provide (all-from-out mod ...))))

(reprovide
 "serde/metadata.rkt"
 "serde/heartbeat.rkt"
 "serde/create-topics.rkt"
 "serde/delete-topics.rkt"
 "serde/describe-groups.rkt"
 "serde/find-coordinator.rkt"
 "serde/group.rkt"
 "serde/list-groups.rkt"
 "serde/produce.rkt"
 "serde/sasl-authenticate.rkt"
 "serde/sasl-handshake.rkt")
