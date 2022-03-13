#lang racket/base

(require openssl
         racket/contract
         racket/string
         sasl
         "private/client.rkt"
         "private/connection.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (all-from-out "private/serde.rkt")
 (contract-out
  [exn:fail:kafka? (-> any/c boolean?)]
  [exn:fail:kafka:client? (-> any/c boolean?)]
  [exn:fail:kafka:server? (-> any/c boolean?)]
  [exn:fail:kafka:server-code (-> exn:fail:kafka:server? exact-integer?)]
  [error-code-symbol (-> exact-integer? symbol?)]

  [client? (-> any/c boolean?)]
  [make-client (->* ()
                    (#:id non-empty-string?
                     #:bootstrap-host string?
                     #:bootstrap-port (integer-in 0 65535)
                     #:sasl-mechanism&ctx (or/c
                                           #f
                                           (list/c 'plain string?)
                                           (list/c symbol? sasl-ctx?))
                     #:ssl-ctx (or/c #f ssl-client-context?))
                    client?)]
  [disconnect-all (-> client? void?)]
  [get-metadata (-> client? string? ... Metadata?)]
  [create-topics (-> client? CreateTopic? CreateTopic? ... CreatedTopics?)]
  [delete-topics (-> client? string? string? ... DeletedTopics?)]
  [find-group-coordinator (-> client? string? Coordinator?)]
  [describe-groups (-> client? string? ... (listof Group?))]
  [list-groups (-> client? (listof Group?))]
  [list-offsets (-> client?
                    (hash/c string? (hash/c exact-nonnegative-integer? (or/c 'earliest 'latest exact-nonnegative-integer?)))
                    (hash/c string? PartitionOffset?))]))


;; admin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (get-metadata c . topics)
  (sync (make-Metadata-evt (get-connection c) topics)))

(define (create-topics c topic0 . topics)
  (sync (make-CreateTopics-evt (get-connection c) (cons topic0 topics))))

(define (delete-topics c topic0 . topics)
  (sync (make-DeleteTopics-evt (get-connection c) (cons topic0 topics))))

(define (find-group-coordinator c group-id)
  (sync (make-FindCoordinator-evt (get-connection c) group-id)))

(define (describe-groups c . groups)
  (sync (make-DescribeGroups-evt (get-connection c) groups)))

(define (list-groups c)
  (sync (make-ListGroups-evt (get-connection c))))

(define (list-offsets c topics)
  (sync (make-ListOffsets-evt (get-connection c) topics)))
