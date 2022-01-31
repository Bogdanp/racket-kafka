#lang racket/base

(require racket/contract
         "private/connection.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (all-from-out "private/serde.rkt")
 (contract-out
  [exn:fail:kafka? (-> any/c boolean?)]
  [exn:fail:kafka-code (-> exn:fail:kafka? exact-integer?)]
  [current-client-id (parameter/c string?)]
  [connection? (-> any/c boolean?)]
  [connect (-> string? (integer-in 0 65535) connection?)]
  [disconnect (-> connection? void)]
  [get-metadata (-> connection? string? ... Metadata?)]
  [create-topics (-> connection? CreateTopic? CreateTopic? ... CreatedTopics?)]
  [delete-topics (-> connection? string? string? ... DeletedTopics?)]))

(define (get-metadata conn . topics)
  (sync (make-Metadata-evt conn topics)))

(define (create-topics conn topic0 . topics)
  (sync (make-CreateTopics-evt conn (cons topic0 topics))))

(define (delete-topics conn topic0 . topics)
  (sync (make-DeleteTopics-evt conn (cons topic0 topics))))
