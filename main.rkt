#lang racket/base

(require racket/contract
         "private/connection.rkt"
         "private/serde.rkt")

(provide
 (all-from-out "private/serde.rkt")
 (contract-out
  [current-client-id (parameter/c string?)]
  [connection? (-> any/c boolean?)]
  [connect (-> string? (integer-in 0 65535) connection?)]
  [disconnect (-> connection? void)]
  [get-metadata (-> connection? string? ... Metadata?)]
  [create-topics (-> connection? CreateTopic? ... CreatedTopics?)]
  [delete-topics (-> connection? string? ... DeletedTopics?)]))

(define (get-metadata conn . topics)
  (sync (make-Metadata-evt conn topics)))

(define (create-topics conn . topics)
  (sync (make-CreateTopics-evt conn topics)))

(define (delete-topics conn . topics)
  (sync (make-DeleteTopics-evt conn topics)))
