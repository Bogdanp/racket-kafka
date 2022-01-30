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
  [get-metadata (-> connection? string? ... Metadata?)]))

(define (get-metadata conn . topics)
  (sync (make-Metadata-evt conn topics)))
