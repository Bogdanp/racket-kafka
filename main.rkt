#lang racket/base

(require openssl
         racket/contract
         sasl
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
  [connect (->* (string? (integer-in 0 65535))
                (#:ssl ssl-client-context?)
                connection?)]
  [disconnect (-> connection? void)]
  [get-metadata (-> connection? string? ... Metadata?)]
  [create-topics (-> connection? CreateTopic? CreateTopic? ... CreatedTopics?)]
  [delete-topics (-> connection? string? string? ... DeletedTopics?)]
  [authenticate (-> connection? symbol? (or/c sasl-ctx? bytes? string?) void?)]))


;; common ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (authenticate conn mechanism ctx)
  (sync (make-SaslHandshake-evt conn mechanism))
  (case mechanism
    [(plain)
     (define req
       (if (string? ctx)
           (string->bytes/utf-8 ctx)
           ctx))
     (sync (make-SaslAuthenticate-evt conn req))]
    [else
     (let loop ()
       (case (sasl-state ctx)
         [(done)
          (void)]
         [(error)
          (error 'authenticate "SASL: unexpected error")]
         [(receive)
          (error 'authenticate "SASL: receive not supported")]
         [(send/receive)
          (define req (sasl-next-message ctx))
          (define res (sync (make-SaslAuthenticate-evt conn req)))
          (sasl-receive-message ctx (SaslAuthenticateResponse-data res))
          (loop)]
         [(send/done)
          (define req (sasl-next-message ctx))
          (sync (make-SaslAuthenticate-evt conn req))]))])
  (void))


;; admin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (get-metadata conn . topics)
  (sync (make-Metadata-evt conn topics)))

(define (create-topics conn topic0 . topics)
  (sync (make-CreateTopics-evt conn (cons topic0 topics))))

(define (delete-topics conn topic0 . topics)
  (sync (make-DeleteTopics-evt conn (cons topic0 topics))))
