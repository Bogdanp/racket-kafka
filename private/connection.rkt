#lang racket/base

(require openssl
         racket/match
         racket/tcp
         "error.rkt"
         "help.rkt"
         (prefix-in proto: "protocol.bnf"))


;; connection ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 current-client-id
 connection?
 connect
 disconnect
 make-request-evt)

(define current-client-id
  (make-parameter "racket-kafka"))

(struct connection (ch mgr [versions #:mutable]))

(define (connect [host "127.0.0.1"] [port 9092] #:ssl [ssl-ctx #f])
  (define-values (in out)
    (if ssl-ctx
        (ssl-connect host port ssl-ctx)
        (tcp-connect host port)))
  (define ch (make-channel))
  (define mgr (thread/suspend-to-kill (make-manager in out ch)))
  (define conn (connection ch mgr (hasheqv)))
  (begin0 conn
    (set-connection-versions! conn (get-api-versions conn))))

(define (disconnect conn)
  (define mgr (connection-mgr conn))
  (unless (thread-dead? mgr)
    (thread-resume mgr)
    (channel-put (connection-ch conn) `(disconnect))))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-logger kafka)

(struct req (flexible? parser res nack ch)
  #:transparent)

(define (set-req-response r response)
  (struct-copy req r [res response]))

(define (read-port amount in)
  (define bs (read-bytes amount in))
  (log-kafka-debug "response bytes: ~s" bs)
  (when (eof-object? bs)
    (raise
     (exn:fail:network
      "unexpected EOF\n  the other end closed the connection"
      (current-continuation-marks))))
  (open-input-bytes bs))

(define ((make-manager in out ch))
  (define client-id
    (current-client-id))
  (let loop ([ok? #t]
             [seq 0]
             [reqs (hasheqv)])
    (apply
     sync
     (handle-evt
      (if ok? in never-evt)
      (lambda (_)
        (with-handlers ([exn:fail:network?
                         (lambda (e)
                           (log-kafka-error "connection failed: ~a" (exn-message e))
                           (loop #f seq reqs))]
                        [exn:fail?
                         (lambda (e)
                           (log-kafka-error "failed to process response: ~a" (exn-message e))
                           (loop ok? seq reqs))])
          (define size-in (read-port 4 in))
          (define size (proto:Size size-in))
          (define resp-in (read-port size in))
          (define resp-id (proto:CorrelationID resp-in))
          (define the-req (hash-ref reqs resp-id #f))
          (when (req-flexible? the-req)
            ;; FIXME: must return tags
            (void (proto:Tags resp-in)))
          (cond
            [the-req
             (define response ((req-parser the-req) resp-in))
             (loop ok? seq (hash-set reqs resp-id (set-req-response the-req response)))]
            [else
             (log-kafka-warning "dropped response w/o associated request~n  id: ~a" resp-id)
             (loop ok? seq reqs)]))))
     (handle-evt
      ch
      (lambda (msg)
        (match msg
          [`(disconnect)
           (close-output-port out)
           (close-input-port in)]
          [`(request ,flexible? ,k ,v ,tags ,data ,parser ,nack ,ch)
           (define id seq)
           (cond
             [ok?
              (define header-data
                (with-output-bytes
                  (if flexible?
                      (proto:un-RequestHeaderV2
                       `((APIKey_1 . ,k)
                         (APIVersion_1 . ,v)
                         (CorrelationID_1 . ,id)
                         (ClientID_1 . ,client-id)
                         (Tags_1 . ,tags)))
                      (proto:un-RequestHeaderV1
                       `((APIKey_1 . ,k)
                         (APIVersion_1 . ,v)
                         (CorrelationID_1 . ,id)
                         (ClientID_1 . ,client-id))))))
              (define size-data
                (with-output-bytes
                  (proto:un-Size (+ (bytes-length header-data)
                                    (bytes-length data)))))
              (write-bytes size-data out)
              (write-bytes header-data out)
              (write-bytes data out)
              (flush-output out)
              (define the-req
                (req flexible? parser #f nack ch))
              (loop ok? (add1 seq) (hash-set reqs id the-req))]
             [else
              (define the-req
                (req parser (kafka-error -1 "not connected") nack ch))
              (loop ok? (add1 seq) (hash-set reqs id the-req))])])))
     (append
      (for/list ([(id r) (in-hash reqs)] #:when (req-res r))
        (handle-evt
         (channel-put-evt (req-ch r) (req-res r))
         (lambda (_)
           (loop ok? seq (hash-remove reqs id)))))
      (for/list ([(id r) (in-hash reqs)])
        (handle-evt
         (req-nack r)
         (lambda (_)
           (loop ok? seq (hash-remove reqs id)))))))))

(define (make-request-evt conn
                          #:key key
                          #:version v
                          #:tags [tags (hasheqv)]
                          #:parser parser
                          #:data [data #""]
                          #:flexible? [flexible? #f])
  (define ch (make-channel))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume (connection-mgr conn))
      (begin0 ch
        (channel-put
         (connection-ch conn)
         `(request ,flexible? ,key ,v ,tags ,data ,parser ,nack ,ch)))))
   (lambda (res-or-exn)
     (begin0 res-or-exn
       (when (exn:fail? res-or-exn)
         (raise res-or-exn))))))


;; version ranges ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (struct-out version-range)
 find-best-version)

(struct version-range (min max)
  #:transparent)

(define (get-api-versions conn)
  (sync
   (handle-evt
    (make-request-evt
     conn
     #:key 18
     #:version 0
     #:parser proto:APIVersionsResponseV0)
    (lambda (res)
      (define err-code (ref 'ErrorCode_1 res))
      (unless (zero? err-code)
        (raise-kafka-error err-code))
      (for/hasheqv ([rng (in-list (ref 'APIVersionRange_1 res))])
        (values
         (ref 'APIKey_1 rng)
         (version-range
          (ref 'MinVersion_1 rng)
          (ref 'MaxVersion_1 rng))))))))

(define (find-best-version conn key [supported (version-range 0 +inf.0)])
  (define server-rng
    (hash-ref (connection-versions conn) key #f))
  (and server-rng
       (<= (version-range-min supported)
           (version-range-max server-rng))
       (inexact->exact
        (max (version-range-min supported)
             (min (version-range-max supported)
                  (version-range-max server-rng))))))
