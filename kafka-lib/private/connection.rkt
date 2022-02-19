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
 connected?
 disconnect
 make-request-evt
 get-requests-in-flight)

(define current-client-id
  (make-parameter "racket-kafka"))

(struct connection (ch mgr [versions #:mutable]))

(define (connect [host "127.0.0.1"]
                 [port 9092]
                 [ssl-ctx #f])
  (define-values (in out)
    (if ssl-ctx
        (ssl-connect host port ssl-ctx)
        (tcp-connect host port)))
  (define ch (make-channel))
  (define mgr (thread/suspend-to-kill (make-manager in out ch)))
  (define conn (connection ch mgr (hasheqv)))
  (begin0 conn
    (set-connection-versions! conn (get-api-versions conn))))

(define (connected? conn)
  (and (not (thread-dead? (connection-mgr conn)))
       (sync (make-message-evt conn `(connected?)))))

(define (disconnect conn)
  (define mgr (connection-mgr conn))
  (unless (thread-dead? mgr)
    (thread-resume mgr)
    (channel-put (connection-ch conn) `(disconnect))
    (void (sync mgr))))

(define (get-requests-in-flight conn)
  (sync (make-message-evt conn '(requests-in-flight))))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 log-kafka-debug
 log-kafka-info
 log-kafka-warning
 log-kafka-error
 log-kafka-fatal)

(define-logger kafka)

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
  (let loop ([s (make-state)])
    (apply
     sync
     (handle-evt
      (if (state-connected? s) in never-evt)
      (lambda (_)
        (with-handlers ([exn:fail:network?
                         (lambda (e)
                           (log-kafka-error "connection failed: ~a" (exn-message e))
                           (loop (set-state-disconnected s)))]
                        [exn:fail?
                         (lambda (e)
                           (log-kafka-error "failed to process response: ~a" (exn-message e))
                           (loop s))])
          (define size-in (read-port 4 in))
          (define size (proto:Size size-in))
          (define resp-in (read-port size in))
          (define resp-id (proto:CorrelationID resp-in))
          (cond
            [(find-state-req s resp-id)
             => (λ (req)
                  (define tags
                    (and (KReq-flexible? req)
                         (proto:Tags resp-in)))
                  (define response
                    ((KReq-parser req) resp-in))
                  (define updated-req
                    (struct-copy KReq req [res #:parent Req (KRes response tags)]))
                  (loop (update-state-req s resp-id updated-req)))]
            [else
             (log-kafka-warning "dropped response w/o associated request~n  id: ~a" resp-id)
             (loop s)]))))
     (handle-evt
      ch
      (lambda (msg)
        (match msg
          [`(disconnect)
           (close-input-port in)
           (close-output-port out)
           (log-kafka-debug "client ~a disconnected" client-id)]

          [`(connected? ,nack ,ch)
           (define req (Req nack ch (state-connected? s)))
           (loop (add-state-req s req))]

          [`(requests-in-flight ,nack ,ch)
           (define req (Req nack ch (state-req-count s)))
           (loop (add-state-req s req))]

          [`(request ,immed-response ,flexible? ,k ,v ,tags ,request-data ,parser ,nack ,ch)
           #:when (state-connected? s)
           (with-handlers ([exn:fail?
                            (lambda (err)
                              (define req (Req nack ch err))
                              (loop (add-state-req s req)))])
             (define maybe-res (and immed-response (KRes immed-response (hasheqv))))
             (define req (KReq nack ch maybe-res flexible? parser))
             (define header-data
               (with-output-bytes
                 ((if flexible?
                      proto:un-RequestHeaderV2
                      proto:un-RequestHeaderV1)
                  `((APIKey_1 . ,k)
                    (APIVersion_1 . ,v)
                    (CorrelationID_1 . ,(state-next-id s))
                    (ClientID_1 . ,client-id)
                    (Tags_1 . ,tags)))))
             (define size
               (+ (bytes-length header-data)
                  (bytes-length request-data)))
             (proto:un-Size size out)
             (write-bytes header-data out)
             (write-bytes request-data out)
             (flush-output out)
             (loop (add-state-req s req)))]

          [`(request ,_ ,_ ,_ ,_ ,_ ,_ ,_ ,nack ,ch)
           (define err (client-error "not connected"))
           (define req (Req nack ch err))
           (loop (add-state-req s req))]

          [msg
           (log-kafka-error "invalid message: ~e" msg)
           (loop s)])))
     (append
      (for/list ([(id r) (in-hash (state-reqs s))] #:when (Req-res r))
        (handle-evt
         (channel-put-evt (Req-ch r) (Req-res r))
         (lambda (_)
           (loop (remove-state-req s id)))))
      (for/list ([(id r) (in-hash (state-reqs s))])
        (handle-evt
         (Req-nack r)
         (lambda (_)
           (loop (remove-state-req s id)))))))))

(define (make-request-evt conn
                          #:key key
                          #:version v
                          #:tags [tags (hasheqv)]
                          #:parser parser
                          #:data [data #""]
                          #:flexible? [flexible? #f]
                          #:immed-response [immed-response #f])
  (define msg `(request ,immed-response ,flexible? ,key ,v ,tags ,data ,parser))
  (handle-evt (make-message-evt conn msg) KRes-data))

(define (make-message-evt conn msg)
  (define ch (make-channel))
  (define mgr (connection-mgr conn))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume mgr)
      (begin0 ch
        (channel-put
         (connection-ch conn)
         (append msg `(,nack ,ch))))))
   (lambda (res-or-exn)
     (begin0 res-or-exn
       (when (exn:fail? res-or-exn)
         (raise res-or-exn))))))


;; manager state ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Req (nack ch res))
(struct KReq Req (flexible? parser))
(struct KRes (data tags))

(struct state (connected? seq reqs))

(define (make-state)
  (state #t 0 (hasheqv)))

(define (state-next-id s)
  (state-seq s))

(define (state-req-count s)
  (hash-count (state-reqs s)))

(define (add-state-req s req)
  (define id (state-next-id s))
  (define reqs (hash-set (state-reqs s) id req))
  (struct-copy state s
               [seq (add1 id)]
               [reqs reqs]))

(define (find-state-req s id)
  (hash-ref (state-reqs s) id #f))

(define (update-state-req s id req)
  (struct-copy state s [reqs (hash-set (state-reqs s) id req)]))

(define (remove-state-req s id)
  (struct-copy state s [reqs (hash-remove (state-reqs s) id)]))

(define (set-state-disconnected s)
  (struct-copy state s [connected? #f]))


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
        (raise-server-error err-code))
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