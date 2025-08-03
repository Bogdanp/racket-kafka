#lang racket/base

(require actor
         data/monocle
         racket/lazy-require
         racket/match
         racket/tcp
         "error.rkt"
         "help.rkt"
         "logger.rkt"
         (prefix-in proto: "protocol.bnf")
         "proxy.rkt")

(lazy-require
 [openssl (ssl-connect)])

;; connection ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 connection?
 connect
 connected?
 disconnect
 make-request-evt
 get-requests-in-flight)

(struct connection (mgr [versions #:mutable]))

(define (connect [id "racket-kafka"]
                 [host "127.0.0.1"]
                 [port 9092]
                 [proxy #f]
                 [ssl-ctx #f])
  (define-values (in out)
    (cond
      [proxy (proxy-connect proxy host port ssl-ctx)]
      [ssl-ctx (ssl-connect host port ssl-ctx)]
      [else (tcp-connect host port)]))
  (define mgr (manager id in out))
  (define conn (connection mgr (hasheqv)))
  (define versions (get-api-versions conn))
  (set-connection-versions! conn versions)
  conn)

(define (connected? conn)
  (-connected? (connection-mgr conn)))

(define (disconnect conn)
  (-disconnect (connection-mgr conn)))

(define (get-requests-in-flight conn)
  (-requests-in-flight (connection-mgr conn)))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (all-from-out "logger.rkt"))

(define (read-port amount in)
  (define bs (read-bytes amount in))
  (when (eof-object? bs)
    (raise
     (exn:fail:network
      "unexpected EOF\n  the other end closed the connection"
      (current-continuation-marks))))
  (open-input-bytes bs))

(define-actor (manager id in out)
  #:state (make-state)
  #:event
  (lambda (st)
    (match-define (state connected? _ pending fulfilled) st)
    (apply
     choice-evt
     (handle-evt
      (if connected? in never-evt)
      (lambda (_)
        (with-handlers ([exn:fail:network?
                         (lambda (e)
                           (log-kafka-warning "connection failed: ~a" (exn-message e))
                           (state-fail-pending st (client-error "disconnected")))])
          (define size-in (read-port 4 in))
          (define size (proto:Size size-in))
          (define resp-in (read-port size in))
          (define resp-id (proto:CorrelationID resp-in))
          (cond
            [(hash-ref pending resp-id #f)
             => (λ (req)
                  (define tags
                    (and (Request-flexible? req)
                         (proto:Tags resp-in)))
                  (define response
                    ((Request-parser req) resp-in))
                  (define updated-req
                    (struct-copy Request req [res (Response response tags)]))
                  (struct-copy
                   state st
                   [pending (hash-remove pending resp-id)]
                   [fulfilled (hash-set fulfilled resp-id updated-req)]))]
            [else
             (log-kafka-warning "dropped response w/o associated request~n  id: ~a" resp-id)
             st]))))
     (append
      (for/list ([(id r) (in-hash pending)])
        (handle-evt
         (Request-nack r)
         (lambda (_)
           (&state-pending st (hash-remove pending id)))))
      (for/list ([(id r) (in-hash fulfilled)])
        (handle-evt
         (channel-put-evt
          (Request-ch r)
          (Request-res r))
         (lambda (_)
           (&state-fulfilled st (hash-remove fulfilled id)))))
      (for/list ([(id r) (in-hash (state-fulfilled st))])
        (handle-evt
         (Request-nack r)
         (lambda (_)
           (&state-fulfilled st (hash-remove fulfilled id))))))))

  (define (-request st nack ch immed flexible? k v tags request-data parser)
    (match-define (state connected? seq pending fulfilled) st)
    (unless connected?
      (raise (client-error "disconnected")))
    (define res
      (and immed (Response immed (hasheqv))))
    (define req (Request nack ch res flexible? parser))
    (define header-data
      (with-output-bytes
        ((if flexible?
             proto:un-RequestHeaderV2
             proto:un-RequestHeaderV1)
         `((APIKey_1 . ,k)
           (APIVersion_1 . ,v)
           (CorrelationID_1 . ,seq)
           (ClientID_1 . ,id)
           (Tags_1 . ,tags)))))
    (define size
      (+ (bytes-length header-data)
         (bytes-length request-data)))
    (proto:un-Size size out)
    (write-bytes header-data out)
    (write-bytes request-data out)
    (flush-output out)
    (values
     (struct-copy
      state st
      [seq (add1 seq)]
      [pending (if immed pending (hash-set pending seq req))]
      [fulfilled (if immed (hash-set fulfilled seq req) fulfilled)])
     (void)))

  (define (-requests-in-flight st)
    (values st (hash-count (state-pending st))))

  (define (-connected? st)
    (values st (state-connected? st)))

  (define (-disconnect st)
    (close-input-port in)
    (close-output-port out)
    (log-kafka-debug "client ~a disconnected" id)
    (values (&state-connected? st #f) (void))))

(define (make-request-evt conn
                          #:key k
                          #:version v
                          #:tags [tags (hasheqv)]
                          #:parser parser
                          #:data [data #""]
                          #:flexible? [flexible? #f]
                          #:immed-response [immed #f])
  (define ch (make-channel))
  (define mgr (connection-mgr conn))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (replace-evt
       (-request-evt mgr nack ch immed flexible? k v tags data parser)
       (lambda (_) ch))))
   (lambda (res-or-exn)
     (when (exn:fail? res-or-exn)
       (raise res-or-exn))
     (Response-data res-or-exn))))


;; manager state ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct Request (nack ch res flexible? parser))
(struct Response (data tags))

(struct state (connected? seq pending fulfilled))
(define-struct-lenses state)

(define (make-state)
  (state
   #;connected? #t
   #;seq 0
   #;pending (hasheqv)
   #;fulfilled (hasheqv)))

(define (state-fail-pending st err)
  (define fulfilled
    (for/fold ([fulfilled (state-fulfilled st)])
              ([(id req) (in-hash (state-pending st))])
      (hash-set fulfilled id (struct-copy Request req [res err]))))
  (struct-copy
   state st
   [pending (hasheqv)]
   [fulfilled fulfilled]))


;; version ranges ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (struct-out version-range)
 get-api-versions
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
