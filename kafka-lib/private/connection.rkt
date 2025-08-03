#lang racket/base

(require actor
         data/monocle
         racket/lazy-require
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
    (apply
     choice-evt
     (handle-evt
      (if (state-connected? st) in never-evt)
      (lambda (_)
        (with-handlers ([exn:fail:network?
                         (lambda (e)
                           (log-kafka-warning "connection failed: ~a" (exn-message e))
                           (fail-pending-reqs
                            (set-state-disconnected st)
                            (client-error "disconnected")))]
                        [exn:fail?
                         (lambda (e)
                           (begin0 st
                             (log-kafka-error "failed to process response: ~a" (exn-message e))))])
          (define size-in (read-port 4 in))
          (define size (proto:Size size-in))
          (define resp-in (read-port size in))
          (define resp-id (proto:CorrelationID resp-in))
          (cond
            [(find-state-req st resp-id)
             => (λ (req)
                  (define tags
                    (and (KReq-flexible? req)
                         (proto:Tags resp-in)))
                  (define response
                    ((KReq-parser req) resp-in))
                  (define updated-req
                    (struct-copy KReq req [res (KRes response tags)]))
                  (update-state-req st resp-id updated-req))]
            [else
             (begin0 st
               (log-kafka-warning "dropped response w/o associated request~n  id: ~a" resp-id))]))))
     (append
      (for/list ([(id r) (in-hash (state-reqs st))]
                 #:unless (pending? (KReq-res r)))
        (handle-evt
         (channel-put-evt
          (KReq-ch r)
          (KReq-res r))
         (lambda (_)
           (remove-state-req st id))))
      (for/list ([(id r) (in-hash (state-reqs st))])
        (handle-evt
         (KReq-nack r)
         (lambda (_)
           (remove-state-req st id)))))))

  (define (-request st nack ch immed-response flexible? k v tags request-data parser)
    (unless (state-connected? st)
      (raise (client-error "disconnected")))
    (define res
      (if immed-response
          (KRes immed-response (hasheqv))
          pending))
    (define req (KReq nack ch res flexible? parser))
    (define header-data
      (with-output-bytes
        ((if flexible?
             proto:un-RequestHeaderV2
             proto:un-RequestHeaderV1)
         `((APIKey_1 . ,k)
           (APIVersion_1 . ,v)
           (CorrelationID_1 . ,(state-next-id st))
           (ClientID_1 . ,id)
           (Tags_1 . ,tags)))))
    (define size
      (+ (bytes-length header-data)
         (bytes-length request-data)))
    (proto:un-Size size out)
    (write-bytes header-data out)
    (write-bytes request-data out)
    (flush-output out)
    (values (add-state-req st req) (void)))

  (define (-requests-in-flight st)
    (values st (state-req-count st)))

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
                          #:immed-response [immed-response #f])
  (define ch (make-channel))
  (define mgr (connection-mgr conn))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (replace-evt
       (-request-evt mgr nack ch immed-response flexible? k v tags data parser)
       (lambda (_) ch))))
   (lambda (res-or-exn)
     (when (exn:fail? res-or-exn)
       (raise res-or-exn))
     (KRes-data res-or-exn))))


;; manager state ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct KReq (nack ch res flexible? parser))
(struct KRes (data tags))

(struct state (connected? seq reqs))
(define-struct-lenses state)

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

(define (fail-pending-reqs s err)
  (struct-copy state s [reqs (for/hasheqv ([(id req) (in-hash (state-reqs s))])
                               (define updated-req
                                 (if (and (KReq? req) (pending? (KReq-res req)))
                                     (struct-copy KReq req [res err])
                                     req))
                               (values id updated-req))]))

(define pending
  (gensym 'pending))

(define (pending? v)
  (eq? v pending))


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
