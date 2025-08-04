#lang racket/base

(require actor
         data/monocle
         racket/lazy-require
         racket/match
         racket/promise
         racket/random
         "connection.rkt"
         "error.rkt"
         "serde.rkt")

(lazy-require
 [sasl (sasl-state
        sasl-next-message
        sasl-receive-message)])

;; API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 client?
 make-client
 client-metadata
 get-connection
 get-controller-connection
 get-node-connection
 reload-metadata
 disconnect-all)

(struct client (manager))

(define (make-client
         #:id [id "racket-kafka"]
         #:bootstrap-host [host "127.0.0.1"]
         #:bootstrap-port [port 9092]
         #:sasl-mechanism&ctx [sasl-mechanism&ctx #f]
         #:ssl-ctx [ssl-ctx #f]
         #:proxy [proxy #f])
  (define bootstrap-conn #f)
  (define metadata
    (dynamic-wind
      (lambda ()
        (set! bootstrap-conn (connect id host port proxy ssl-ctx)))
      (lambda ()
        (when sasl-mechanism&ctx
          (apply authenticate host port bootstrap-conn sasl-mechanism&ctx))
        (sync (make-Metadata-evt bootstrap-conn null)))
      (lambda ()
        (disconnect bootstrap-conn))))
  (define (connect* broker)
    (define host (BrokerMetadata-host broker)) ;; noqa
    (define port (BrokerMetadata-port broker)) ;; noqa
    (define conn (connect id host port proxy ssl-ctx))
    (when sasl-mechanism&ctx
      (apply authenticate host port conn sasl-mechanism&ctx))
    conn)
  (client (manager connect* metadata)))

(define (get-connection c [node-ids #f])
  (force (-get-best-connection (client-manager c) node-ids)))

(define (get-controller-connection c)
  (define metadata
    (client-metadata c))
  (define (controller-node? b)
    (= (BrokerMetadata-node-id b)
       (Metadata-controller-id metadata)))
  (get-node-connection c controller-node?))

(define (get-node-connection c node-id)
  (define metadata (client-metadata c))
  (define maybe-broker
    (findf
     (cond
       [(procedure? node-id) node-id]
       [else (λ (b) (= (BrokerMetadata-node-id b) node-id))])
     (Metadata-brokers metadata)))
  (unless maybe-broker
    (raise-argument-error 'get-node-connection "node-id/c" node-id))
  (force (-get-connection (client-manager c) maybe-broker)))

(define (client-metadata c)
  (-get-metadata (client-manager c)))

(define (reload-metadata c)
  (define manager (client-manager c))
  (define ctl-conn (get-controller-connection c))
  (force (-reload-metadata manager ctl-conn)))

(define (disconnect-all c)
  (force (-disconnect-all (client-manager c))))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-actor (manager connect meta)
  #:state (make-state meta)
  #:event (lambda (st)
            (handle-evt
             (apply choice-evt (state-evts st))
             (lambda (update-proc)
               (update-proc st))))

  (define/private (enliven broker conn)
    (if (connected? conn) conn (connect broker)))

  (define (-get-best-connection st node-ids)
    (let connect-loop ([st st])
      (match-define (state meta conns _) st)
      (define brokers (Metadata-brokers meta))
      (define connected-node-ids (hash-keys conns))
      (define unconnected-brokers
        (for*/list ([b (in-list brokers)]
                    [node-id (in-value (BrokerMetadata-node-id b))]
                    #:unless (memv node-id connected-node-ids)
                    #:when (if node-ids (memv node-id node-ids) #t))
          b))
      (define filtered-conns
        (for/hasheqv ([(node-id conn-promise) (in-hash conns)]
                      #:when (if node-ids (memv node-id node-ids) #t))
          (values node-id conn-promise)))
      (cond
        [(null? unconnected-brokers)
         (define dead-conns
           (for/hasheqv ([(node-id conn-promise) (in-hash filtered-conns)]
                         #:unless (with-handlers ([exn:fail? (λ (_) #f)])
                                    (connected? (force conn-promise))))
             (values node-id #t)))
         (cond
           [(hash-empty? filtered-conns)
            (raise (client-error "invalid node subset: ~e" node-ids))]
           [(hash-empty? dead-conns)
            (define filtered-live-conns
              (for/hasheqv ([(node-id conn-promise) (in-hash filtered-conns)])
                (values node-id (force conn-promise))))
            (define-values (node-id conn)
              (find-best-connection filtered-live-conns))
            (define broker
              (findf (λ (b) (= (BrokerMetadata-node-id b) node-id)) brokers))
            (define conn-promise
              (delay/thread (enliven broker conn)))
            (log-kafka-debug "client: picked node ~e from ~e" node-id (hash-keys filtered-live-conns))
            (values (state-set-conn st node-id conn-promise) conn-promise)]
           [else
            (log-kafka-debug "client: found stale connections; retrying")
            (define all-live-conns
              (for/hasheqv ([(node-id conn-promise) (in-hash conns)]
                            #:unless (hash-has-key? dead-conns node-id))
                (values node-id conn-promise)))
            (connect-loop (&state-conns st all-live-conns))])]
        [else
         (define broker (random-ref unconnected-brokers))
         (define node-id (BrokerMetadata-node-id broker))
         (define conn-promise (delay/thread (connect broker)))
         (log-kafka-debug "client: picked unconnected node ~e" node-id)
         (values (state-set-conn st node-id conn-promise) conn-promise)])))

  (define (-get-connection st broker)
    (define node-id
      (BrokerMetadata-node-id broker))
    (define conn-promise
      (delay/thread
       (cond
         [(hash-ref (state-conns st) node-id #f)
          => (lambda (promise)
               (define conn
                 (with-handlers ([exn:fail? (λ (_) (connect broker))])
                   (force promise)))
               (enliven broker conn))]
         [else
          (connect broker)])))
    (values (state-set-conn st node-id conn-promise) conn-promise))

  (define (-get-metadata st)
    (values st (state-meta st)))

  (define (-reload-metadata st ctl-conn)
    (define metadata-promise
      (delay/thread
       (sync (make-Metadata-evt ctl-conn null))))
    (define metadata-evt
      (handle-evt
       metadata-promise
       (lambda (_)
         (with-handlers ([exn:fail? (λ (_) values)])
           (define metadata (force metadata-promise))
           (lambda (st) ;; noqa
             (&state-meta st metadata))))))
    (values
     (state-add-evt st metadata-evt)
     metadata-promise))

  (define (-disconnect-all st)
    (define promise
      (delay/thread
       (for ([conn-promise (in-hash-values (state-conns st))])
         (with-handlers ([exn:fail? void])
           (disconnect (force conn-promise))))))
    (values (&state-conns st (hasheqv)) promise)))


;; manager state ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; conns: node id -> promiseof Connection
;; evts: listof evtof (state -> state)
(struct state (meta conns evts))
(define-struct-lenses state)

(define (make-state meta)
  (state meta (hasheqv) null))

(define (state-add-evt st evt)
  ;; Adds an event to the event list that removes itself from the list
  ;; upon synchronization.
  (define update-evt
    (handle-evt
     evt (lambda (update-proc)
           (lambda (st) ;; noqa
             (lens-update
              &state-evts (update-proc st)
              (lambda (evts)
                (remq update-evt evts)))))))
  (lens-update
   &state-evts st
   (λ (evts)
     (cons update-evt evts))))

(define (state-set-conn st node-id conn)
  (lens-update
   &state-conns st
   (λ (conns)
     (hash-set conns node-id conn))))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (find-best-connection conns)
  (for/fold ([best null]
             [least-reqs +inf.0]
             #:result (apply values (random-ref best)))
            ([(node-id conn) (in-hash conns)])
    (define reqs
      (get-requests-in-flight conn))
    (cond
      [(= reqs least-reqs)
       (values `((,node-id ,conn) . ,best) least-reqs)]
      [(< reqs least-reqs)
       (values `((,node-id ,conn)) reqs)]
      [else
       (values best least-reqs)])))

(define (authenticate host port conn mechanism ctx)
  (sync (make-SaslHandshake-evt conn mechanism))
  (case mechanism
    [(plain)
     (define req
       (if (string? ctx)
           (string->bytes/utf-8 ctx)
           ctx))
     (sync (make-SaslAuthenticate-evt conn req))]
    [else
     (let ([ctx (ctx host port)])
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
            (define res (sync (make-SaslAuthenticate-evt conn (->bytes req))))
            (sasl-receive-message ctx (SaslAuthenticateResponse-data res))
            (loop)]
           [(send/done)
            (define req (sasl-next-message ctx))
            (sync (make-SaslAuthenticate-evt conn (->bytes req)))])))])
  (void))

(define (->bytes s)
  (if (bytes? s) s (string->bytes/utf-8 s)))
