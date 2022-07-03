#lang racket/base

(require box-extra
         racket/random
         sasl
         "connection.rkt"
         "serde.rkt")

(provide
 client?
 make-client
 client-metadata
 get-connection
 get-controller-connection
 get-node-connection
 reload-metadata
 disconnect-all)

(struct client
  (id
   sasl-mechanism&ctx
   ssl-ctx
   [metadata #:mutable]
   connections-box
   update-connections-box))

(define (make-client
         #:id [id "racket-kafka"]
         #:bootstrap-host [host "127.0.0.1"]
         #:bootstrap-port [port 9092]
         #:sasl-mechanism&ctx [sasl-mechanism&ctx #f]
         #:ssl-ctx [ssl-ctx #f])
  (define bootstrap-conn
    (connect id host port ssl-ctx))
  (when sasl-mechanism&ctx
    (apply authenticate bootstrap-conn sasl-mechanism&ctx))
  (define metadata
    (sync (make-Metadata-evt bootstrap-conn null)))
  (disconnect bootstrap-conn)
  (define connections-box
    (box (hasheqv)))
  (define update-connections-box
    (make-box-update-proc connections-box))
  (client id sasl-mechanism&ctx ssl-ctx metadata connections-box update-connections-box))

(define (get-connection c [node-ids #f])
  (define conns (drop-disconnected c))
  (define filtered-conns
    (for/hasheqv ([(node-id conn) (in-hash conns)]
                  #:when (if node-ids (memv node-id node-ids) #t))
      (values node-id conn)))
  (define brokers (Metadata-brokers (client-metadata c)))
  (define connected-node-ids (hash-keys conns))
  (define unconnected-brokers
    (for*/list ([b (in-list brokers)]
                [node-id (in-value (BrokerMetadata-node-id b))]
                #:unless (memv node-id connected-node-ids)
                #:when (if node-ids (memv node-id node-ids) #t))
      b))
  (if (null? unconnected-brokers)
      (find-best-connection filtered-conns)
      (establish-new-connection c conns (random-ref unconnected-brokers))))

(define (get-controller-connection c)
  (define metadata (client-metadata c))
  (define maybe-broker (findf (λ (b) (= (BrokerMetadata-node-id b)
                                        (Metadata-controller-id metadata)))
                              (Metadata-brokers metadata)))
  (unless maybe-broker
    (raise-argument-error 'get-node-connection "controller node not found"))
  (define conns
    (drop-disconnected c))
  (hash-ref conns
            (BrokerMetadata-node-id maybe-broker)
            (λ () (establish-new-connection c conns maybe-broker))))

(define (get-node-connection c node-id)
  (define brokers (Metadata-brokers (client-metadata c)))
  (define maybe-broker (findf (λ (b) (= (BrokerMetadata-node-id b) node-id)) brokers))
  (unless maybe-broker
    (raise-argument-error 'get-node-connection "unknown node id" node-id))
  (define conns
    (drop-disconnected c))
  (hash-ref conns node-id (λ ()
                            (establish-new-connection c conns maybe-broker))))

(define (reload-metadata c)
  (define ctl-conn (get-controller-connection c))
  (define metadata (sync (make-Metadata-evt ctl-conn null)))
  (begin0 metadata
    (set-client-metadata! c metadata)))

(define (disconnect-all c)
  (define connections-box (client-connections-box c))
  (for/list ([conn (in-hash-values (unbox connections-box))])
    (disconnect conn))
  (set-box! connections-box (hasheqv)))

(define (drop-disconnected c)
  ((client-update-connections-box c)
   (λ (conns)
     (for/hasheqv ([(node-id conn) (in-hash conns)] #:when (connected? conn))
       (values node-id conn)))))

(define (establish-new-connection c conns broker)
  (define node-id (BrokerMetadata-node-id broker))
  (define conn
    (connect
     (client-id c)
     (BrokerMetadata-host broker)
     (BrokerMetadata-port broker)
     (client-ssl-ctx c)))
  (when (client-sasl-mechanism&ctx c)
    (apply authenticate conn (client-sasl-mechanism&ctx c)))
  (define connections-box
    (client-connections-box c))
  (define updated-conns
    (hash-set conns node-id conn))
  (cond
    [(box-cas! connections-box conns updated-conns)
     (begin0 conn
       (log-kafka-debug "established connection to node ~a" node-id))]
    [else
     (log-kafka-debug "lost race while establishing connection, disconnecting~n current-thread: ~a" (current-thread))
     (disconnect conn)
     (log-kafka-debug "retrying get-connection")
     (get-connection c)]))

(define (find-best-connection conns)
  (for/fold ([best null]
             [least-reqs +inf.0]
             #:result (random-ref best))
            ([conn (in-hash-values conns)])
    (define reqs
      (get-requests-in-flight conn))
    (cond
      [(= reqs least-reqs)
       (values (cons conn best) least-reqs)]
      [(< reqs least-reqs)
       (values (list conn) reqs)]
      [else
       (values best least-reqs)])))

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
