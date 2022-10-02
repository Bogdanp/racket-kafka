#lang racket/base

(require racket/match
         racket/promise
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


;; API ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct client (manager-ch manager))

(define (make-client
         #:id [id "racket-kafka"]
         #:bootstrap-host [host "127.0.0.1"]
         #:bootstrap-port [port 9092]
         #:sasl-mechanism&ctx [sasl-mechanism&ctx #f]
         #:ssl-ctx [ssl-ctx #f])
  (define bootstrap-conn #f)
  (define metadata
    (dynamic-wind
      (lambda ()
        (set! bootstrap-conn (connect id host port ssl-ctx)))
      (lambda ()
        (when sasl-mechanism&ctx
          (apply authenticate bootstrap-conn sasl-mechanism&ctx))
        (sync (make-Metadata-evt bootstrap-conn null)))
      (lambda ()
        (disconnect bootstrap-conn))))
  (define manager-ch
    (make-channel))
  (define manager
    (thread/suspend-to-kill
     (make-manager manager-ch id sasl-mechanism&ctx ssl-ctx metadata)))
  (client manager-ch manager))

(define (get-connection c [node-ids #f])
  (force (send-manager c get-best-connection node-ids)))

(define (get-controller-connection c)
  (define metadata
    (client-metadata c))
  (get-node-connection c (λ (b)
                           (= (BrokerMetadata-node-id b)
                              (Metadata-controller-id metadata)))))

(define (get-node-connection c node-id)
  (define metadata (client-metadata c))
  (define maybe-broker
    (findf
     (cond
       [(procedure? node-id) node-id]
       [else (λ (b) (= (BrokerMetadata-node-id b) node-id))])
     (Metadata-brokers metadata)))
  (unless maybe-broker
    (raise-argument-error 'get-node-connection "node not found"))
  (force (send-manager c get-connection maybe-broker)))

(define (client-metadata c)
  (send-manager c get-metadata))

(define (reload-metadata c)
  (send-manager c reload-metadata (get-controller-connection c)))

(define (disconnect-all c)
  (send-manager c disconnect-all))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define ((make-manager manager-ch client-id sasl-mechanism&ctx ssl-ctx metadata))
  (define (connect* broker)
    (define conn
      (connect
       client-id
       (BrokerMetadata-host broker)
       (BrokerMetadata-port broker)
       ssl-ctx))
    (begin0 conn
      (when sasl-mechanism&ctx
        (apply authenticate conn sasl-mechanism&ctx))))
  (define (enliven broker conn)
    (if (connected? conn) conn (connect* broker)))
  (let loop ([st (make-state metadata)])
    (loop
     (apply
      sync
      (handle-evt
       manager-ch
       (lambda (msg)
         (match msg
           [`(get-best-connection ,res-ch ,nack ,node-ids)
            (with-handlers ([exn:fail? (λ (e) (state-add-req st `(,res-ch ,nack ,e)))])
              (match-define (state meta conns _) st)
              (define filtered-conns
                (for/hasheqv ([(node-id conn-promise) (in-hash conns)]
                              #:when (if node-ids (memv node-id node-ids) #t))
                  (values node-id conn-promise)))
              (define brokers (Metadata-brokers meta))
              (define connected-node-ids (hash-keys conns))
              (define unconnected-brokers
                (for*/list ([b (in-list brokers)]
                            [node-id (in-value (BrokerMetadata-node-id b))]
                            #:unless (memv node-id connected-node-ids)
                            #:when (if node-ids (memv node-id node-ids) #t))
                  b))
              (define-values (node-id conn)
                (cond
                  [(null? unconnected-brokers)
                   (define-values (node-id conn)
                     (find-best-connection
                      (for/hasheqv ([(node-id conn-promise) (in-hash filtered-conns)])
                        (values node-id (force conn-promise)))))
                   (define broker
                     (findf (λ (b) (= (BrokerMetadata-node-id b) node-id)) brokers))
                   (values node-id (delay/thread (enliven broker conn)))]
                  [else
                   (define broker (random-ref unconnected-brokers))
                   (define node-id (BrokerMetadata-node-id broker))
                   (values node-id (delay/thread (connect* broker)))]))
              (state-add-req
               (state-set-conn st node-id conn)
               `(,res-ch ,nack ,conn)))]

           [`(get-connection ,res-ch ,nack ,broker)
            (define node-id
              (BrokerMetadata-node-id broker))
            (define maybe-conn-promise
              (hash-ref (state-conns st) node-id #f))
            (define conn-promise
              (delay/thread
               (if maybe-conn-promise
                   (enliven broker (force maybe-conn-promise))
                   (connect* broker))))
            (state-add-req
             (state-set-conn st node-id conn-promise)
             `(,res-ch ,nack ,conn-promise))]

           [`(get-metadata ,res-ch ,nack)
            (state-add-req st `(,res-ch ,nack ,(state-meta st)))]

           [`(reload-metadata ,res-ch ,nack ,ctl-conn)
            (with-handlers ([exn:fail? (λ (e) (state-add-req st `(,res-ch ,nack ,e)))])
              (define meta (sync (make-Metadata-evt ctl-conn null)))
              (state-add-req
               (state-set-meta st meta)
               `(,res-ch ,nack ,meta)))]

           [`(disconnect-all ,res-ch ,nack)
            (with-handlers ([exn:fail? (λ (e) (state-add-req st `(,res-ch ,nack ,e)))])
              (for ([conn-promise (in-hash-values (state-conns st))])
                (disconnect (force conn-promise)))
              (state-add-req
               (state-clear-conns st)
               `(,res-ch ,nack ,(void))))]

           [_
            (begin0 st
              (log-kafka-error "client: unexpected message ~e" msg))])))
      (append
       (for/list ([r (in-list (state-reqs st))])
         (match-define `(,res-ch ,_ ,res) r)
         (handle-evt
          (channel-put-evt res-ch res)
          (λ (_) (state-remove-req st r))))
       (for/list ([r (in-list (state-reqs st))])
         (match-define `(,_ ,nack ,_) r)
         (handle-evt nack (λ (_) (state-remove-req st r)))))))))

(define-syntax-rule (send-manager c id . args)
  (sync (make-manager-evt c 'id . args)))

(define (make-manager-evt c id . args)
  (define res-ch
    (make-channel))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume
       (client-manager c)
       (current-thread))
      (begin0 res-ch
        (channel-put
         (client-manager-ch c)
         `(,id ,res-ch ,nack ,@args)))))
   (lambda (res-or-exn)
     (begin0 res-or-exn
       (when (exn:fail? res-or-exn)
         (raise res-or-exn))))))


;; manager state ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; conns: node id -> promise of Connection
(struct state (meta conns reqs)
  #:transparent)

(define (make-state meta)
  (state meta (hasheqv) null))

(define (state-set-meta st meta)
  (struct-copy state st [meta meta]))

(define (state-add-req st req)
  (struct-copy state st [reqs (cons req (state-reqs st))]))

(define (state-remove-req st req)
  (struct-copy state st [reqs (remq req (state-reqs st))]))

(define (state-set-conn st node-id conn)
  (struct-copy state st [conns (hash-set (state-conns st) node-id conn)]))

(define (state-clear-conns st)
  (struct-copy state st [conns (hasheqv)]))


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
