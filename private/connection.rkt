#lang racket/base

(require racket/match
         racket/port
         racket/tcp
         "help.rkt"
         (prefix-in proto: "protocol.bnf"))

(provide
 current-client-id
 connection?
 connect
 disconnect
 make-request-evt)

(define current-client-id
  (make-parameter "racket-kafka"))

(struct connection (ch mgr)
  #:transparent)

(define (connect host port)
  (define-values (in out)
    (tcp-connect host port))
  (define ch (make-channel))
  (define mgr (thread/suspend-to-kill (make-manager in out ch)))
  (connection ch mgr))

(define (disconnect conn)
  (define mgr (connection-mgr conn))
  (unless (thread-dead? mgr)
    (thread-resume mgr)
    (channel-put (connection-ch conn) `(disconnect))))


;; manager ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct req (res nack ch) #:transparent)

(define ((make-manager in out ch))
  (define client-id
    (kstring (current-client-id)))
  (let loop ([seq 0] [reqs (hasheqv)])
    (apply
     sync
     (handle-evt
      in
      (lambda (_)
        (define size (proto:Size in))
        (define data (read-bytes size in))
        (define-values (id response)
          (call-with-input-bytes data
            (lambda (resp-in)
              (values
               (proto:ResponseHeader resp-in)
               (proto:ResponseData resp-in)))))
        (loop seq (if (hash-has-key? reqs id)
                      (hash-update reqs id (λ (r) (struct-copy req r [res response])))
                      reqs))))
     (handle-evt
      ch
      (lambda (msg)
        (match msg
          [`(disconnect)
           (close-output-port out)
           (close-input-port in)]
          [`(request ,k ,v ,data ,nack ,ch)
           (define id seq)
           (define header-data
             (with-output-bytes
               (proto:un-RequestHeader
                `((APIKey_1 . ,k)
                  (APIVersion_1 . ,v)
                  (CorrelationID_1 . ,id)
                  (ClientID_1 . ,client-id)))))
           (define request-data
             (if data
                 (with-output-bytes
                   (proto:un-RequestData data))
                 #""))
           (define size-data
             (with-output-bytes
               (proto:un-Size (+ (bytes-length header-data)
                                 (bytes-length request-data)))))
           (write-bytes size-data out)
           (write-bytes header-data out)
           (write-bytes request-data out)
           (flush-output out)
           (define the-req
             (req #f nack ch))
           (loop (add1 seq) (hash-set reqs id the-req))])))
     (append
      (for/list ([(id r) (in-hash reqs)] #:when (req-res r))
        (handle-evt
         (channel-put-evt (req-ch r) (req-res r))
         (lambda (_)
           (loop seq (hash-remove reqs id)))))
      (for/list ([(id r) (in-hash reqs)])
        (handle-evt
         (req-nack r)
         (lambda (_)
           (loop seq (hash-remove reqs id)))))))))

(define (make-request-evt conn k v data)
  (define ch (make-channel))
  (nack-guard-evt
   (lambda (nack)
     (thread-resume (connection-mgr conn))
     (begin0 ch
       (channel-put
        (connection-ch conn)
        `(request ,k ,v ,data ,nack ,ch))))))
