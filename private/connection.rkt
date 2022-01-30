#lang racket/base

(require (for-syntax racket/base
                     syntax/parse)
         racket/match
         racket/tcp
         "help.rkt"
         (prefix-in proto: "protocol.bnf"))

;; exn ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 exn:fail:kafka?)

(struct exn:fail:kafka exn:fail (code))

(define (kafka-err code message . args)
  (exn:fail:kafka (apply format message args) (current-continuation-marks) code))


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

(define (connect [host "127.0.0.1"] [port 9092])
  (define-values (in out)
    (tcp-connect host port))
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

(struct req (parser res nack ch) #:transparent)

(define (set-req-response r response)
  (struct-copy req r [res response]))

(define (read-port amount in)
  (define bs (read-bytes amount in))
  (when (eof-object? bs)
    (raise (exn:fail:network "unexpected EOF~n  the other end closed the connection prematurely"
                             (current-continuation-marks))))
  (open-input-bytes bs))

(define ((make-manager in out ch))
  (define client-id
    (kstring (current-client-id)))
  (let loop ([connected? #t]
             [seq 0]
             [reqs (hasheqv)])
    (apply
     sync
     (handle-evt
      (if connected? in never-evt)
      (lambda (_)
        (with-handlers ([exn:fail:network?
                         (lambda (e)
                           (log-kafka-error "connection failed: ~a" (exn-message e))
                           (loop #f seq reqs))]
                        [exn:fail?
                         (lambda (e)
                           (println e)
                           (log-kafka-error "failed to process response: ~a" (exn-message e))
                           (loop connected? seq reqs))])
          (define size-in (read-port 4 in))
          (define size (proto:Size size-in))
          (define resp-in (read-port size in))
          (define resp-id (proto:ResponseHeader resp-in))
          (define the-req (hash-ref reqs resp-id #f))
          (cond
            [the-req
             (define response ((req-parser the-req) resp-in))
             (loop connected? seq (hash-set reqs resp-id (set-req-response the-req response)))]
            [else
             (log-kafka-warning "dropped response w/o associated request~n  id: ~a" resp-id)
             (loop connected? seq reqs)]))))
     (handle-evt
      ch
      (lambda (msg)
        (match msg
          [`(disconnect)
           (close-output-port out)
           (close-input-port in)]
          [`(request ,k ,v ,data ,parser ,nack ,ch)
           (define id seq)
           (cond
             [connected?
              (define header-data
                (with-output-bytes
                  (proto:un-RequestHeader
                   `((APIKey_1 . ,k)
                     (APIVersion_1 . ,v)
                     (CorrelationID_1 . ,id)
                     (ClientID_1 . ,client-id)))))
              (define size-data
                (with-output-bytes
                  (proto:un-Size (+ (bytes-length header-data)
                                    (bytes-length data)))))
              (write-bytes size-data out)
              (write-bytes header-data out)
              (write-bytes data out)
              (flush-output out)
              (define the-req
                (req parser #f nack ch))
              (loop connected? (add1 seq) (hash-set reqs id the-req))]
             [else
              (define the-req
                (req parser (kafka-err -1 "not connected") nack ch))
              (loop connected? (add1 seq) (hash-set reqs id the-req))])])))
     (append
      (for/list ([(id r) (in-hash reqs)] #:when (req-res r))
        (handle-evt
         (channel-put-evt (req-ch r) (req-res r))
         (lambda (_)
           (loop connected? seq (hash-remove reqs id)))))
      (for/list ([(id r) (in-hash reqs)])
        (handle-evt
         (req-nack r)
         (lambda (_)
           (loop connected? seq (hash-remove reqs id)))))))))

(define (make-request-evt conn
                          #:key key
                          #:version v
                          #:data [data #""]
                          #:parser [parser proto:ResponseData])
  (define ch (make-channel))
  (handle-evt
   (nack-guard-evt
    (lambda (nack)
      (thread-resume (connection-mgr conn))
      (begin0 ch
        (channel-put
         (connection-ch conn)
         `(request ,key ,v ,data ,parser ,nack ,ch)))))
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
     #:key (request-key 'ApiVersions)
     #:version 0
     #:parser proto:APIVersionsResponseV0)
    (lambda (res)
      (define err-code (ref 'ErrorCode_1 res))
      (unless (zero? err-code)
        (raise (kafka-err err-code "api versions request failed")))
      (for/hasheqv ([rng (in-list (ref 'APIVersionRange_1 res))])
        (values
         (ref 'APIKey_1 rng)
         (version-range
          (ref 'MinVersion_1 rng)
          (ref 'MaxVersion_1 rng))))))))

(define (find-best-version conn id [supported (version-range 0 +inf.0)])
  (define key (request-key id))
  (define server-rng (hash-ref (connection-versions conn) key #f))
  (and server-rng
       (<= (version-range-min supported)
           (version-range-max server-rng))
       (inexact->exact
        (max (version-range-min supported)
             (min (version-range-max supported)
                  (version-range-max server-rng))))))


;; request keys ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 request-key)

(define-syntax (define-requests stx)
  (syntax-parse stx
    [(_ func-id:id request-id:id ...)
     #:with (request-num ...) (for/list ([num (in-naturals)]
                                         [stx (in-list (syntax-e #'(request-id ...)))])
                                (datum->syntax stx num))
     #'(define (func-id id)
         (case id
           [(request-id) request-num] ...
           [else (raise-argument-error 'func-id "a valid request id" id)]))]))

(define-requests request-key
  Produce
  Fetch
  ListOffsets
  Metadata
  LeaderAndIsr
  StopReplica
  UpdateMetadata
  ControlledShutdown
  OffsetCommit
  OffsetFetch
  FindCoordinator
  JoinGroup
  Heartbeat
  LeaveGroup
  SyncGroup
  DescribeGroups
  ListGroups
  SaslHandshake
  ApiVersions
  CreateTopics
  DeleteTopics
  DeleteRecords
  InitProducerId
  OffsetForLeaderEpoch
  AddPartitionsToTxn
  EndTxn
  WriteTxnMarkers
  TxnOffsetCommit
  DescribeAcls
  CreateAcls
  DeleteAcls
  DescribeConfigs
  AlterConfigs
  AlterReplicaLogDirs
  DescribeLogDirs
  SaslAuthenticate
  CreatePartitions
  CreateDelegationToken
  RenewDelegationToken
  ExpireDelegationToken
  DescribeDelegationToken
  DeleteGroups
  ElectLeaders
  IncrementalAlterConfigs
  AlterPartitionReassignments
  ListPartitionReassignments
  OffsetDelete
  DescribeClientQuotas
  AlterClientQuotas
  DescribeUserScramCredentials
  AlterUserScramCredentials
  AlterIsr
  UpdateFeatures
  DescribeCluster
  DescribeProducers
  DescribeTransactions
  ListTransactions
  AllocateProducerIds)
