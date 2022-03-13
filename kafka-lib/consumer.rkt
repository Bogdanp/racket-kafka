#lang racket/base

(require racket/contract
         racket/hash
         racket/list
         racket/match
         racket/port
         (prefix-in assign: "private/assignor.rkt")
         "private/batch.rkt"
         "private/client.rkt"
         "private/connection.rkt"
         "private/error.rkt"
         "private/help.rkt"
         (prefix-in cproto: "private/protocol-consumer.bnf")
         "private/record.rkt"
         "private/serde.rkt")

(provide
 record?
 record-offset
 record-key
 record-value

 (contract-out
  [consumer? (-> any/c boolean?)]
  [make-consumer (->* (client? string?)
                      (#:assignors (listof assign:assignor?)
                       #:reset-strategy (or/c 'earliest 'latest)
                       #:session-timeout-ms exact-nonnegative-integer?)
                      #:rest (non-empty-listof string?)
                      consumer?)]
  [consume-evt (->* (consumer?) (exact-nonnegative-integer?) evt?)]
  [consumer-commit (-> consumer? void?)]
  [consumer-stop (-> consumer? void?)]))

(struct consumer
  (client
   group-id
   [generation-id #:mutable]
   [member-id #:mutable]
   [topics #:mutable]
   [topic-partitions #:mutable]
   heartbeat-in-ch
   heartbeat-out-ch
   [heartbeat-thd #:mutable]
   assignors
   offset-reset-strategy
   sesion-timeout-ms))

(define (make-consumer client group-id
                       #:assignors [assignors (list assign:range assign:round-robin)]
                       #:reset-strategy [offset-reset-strategy 'earliest]
                       #:session-timeout-ms [session-timeout-ms 30000]
                       . topics)
  (define heartbeat-in-ch (make-channel))
  (define heartbeat-out-ch (make-channel))
  (define the-consumer
    (consumer
     client
     group-id
     0            ;; generation-id
     #f           ;; member-id
     topics
     #f           ;; topic-partitions
     heartbeat-in-ch
     heartbeat-out-ch
     #f           ;; heartbeat-thd
     assignors
     offset-reset-strategy
     session-timeout-ms))
  (begin0 the-consumer
    (join-group! the-consumer)
    (start-heartbeat-thd! the-consumer)))

(define (consume-evt c [timeout 1000])
  (choice-evt
   (handle-evt
    (consumer-heartbeat-out-ch c)
    (lambda (e)
      (cond
        [(exn:fail:kafka:server? e)
         (case (error-code-symbol (exn:fail:kafka:server-code e))
           [(unknown-member-id rebalance-in-progress)
            (join-group! c)
            (start-heartbeat-thd! c)
            (values 'rebalance (consumer-topic-partitions c))]
           [else (raise e)])]
        [else (raise e)])))
   (handle-evt
    (make-Fetch-evt
     (get-connection (consumer-client c))
     (for/hash ([(topic pids) (in-hash (consumer-topic-partitions c))])
       (values topic (for/list ([(pid offset) (in-hash pids)])
                       (make-TopicPartition
                        #:id pid
                        #:offset offset))))
     timeout)
    (lambda (res)
      (define current-topic-partitions
        (consumer-topic-partitions c))
      (define records
        (for*/vector ([(topic partitions) (in-hash (FetchResponse-topics res))]
                      [p (in-list partitions)]
                      [pid (in-value (FetchResponsePartition-id p))]
                      [offsets (in-value (hash-ref current-topic-partitions topic))]
                      [offset (in-value (hash-ref offsets pid))]
                      [b (in-list (FetchResponsePartition-batches p))]
                      [r (in-vector (batch-records b))]
                      #:when (>= (record-offset r) offset))
          r))
      (define offsets
        (for*/fold ([offsets (hash)])
                   ([(topic partitions) (in-hash (FetchResponse-topics res))]
                    [p (in-list partitions)]
                    [b (in-list (FetchResponsePartition-batches p))])
          (define key (cons topic (FetchResponsePartition-id p)))
          (define size (batch-size b))
          (define last-record
            (and (not (zero? size))
                 (vector-ref (batch-records b) (sub1 size))))
          (if last-record
              (hash-set offsets key (add1 (record-offset last-record)))
              offsets)))
      (define topic-partitions
        (for/hash ([(topic partitions) (in-hash (consumer-topic-partitions c))])
          (values topic (for/hash ([(pid offset) (in-hash partitions)])
                          (values pid (hash-ref offsets (cons topic pid) offset))))))
      (set-consumer-topic-partitions! c topic-partitions)
      (values 'records records)))))

(define (consumer-commit c)
  (void
   (sync
    (handle-evt
     (make-Commit-evt
      (get-connection (consumer-client c))
      (consumer-group-id c)
      (consumer-generation-id c)
      (consumer-member-id c)
      (for/hash ([(topic partitions) (in-hash (consumer-topic-partitions c))])
        (values topic (for/list ([(pid offset) (in-hash partitions)])
                        (make-CommitPartition
                         #:id pid
                         #:offset offset)))))
     (lambda (res)
       (for* ([(topic partitions) (in-hash res)]
              [part (in-list partitions)])
         (define pid (CommitPartitionResult-id part))
         (define err (CommitPartitionResult-error-code part))
         (case (error-code-symbol err)
           [(no-error) (void)]
           [(unknown-member-id rebalance-in-progress)
            (log-kafka-warning "commit on (~a, ~a) ignored due to rebalance" topic pid)]
           [else
            (raise-server-error err)])))))))

(define (consumer-stop c)
  (when (consumer-member-id c)
    (stop-heartbeat-thd! c)
    (void (leave-group! c))))

(define (start-heartbeat-thd! c #:interval-ms [interval-ms 3000])
  (define thd
    (thread
     (lambda ()
       (define group-id (consumer-group-id c))
       (define generation-id (consumer-generation-id c))
       (define member-id (consumer-member-id c))
       (define in-ch (consumer-heartbeat-in-ch c))
       (define out-ch (consumer-heartbeat-out-ch c))
       (with-handlers ([exn:fail?
                        (lambda (e)
                          (log-kafka-debug "heartbeat: ~a" (exn-message e))
                          (sync
                           (handle-evt in-ch void)
                           (channel-put-evt out-ch e)))])
         (define conn (get-coordinator c))
         (let loop ([pending-evt #f])
           (sync
            (handle-evt in-ch void)
            (if pending-evt
                (handle-evt
                 pending-evt
                 (lambda (_res)
                   (log-kafka-debug
                    "heartbeat ok~n  group-id: ~s~n  generation-id: ~s~n  member-id: ~s"
                    group-id generation-id member-id)
                   (loop #f)))
                (handle-evt
                 (alarm-evt (+ (current-inexact-milliseconds) interval-ms))
                 (lambda (_)
                   (log-kafka-debug
                    "sending heartbeat~n  group-id: ~s~n  generation-id: ~s~n  member-id: ~s"
                    group-id generation-id member-id)
                   (loop (make-Heartbeat-evt conn group-id generation-id member-id))))))))
       (log-kafka-debug "heartbeat thread stopped"))))
  (set-consumer-heartbeat-thd! c thd))

(define (stop-heartbeat-thd! c)
  (define thd (consumer-heartbeat-thd c))
  (define ch (consumer-heartbeat-in-ch c))
  (sync
   (thread-dead-evt thd)
   (handle-evt
    (channel-put-evt ch '(stop))
    (lambda (_)
      (sync thd)))))

(define (join-group! c)
  (define conn (get-coordinator c))
  (define assignors (consumer-assignors c))
  (define protocols
    (for/list ([assignor (in-list assignors)])
      (make-Protocol
       #:name (assign:assignor-name assignor)
       #:metadata (assign:assignor-metadata assignor (consumer-topics c)))))
  (define res
    (sync
     (make-JoinGroup-evt
      conn
      (consumer-group-id c)
      (consumer-sesion-timeout-ms c)
      "" "consumer" protocols)))
  (set-consumer-generation-id! c (JoinGroupResponse-generation-id res))
  (set-consumer-member-id! c (JoinGroupResponse-member-id res))
  (define leader?
    (equal?
     (JoinGroupResponse-leader res)
     (JoinGroupResponse-member-id res)))
  (define assignments
    (cond
      [leader?
       (define assignor
         (findf
          (λ (a)
            (equal?
             (assign:assignor-name a)
             (JoinGroupResponse-protocol-name res)))
          assignors))
       (define member-metadata
         (parse-member-metadata (JoinGroupResponse-members res)))
       (define topic-partitions
         (get-topic-partitions conn member-metadata))
       (define member-assignments
         (assign:assignor-assign assignor topic-partitions member-metadata))
       (for/list ([(member-id topics) (in-hash member-assignments)])
         (make-Assignment
          #:member-id member-id
          #:data (with-output-bytes
                   (cproto:un-MemberAssignment
                    `((Version_1 . 0)
                      (ArrayLen_1 . ,(hash-count topics))
                      (Assignment_1 . ,(for/list ([(topic pids) (in-hash topics)])
                                         `((TopicName_1 . ,topic)
                                           (ArrayLen_1 . ,(length pids))
                                           (PartitionID_1 . ,pids))))
                      (Data_1 . #""))))))]
      [else null]))
  (define assignment-data
    (sync
     (make-SyncGroup-evt
      conn
      (consumer-group-id c)
      (consumer-generation-id c)
      (consumer-member-id c)
      assignments)))
  (define topics&partitions
    (call-with-input-bytes assignment-data
      (lambda (in)
        (for/hash ([a (in-list (ref 'Assignment_1 (cproto:MemberAssignment in)))])
          (define topic (ref 'TopicName_1 a))
          (define pids (ref 'PartitionID_1 a))
          (values topic pids)))))
  (define commmitted-topic-partitions
    (sync
     (handle-evt
      (make-FetchOffsets-evt conn (consumer-group-id c) topics&partitions)
      (lambda (res)
        (for/hash ([(topic partitions) (in-hash res)])
          (values topic (for/hash ([part (in-list partitions)])
                          (define pid (PartitionOffset/Group-id part))
                          (define err (PartitionOffset/Group-error-code part))
                          (unless (zero? err)
                            (raise-server-error err))
                          (values pid (PartitionOffset/Group-offset part)))))))))
  (define uncommitted-topic-partitions
    (for/fold ([topics (hash)])
              ([(topic partitions) (in-hash commmitted-topic-partitions)])
      (define uncommitted-partitions
        (for/hash ([(pid offset) (in-hash partitions)] #:when (= offset -1))
          (values pid (consumer-offset-reset-strategy c))))
      (cond
        [(hash-empty? uncommitted-partitions) topics]
        [else (hash-set topics topic uncommitted-partitions)])))
  (define reset-topic-partitions
    (if (hash-empty? uncommitted-topic-partitions)
        (hash)
        (sync
         (handle-evt
          (make-ListOffsets-evt conn uncommitted-topic-partitions)
          (lambda (res)
            (for/hash ([(topic partitions) (in-hash res)])
              (values topic (for/hash ([part (in-list partitions)])
                              (define pid (PartitionOffset-id part))
                              (define err (PartitionOffset-error-code part))
                              (unless (zero? err)
                                (raise-server-error err))
                              (values pid (PartitionOffset-offset part))))))))))
  (define topic-partitions
    (hash-union
     #:combine/key (λ (_topic committed-partitions reset-partitions)
                     (hash-union
                      #:combine/key (λ (_pid _committed-offset reset-offset) reset-offset)
                      committed-partitions
                      reset-partitions))
     commmitted-topic-partitions
     reset-topic-partitions))
  (set-consumer-topic-partitions! c topic-partitions))

(define (leave-group! c)
  (sync
   (make-LeaveGroup-evt
    (get-coordinator c)
    (consumer-group-id c)
    (consumer-member-id c)))
  (set-consumer-generation-id! c 0)
  (set-consumer-member-id! c #f)
  (set-consumer-topic-partitions! c #f))

(define (get-coordinator c)
  (define client (consumer-client c))
  (define coordinator-id
    (sync
     (handle-evt
      (make-FindCoordinator-evt
       (get-connection client)
       (consumer-group-id c))
      Coordinator-node-id)))
  (get-node-connection client coordinator-id))

(define (parse-member-metadata members)
  (for/list ([m (in-list members)])
    (call-with-input-bytes (Member-metadata m)
      (lambda (in)
        (define mid (Member-id m))
        (define data (cproto:MemberMetadata in))
        (assign:metadata
         mid
         (ref 'Version_1 data)
         (ref 'TopicName_1 data)
         (ref 'Data_1 data))))))

(define (get-topic-partitions conn member-metadata)
  (define topics
    (remove-duplicates
     (flatten (map assign:metadata-topics member-metadata))))
  (define topic-metadata
    (sync (make-Metadata-evt conn topics)))
  (sort
   (flatten
    (for/list ([t (in-list (Metadata-topics topic-metadata))])
      (define topic (TopicMetadata-name t))
      (define err-code (TopicMetadata-error-code t))
      (unless (zero? err-code)
        (raise-server-error err-code))
      (for/list ([p (in-list (TopicMetadata-partitions t))])
        (define pid (PartitionMetadata-id p))
        (define err-code (PartitionMetadata-error-code p))
        (unless (zero? err-code)
          (raise-server-error err-code))
        (assign:topic-partition topic pid))))
   (lambda (a b)
     (match-define (assign:topic-partition topic-a pid-a) a)
     (match-define (assign:topic-partition topic-b pid-b) b)
     (if (equal? topic-a topic-b)
         (< pid-a pid-b)
         (string<? topic-a topic-b)))))
