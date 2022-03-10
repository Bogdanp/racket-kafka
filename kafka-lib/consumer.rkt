#lang racket/base

(require racket/list
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

 consumer?
 make-consumer
 consume-evt
 consumer-commit
 consumer-stop)

(struct consumer
  (client
   group-id
   [generation-id #:mutable]
   [member-id #:mutable]
   [topics #:mutable]
   [topic-partitions #:mutable]
   heartbeat-ch
   [heartbeat-thd #:mutable]
   assignors
   sesion-timeout-ms))

(define (make-consumer client group-id
                       #:assignors [assignors (list assign:round-robin)]
                       #:session-timeout-ms [session-timeout-ms 30000]
                       . topics)
  (define the-consumer
    (consumer client group-id 0 #f topics #f (make-channel) #f assignors session-timeout-ms))
  (begin0 the-consumer
    (join-group! the-consumer)
    (start-heartbeat-thd! the-consumer)))

(define (consume-evt c [timeout 1000])
  (choice-evt
   (handle-evt
    (consumer-heartbeat-ch c)
    (lambda (e)
      (cond
        [(exn:fail:kafka:server? e)
         (case (exn:fail:kafka:server-code e)
           [(27) ;; rebalance in progress
            (join-group! c)
            (start-heartbeat-thd! c)
            (values 'rebalance (consumer-topic-partitions c))]
           [else (raise e)])]
        [else (raise e)])))
   (handle-evt
    (make-Fetch-evt
     (get-connection (consumer-client c))
     (for/hash ([(topic pids) (in-hash (consumer-topic-partitions c))])
       (define offsets
         (for/list ([(pid offset) (in-hash pids)])
           (make-TopicPartition
            #:id pid
            #:offset offset)))
       (println offsets)
       (values topic offsets))
     timeout)
    (lambda (res)
      (define-values (offsets num-records)
        (for*/fold ([offsets (hash)]
                    [num-records 0])
                   ([(topic partitions) (in-hash (FetchResponse-topics res))]
                    [p (in-list partitions)]
                    [b (in-list (FetchResponsePartition-batches p))])
          (define key (cons topic (FetchResponsePartition-id p)))
          (define size (batch-size b))
          (define last-record
            (and (not (zero? size))
                 (vector-ref (batch-records b) (sub1 size))))
          (values
           (if last-record
               (hash-set offsets key (add1 (+ (batch-base-offset b)
                                              (record-offset last-record))))
               offsets)
           (+ num-records size))))
      (define topic-partitions
        (for/hash ([(topic partitions) (in-hash (consumer-topic-partitions c))])
          (values topic (for/hash ([(pid offset) (in-hash partitions)])
                          (values pid (hash-ref offsets (cons topic pid) offset))))))
      (define records
        (for*/vector #:length num-records
                     ([partitions (in-hash-values (FetchResponse-topics res))]
                      [p (in-list partitions)]
                      [b (in-list (FetchResponsePartition-batches p))]
                      [r (in-vector (batch-records b))])
          r))
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
       (for* ([partitions (in-hash-values res)]
              [part (in-list partitions)])
         (define err (CommitPartitionResult-error-code part))
         (unless (zero? err)
           (raise-server-error err))))))))

(define (consumer-stop c)
  (when (consumer-member-id c)
    (stop-heartbeat-thd! c)
    (void (leave-group! c))))

(define (start-heartbeat-thd! c #:interval-ms [interval-ms 3000])
  (define thd
    (thread
     (lambda ()
       (define interval
         (/ interval-ms 1000.0))
       (define ch
         (consumer-heartbeat-ch c))
       (with-handlers ([exn:fail? (λ (e) (channel-put ch e))]
                       [exn:break? (λ (_) (log-kafka-debug "stopping heartbeat thread"))])
         (define group-id (consumer-group-id c))
         (define generation-id (consumer-generation-id c))
         (define member-id (consumer-member-id c))
         (define conn (get-coordinator c))
         (let loop ()
           (sleep interval)
           (log-kafka-debug
            "sending heartbeat~n  group-id: ~s~n  generation-id: ~s~n  member-id: ~s"
            group-id generation-id member-id)
           (sync (make-Heartbeat-evt conn group-id generation-id member-id))
           (loop))))))
  (set-consumer-heartbeat-thd! c thd))

(define (stop-heartbeat-thd! c)
  (break-thread (consumer-heartbeat-thd c)))

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
  (define topic-partitions
    (call-with-input-bytes assignment-data
      (lambda (in)
        (for/hash ([a (in-list (ref 'Assignment_1 (cproto:MemberAssignment in)))])
          (define topic (ref 'TopicName_1 a))
          (define pids (ref 'PartitionID_1 a))
          (values topic (for/hash ([pid (in-list pids)])
                          (values pid 0)))))))
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
