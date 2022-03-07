#lang racket/base

(require racket/list
         racket/match
         racket/port
         (prefix-in assign: "private/assignor.rkt")
         "private/client.rkt"
         "private/error.rkt"
         "private/help.rkt"
         (prefix-in cproto: "private/protocol-consumer.bnf")
         "private/serde.rkt")

(provide
 consumer?
 make-consumer
 consumer-stop)

(struct consumer
  (client
   group-id
   [generation-id #:mutable]
   [member-id #:mutable]
   [topics #:mutable]
   assignors
   sesion-timeout-ms))

(define (make-consumer client group-id
                       #:assignors [assignors (list assign:round-robin)]
                       #:session-timeout-ms [session-timeout-ms 30000]
                       . topics)
  (define c (consumer client group-id 0 #f topics assignors session-timeout-ms))
  (begin0 c
    (println (coordinate-join c))))

(define (consume-evt c)
  (void))

(define (consumer-stop c)
  (void
   (sync
    (make-LeaveGroup-evt
     (get-connection (consumer-client c))
     (consumer-group-id c)
     (consumer-member-id c)))))

(define (coordinate-join c)
  (define coordinator-id
    (sync
     (handle-evt
      (make-FindCoordinator-evt
       (get-connection (consumer-client c))
       (consumer-group-id c))
      Coordinator-node-id)))
  (define conn
    (get-node-connection (consumer-client c) coordinator-id))
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
  (call-with-input-bytes assignment-data
    (lambda (in)
      (cproto:MemberAssignment in))))

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
