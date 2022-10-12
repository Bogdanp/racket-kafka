#lang racket/base

(require racket/contract
         "core.rkt")

(define-record CreatedTopic
  ([error-code error-code/c]
   [error-message (or/c #f string?)]
   [name string?]))

(define-record CreatedTopics
  ([topics (listof CreatedTopic?)]))

(define-record CreateTopic
  ([name string?]
   [partitions exact-positive-integer?]
   [(replication-factor 1) exact-positive-integer?]
   [(assignments (hasheqv)) (hash/c exact-nonnegative-integer? (listof exact-nonnegative-integer?))]
   [(configs (hash)) (hash/c string? string?)]))

(define-request CreateTopics
  (topics
   [timeout-ms 30000]
   [validate-only? #f])
  #:code 19
  #:version 1
  #:response proto:CreateTopicsResponseV1
  (lambda (topics timeout-ms validate-only?)
    (define topic-requests
      (for/list ([t (in-list topics)])
        (define assignments (CreateTopic-assignments t))
        (define configs (CreateTopic-configs t))
        `((TopicName_1         . ,(CreateTopic-name t))
          (NumPartitions_1     . ,(CreateTopic-partitions t))
          (ReplicationFactor_1 . ,(CreateTopic-replication-factor t))
          (Assignments_1
           . ((ArrayLen_1   . ,(hash-count assignments))
              (Assignment_1 . ,(for/list ([(pid bids) (in-hash assignments)])
                                 `((PartitionID_1 . ,pid)
                                   (BrokerIDs_1 . ((ArrayLen_1 . ,(length bids))
                                                   (BrokerID_1 . ,bids))))))))
          (Configs_1
           . ((ArrayLen_1 . ,(hash-count configs))
              (Config_1   . ,(for/list ([(name value) (in-hash configs)])
                               `((ConfigName_1  . ,name)
                                 (ConfigValue_1 . ,value)))))))))

    (with-output-bytes
      (proto:un-CreateTopicsRequestV1
       `((CreateTopicsRequestsV1_1 . ((ArrayLen_1             . ,(length topics))
                                      (CreateTopicRequestV1_1 . ,topic-requests)))
         (TimeoutMs_1 . ,timeout-ms)
         (ValidateOnly_1 . ,validate-only?)))))
  (lambda (res)
    (CreatedTopics
     (for/list ([t (in-list (ref 'CreateTopicsResponseTopicV1_1 res))])
       (make-CreatedTopic
        #:name (ref 'TopicName_1 t)
        #:error-code (ref 'ErrorCode_1 t)
        #:error-message (ref 'ErrorMessage_1 t))))))
