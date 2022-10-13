#lang racket/base

(require openssl
         racket/contract
         racket/promise
         racket/string
         sasl
         "private/client.rkt"
         "private/error.rkt"
         "private/serde.rkt")

(provide
 (all-from-out "private/serde.rkt")
 (contract-out
  [exn:fail:kafka? (-> any/c boolean?)]
  [exn:fail:kafka:client? (-> any/c boolean?)]
  [exn:fail:kafka:server? (-> any/c boolean?)]
  [exn:fail:kafka:server-code (-> exn:fail:kafka:server? exact-integer?)]
  [error-code-symbol (-> exact-integer? symbol?)]

  [client? (-> any/c boolean?)]
  [make-client (->* ()
                    (#:id non-empty-string?
                     #:bootstrap-host string?
                     #:bootstrap-port (integer-in 0 65535)
                     #:sasl-mechanism&ctx (or/c
                                           #f
                                           (list/c 'plain string?)
                                           (list/c symbol? sasl-ctx?))
                     #:ssl-ctx (or/c #f ssl-client-context?))
                    client?)]
  [client-metadata (-> client? Metadata?)]
  [disconnect-all (-> client? void?)]
  [get-metadata (-> client? string? ... Metadata?)]
  [describe-cluster (-> client? Cluster?)]
  [describe-configs (-> client? DescribeResource? DescribeResource? ... DescribedResources?)]
  [describe-producers (-> client? (hash/c string? (non-empty-listof exact-nonnegative-integer?)) DescribedProducers?)]
  [create-topics (-> client? CreateTopic? CreateTopic? ... CreatedTopics?)]
  [delete-topics (-> client? string? string? ... DeletedTopics?)]
  [find-group-coordinator (-> client? string? Coordinator?)]
  [describe-groups (-> client? string? ... (listof Group?))]
  [delete-groups (-> client? string? ... (listof DeletedGroup?))]
  [list-groups (-> client? (listof Group?))]
  [list-offsets (-> client?
                    (hash/c topic&partition/c (or/c 'earliest 'latest exact-nonnegative-integer?))
                    (hash/c topic&partition/c PartitionOffset?))]))


;; admin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define topic&partition/c
  (cons/c string? exact-nonnegative-integer?))

(define (get-metadata c . topics)
  (sync (make-Metadata-evt (get-controller-connection c) topics)))

(define (describe-cluster c)
  (sync (make-DescribeCluster-evt (get-controller-connection c))))

(define (describe-configs c . resources)
  (sync (make-DescribeConfigs-evt (get-controller-connection c) resources)))

(define (describe-producers c topics)
  (when (zero? (hash-count topics))
    (raise-argument-error 'describe-producers "(non-empty-hash/c string? (listof integer?))" topics))
  (sync (make-DescribeProducers-evt (get-controller-connection c) topics)))

(define (create-topics c topic0 . topics)
  (sync (make-CreateTopics-evt (get-controller-connection c) (cons topic0 topics))))

(define (delete-topics c topic0 . topics)
  (sync (make-DeleteTopics-evt (get-controller-connection c) (cons topic0 topics))))

(define (find-group-coordinator c group-id)
  (sync (make-FindCoordinator-evt (get-controller-connection c) group-id)))

(define (describe-groups c . groups)
  (define groupss
    (for/list ([(node-id group-ids) (in-hash (get-groups-by-coordinator c groups))])
      (delay/thread
       (define res
         (sync (make-DescribeGroups-evt (get-node-connection c node-id) group-ids)))
       (DescribedGroups-groups res))))
  (apply append (map force groupss)))

(define (delete-groups c . groups)
  (define deleted-groupss
    (for/list ([(node-id group-ids) (in-hash (get-groups-by-coordinator groups))])
      (delay/thread
       (define res
         (sync (make-DeleteGroups-evt (get-node-connection c node-id) group-ids)))
       (DeletedGroups-groups res))))
  (apply append (map force deleted-groupss)))

(define (list-groups c)
  (define groupss
    (for/list ([b (in-list (Metadata-brokers (client-metadata c)))])
      (delay/thread
       (define node-id (BrokerMetadata-node-id b))
       (sync (make-ListGroups-evt (get-node-connection c node-id))))))
  (apply append (map force groupss)))

(define (list-offsets c topic&partitions)
  (define offsetss
    (for/list ([(node-id t&ps) (in-hash (get-topic-partitions-by-leader c topic&partitions))])
      (define topics
        (for/fold ([topics (hash)])
                  ([t&p (in-list t&ps)])
          (define topic (car t&p))
          (define part (cdr t&p))
          (define offset (hash-ref topic&partitions t&p))
          (hash-update
           topics
           topic
           (λ (parts) (hash-set parts part offset))
           hasheqv)))
      (delay/thread
       (sync (make-ListOffsets-evt (get-node-connection c node-id) topics)))))
  (for*/hash ([offsets (in-list (map force offsetss))]
              [(topic parts) (in-hash offsets)]
              [part (in-list parts)])
    (values (cons topic (PartitionOffset-id part)) part)))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (get-groups-by-coordinator c groups)
  (for/fold ([nodes-to-groups (hasheqv)])
            ([group-id (in-list groups)])
    (define coord
      (find-group-coordinator c group-id))
    (hash-update
     nodes-to-groups
     (Coordinator-node-id coord)
     (λ (gs) (cons group-id gs))
     null)))

(define (get-topic-partitions-by-leader c topic&partitions)
  (define topic&partitions-to-node-ids
    (for*/hash ([topic (in-list (Metadata-topics (client-metadata c)))]
                [part (in-list (TopicMetadata-partitions topic))])
      (define topic&partition
        (cons (TopicMetadata-name topic)
              (PartitionMetadata-id part)))
      (values topic&partition (PartitionMetadata-leader-id part))))
  (for/fold ([nodes-to-topic&partition (hasheqv)])
            ([(topic&partition node-id) (in-hash topic&partitions-to-node-ids)]
             #:when (hash-has-key? topic&partitions topic&partition))
    (hash-update
     nodes-to-topic&partition
     node-id
     (λ (t&ps) (cons topic&partition t&ps))
     null)))
