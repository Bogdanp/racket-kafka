#lang racket/base

(require "serde/metadata.rkt")

(provide
 collect-nodes-by-topic&pid)

(define (collect-nodes-by-topic&pid metadata topics)
  (for*/hash ([t (in-list (Metadata-topics metadata))]
              [topic (in-value (TopicMetadata-name t))]
              #:when (member topic topics string=?)
              [p (in-list (TopicMetadata-partitions t))]
              [pid (in-value (PartitionMetadata-id p))])
    (values (cons topic pid) (PartitionMetadata-leader-id p))))
