#lang racket/base

(require kafka
         kafka/consumer)

(define c (make-consumer
           (make-client)
           "zstd-group"
           "zstd-data"))
(with-handlers ([exn:break? void]
                [exn:fail? (λ (e) ((error-display-handler) (exn-message e) e))])
  (let loop ()
    (define-values (type data)
      (sync (consume-evt c)))
    (case type
      [(rebalance)
       (println `(rebalance ,data))]
      [(records)
       (println `(records ,(vector-length data)))
       (for ([r (in-vector data)])
         (println (list
                   (record-partition-id r)
                   (record-offset r)
                   (record-key r)
                   (record-value r))))])
    (consumer-commit c)
    (loop)))
(consumer-stop c)
