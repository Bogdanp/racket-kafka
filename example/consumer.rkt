#lang racket/base

(require kafka
         kafka/consumer)

(define c (make-consumer (make-client) "example-group-1" "example-topic"))
(for ([_ (in-range 2)])
  (with-handlers ([exn:break? void])
    (define-values (type data)
      (sync (consume-evt c)))
    (println (list type (vector-length data)))
    (for ([r (in-vector data)])
      (println (list
                (record-offset r)
                (record-key r)
                (record-value r))))
    (consumer-commit c)))
(consumer-stop c)
