#lang racket/base

(require kafka
         kafka/consumer)

(define c (make-consumer (make-client) "example-group" "example-topic"))
(for ([_ (in-range 2)])
  (with-handlers ([exn:break? void])
    (define-values (type data)
      (sync (consume-evt c)))
    (println (list type (vector-length data)))
    (for ([r (in-vector data)])
      (println (list (record-key r) (record-value r))))))
(consumer-stop c)
