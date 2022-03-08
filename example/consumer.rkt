#lang racket/base

(require kafka
         kafka/consumer)

(define c (make-consumer (make-client) "test" "foo"))
(with-handlers ([exn:break? void])
  (sync (consume-evt c)))
(consumer-stop c)
