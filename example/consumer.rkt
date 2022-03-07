#lang racket/base

(require kafka
         kafka/consumer)

(define c (make-consumer (make-client) "test" "foo"))
(consumer-stop c)
