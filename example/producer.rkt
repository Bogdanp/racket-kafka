#lang racket/base

(require kafka
         kafka/producer
         racket/format)

(define c (make-client))
(define p (make-producer c))
(create-topics
 c
 (make-CreateTopic
  #:name "example-topic"
  #:partitions 2))
(define evts
  (for/list ([i (in-range 128)])
    (define pid (modulo i 2))
    (produce p "example-topic" #"a" (string->bytes/utf-8 (~a i)) #:partition pid)))

(producer-flush p)
(for ([evt (in-list evts)])
  (println (sync evt)))
(disconnect-all c)
