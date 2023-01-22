#lang racket/base

(require kafka
         kafka/producer
         racket/format)

(define c (make-client))
(define p (make-producer c))
(define t "example-topic")
(create-topics
 c
 (make-CreateTopic
  #:name t
  #:partitions 2))
(define evts
  (for/list ([i (in-range 128)])
    (define pid (modulo i 2))
    (define k #"a")
    (define v (string->bytes/utf-8 (~a i)))
    (produce
     #:partition pid
     #:headers (hash "Content-Type" #"text/plain")
     p t k v)))

(producer-flush p)
(for ([evt (in-list evts)])
  (println (sync evt)))
(disconnect-all c)
