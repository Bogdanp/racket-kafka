#lang racket/base

(require kafka
         kafka/producer
         racket/random)

(define conn (connect))
(define p (make-producer conn))
(create-topics
 conn
 (make-CreateTopic
  #:name "foo"
  #:partitions 8)
 (make-CreateTopic
  #:name "bar"
  #:partitions 8))
(define evts
  (for/list ([i (in-range 8)])
    (define pid (modulo i 8))
    (define topic (random-ref '("foo" "bar")))
    (produce p topic #"a" #"abcde" #:partition pid)))

(producer-flush p)
(for ([evt (in-list evts)])
  (println (sync evt)))
