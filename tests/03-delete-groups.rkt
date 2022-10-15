#lang racket/base

(require kafka
         kafka/consumer
         kafka/producer
         rackunit
         rackunit/text-ui)

(define t "03-delete-groups")
(define k (make-client))

(run-tests
 (test-suite
  "delete-groups"
  #:before
  (lambda ()
    (create-topics k (make-CreateTopic
                      #:name t
                      #:partitions 2))

    (define p (make-producer k))
    (produce p t #"a" #"a" #:partition 0)
    (produce p t #"b" #"b" #:partition 1)
    (producer-flush p)
    (producer-stop p))
  #:after
  (lambda ()
    (delete-topics k t)
    (disconnect-all k))

  (let ()
    (define g "03-delete-groups-group")
    (define c (make-consumer k g t))
    (sync (consume-evt c))
    (consumer-stop c)
    (disconnect-all k)
    (delete-groups k g)
    (check-false (member g (map Group-id (list-groups k)))))))
