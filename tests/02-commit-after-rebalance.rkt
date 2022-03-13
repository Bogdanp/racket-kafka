#lang racket/base

(require kafka
         kafka/consumer
         kafka/producer
         rackunit
         rackunit/text-ui)

(define t "01-commit-after-rebalance")
(define k (make-client))

(run-tests
 (test-suite
  "commit-after-rebalance"
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
    (define g "commit-after-rebalance-group")
    (define k0 (make-client))
    (define c0 (make-consumer k0 g t))
    (sync (consume-evt c0))
    (define k1 (make-client))
    (define c1 (make-consumer k1 g t))
    (consumer-stop c1)
    (disconnect-all k1)
    (consumer-commit c0)
    (let ()
      (define-values (type _data)
        (sync (consume-evt c0)))
      (check-equal? type 'rebalance))
    (let ()
      (define-values (type data)
        (sync (consume-evt c0)))
      (check-equal? type 'records)
      (check-equal? (vector-length data) 2))
    (consumer-stop c0)
    (disconnect-all k0))))
