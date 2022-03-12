#lang racket/base

(require kafka
         kafka/consumer
         kafka/producer
         racket/match
         rackunit
         rackunit/text-ui)

(define t "00-publish-consume")
(define k (make-client))

(run-tests
 (test-suite
  "publish-consume"
  #:before
  (lambda ()
    (create-topics k (make-CreateTopic
                      #:name t
                      #:partitions 2)))
  #:after
  (lambda ()
    (delete-topics k t)
    (disconnect-all k))

  (let ()
    (define p (make-producer k))
    (define evts
      (list
       (produce p t #"k-1" #"v-1" #:partition 0)
       (produce p t #"k-2" #"v-2" #:partition 1)))
    (producer-flush p)
    (for ([res (in-list (map sync evts))])
      (match-define (RecordResult _ (ProduceResponsePartition _ err _)) res)
      (check-true (zero? err)))
    (producer-stop p)

    (define c (make-consumer k "00-consume" t))
    (test-case "consume before end"
      (define-values (type data)
        (sync (consume-evt c)))
      (check-equal? type 'records)
      (check-equal? (vector-length data) 2)
      (check-equal? (record-key (vector-ref data 0)) #"k-1")
      (check-equal? (record-key (vector-ref data 1)) #"k-2")
      (check-equal? (record-value (vector-ref data 0)) #"v-1")
      (check-equal? (record-value (vector-ref data 1)) #"v-2")
      (consumer-commit c))

    (test-case "consume after end"
      (define-values (type data)
        (sync (consume-evt c 100)))
      (check-equal? type 'records)
      (check-equal? (vector-length data) 0)
      (consumer-commit c))
    (consumer-stop c))))
