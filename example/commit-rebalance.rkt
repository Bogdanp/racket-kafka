#lang racket/base

;; This example tests that committing after a rebalance doesn't raise
;; any errors.

(require kafka
         kafka/consumer)

(define c
  (make-consumer
   (make-client)
   "rebalance-group"
   "example-topic"))
(sync (consume-evt c))
(thread-wait
 (thread
  (lambda ()
    (define tc
      (make-consumer
       (make-client)
       "rebalance-group"
       "example-topic"))
    (consumer-stop tc))))
(with-handlers ([exn:fail? (lambda (e)
                             ((error-display-handler) (exn-message e) e))])
  (consumer-commit c))
(consumer-stop c)
