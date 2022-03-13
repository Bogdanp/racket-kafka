#lang racket/base

(require kafka
         kafka/consumer
         kafka/producer
         racket/format
         rackunit
         rackunit/text-ui)

(define N 10000)
(define P 8)
(define t "99-publish-consume-for-a-while")

(run-tests
 (test-suite
  "publish-consume-for-a-while"
  #:before
  (lambda ()
    (define k (make-client))
    (create-topics k (make-CreateTopic
                      #:name t
                      #:partitions P))
    (disconnect-all k))

  #:after
  (lambda ()
    (define k (make-client))
    (delete-topics k t)
    (disconnect-all k))

  (let ()
    (define msgs null)
    (define msg-ch (make-channel))

    (define g "publish-consume-for-a-while-group")
    (define consumer-ch (make-channel))
    (define consumer-thds
      (for/list ([i (in-range P)])
        (thread
         (lambda ()
           (let loop ()
             (with-handlers ([exn:fail:kafka:server?
                              (lambda (e)
                                (define code (exn:fail:kafka:server-code e))
                                (case (error-code-symbol code)
                                  [(rebalance-in-progress)
                                   (loop)]
                                  [else
                                   (raise e)]))])
               (define k (make-client #:id (~a "consumer-" i)))
               (define c (make-consumer k g t))
               (let loop ()
                 (sync
                  (handle-evt
                   consumer-ch
                   (lambda (_)
                     (consumer-stop c)
                     (disconnect-all k)))
                  (handle-evt
                   (consume-evt c)
                   (lambda (type data)
                     (case type
                       [(records)
                        (for ([r (in-vector data)])
                          (channel-put msg-ch (record-value r)))
                        (consumer-commit c)
                        (loop)]

                       [else
                        (loop)])))))))))))

    (define producer-ch (make-channel))
    (define producer-thd
      (thread
       (lambda ()
         (define k (make-client #:id "producer"))
         (define p (make-producer k #:flush-interval 5))
         (let loop ([n 0] [evts null])
           (cond
             [(= (length evts) 500)
              (time (for-each sync evts))
              (loop n null)]
             [(< n N)
              (define produce-evt
                (produce p t #"k" #"v" #:partition (modulo n 8)))
              (loop (add1 n) (cons produce-evt evts))]
             [else
              (time (for-each sync evts))
              (sync producer-ch)
              (disconnect-all k)])))))

    (let loop ()
      (define msg (channel-get msg-ch))
      (set! msgs (cons msg msgs))
      (when (< (length msgs) N)
        (loop)))

    (channel-put producer-ch '(stop))
    (thread-wait producer-thd)
    (for ([_ (in-range P)])
      (channel-put consumer-ch '(stop)))
    (for-each thread-wait consumer-thds))))
