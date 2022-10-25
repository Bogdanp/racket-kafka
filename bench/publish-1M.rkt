#lang racket/base

(require kafka
         kafka/producer
         profile)

(define (bench)
  (define N 1000000)
  (define t "bench-publish-1M")
  (define k (make-client))
  (define p (make-producer k
                           #:compression 'none
                           #:flush-interval 10000
                           #:max-batch-size 100000))
  (create-topics k (make-CreateTopic #:name t #:partitions 8))
  (time
   (for ([n (in-range N)])
     (produce p t #"k" #"v" #:partition (modulo n 8)))
   (producer-stop p))
  (delete-topics k t)
  (disconnect-all k))

(define (bench-with-res)
  (define N 100000)
  (define t "bench-publish-1M")
  (define k (make-client))
  (define p (make-producer k
                           #:compression 'none
                           #:flush-interval 1000
                           #:max-batch-size 10000))
  (create-topics k (make-CreateTopic #:name t #:partitions 8))
  (time
   (for/fold ([evts null])
             ([n (in-range N)])
     (define evt (produce p t #"k" #"v" #:partition (modulo n 8)))
     (define evts* (cons evt evts))
     (cond
       [(= (length evts*) 1000)
        (producer-flush p)
        (begin0 null
          (for-each sync evts*))]
       [else
        evts*]))
   (producer-stop p))
  (delete-topics k t)
  (disconnect-all k))

(bench)
#;
(profile-thunk
 #:use-errortrace? #t
 #:threads #t
 #:delay 0.05
 bench)

(bench-with-res)
#;
(profile-thunk
 #:use-errortrace? #t
 #:threads #t
 #:delay 0.05
 bench-with-res)
