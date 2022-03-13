#lang racket/base

(require kafka)

(define deadline (+ (current-seconds) 60))
(let loop ()
  (cond
    [(> (current-seconds) deadline)
     (error 'wait-for-kafka "kafka not available")]
    [else
     (with-handlers ([exn:fail?
                      (lambda (e)
                        ((error-display-handler) (exn-message e) e)
                        (sleep 1)
                        (loop))])
       (define k (make-client))
       (void (find-group-coordinator k "example")))]))
