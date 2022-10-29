#lang racket/base

(require kafka
         kafka/internal
         racket/vector)

(define c (make-client))
(define it (make-internal-events c))
(define (make-deadline-evt)
  (alarm-evt (+ (current-inexact-monotonic-milliseconds) 1000) #t))
(with-handlers ([exn:break? void])
  (let loop ([deadline-evt (make-deadline-evt)])
    (define events
      (get-events it))
    (cond
      [(vector-empty? events)
       (sync deadline-evt)
       (loop (make-deadline-evt))]
      [else
       (for ([e (in-vector events)])
         (println e))
       (loop deadline-evt)])))
(disconnect-all c)
