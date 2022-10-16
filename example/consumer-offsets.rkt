#lang racket/base

(require kafka
         kafka/internal)

(define events
  (make-internal-events (make-client)))
(let loop ()
  (for ([event (in-list (sync events))])
    (println event))
  (loop))
