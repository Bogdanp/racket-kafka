#lang racket/base

(require kafka
         kafka/internal)

(define c (make-client))
(define it (make-internal-events c))
(with-handlers ([exn:break? void])
  (let loop ()
    (for ([event (in-vector (sync it))])
      (println event))
    (loop)))
(disconnect-all c)
