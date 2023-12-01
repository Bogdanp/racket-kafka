#lang racket/base

(require (for-syntax racket/base))

(provide
 kafka-logger
 log-kafka-debug
 log-kafka-info
 log-kafka-warning
 log-kafka-error
 log-kafka-fatal
 log-kafka-fault
 (struct-out fault))

(define-logger kafka)

(struct fault (original-error))

(define (make-fault err)
  (fault err))

(define-syntax (log-kafka-fault stx)
  (syntax-case stx ()
    [(_ err format-str format-arg ...)
     #'(let ([l kafka-logger])
         (when (log-level? l 'warning 'kafka)
           (log-message
            l 'warning
            (format format-str format-arg ...)
            (make-fault err))))]))
