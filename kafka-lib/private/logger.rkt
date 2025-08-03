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

(define-syntax (log-kafka-fault stx)
  (syntax-case stx ()
    [(_ err format-str format-arg ...)
     #'(let ([l kafka-logger])
         (when (log-level? l 'debug 'kafka)
           (log-message
            l 'debug
            (format format-str format-arg ...)
            (fault err))))]))
