#lang racket/base

(require "private/logger.rkt")

(provide
 kafka-logger
 fault?
 fault-original-error)
