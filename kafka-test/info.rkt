#lang info

(define collection "test")
(define deps '("base"
               "binfmt"
               "kafka-lib"))
(define build-deps '("rackunit-lib"))
(define implies '("kafka-lib"))
