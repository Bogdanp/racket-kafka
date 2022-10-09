#lang info

(define collection "kafka")
(define version "0.2")
(define deps '("base"
               ("binfmt" #:version "0.3")
               "sasl-lib"))
(define build-deps '("rackunit-lib"))
