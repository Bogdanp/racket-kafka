#lang info

(define collection "kafka")
(define version "0.1")
(define deps '("base"
               ("binfmt" #:version "0.2")
               "box-extra-lib"
               "sasl-lib"))
(define build-deps '("rackunit-lib"))
