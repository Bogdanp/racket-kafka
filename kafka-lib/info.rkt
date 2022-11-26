#lang info

(define collection "kafka")
(define version "0.2")
(define deps '("base"
               ("binfmt" #:version "0.4")
               "lz4-lib"
               "sasl-lib"))
(define build-deps '("rackunit-lib"))
