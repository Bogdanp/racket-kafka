#lang info

(define license 'BSD-3-Clause)
(define version "0.4")
(define collection "kafka")
(define deps '("base"
               ["binfmt" #:version "0.4"]
               ["lz4-lib" #:version "1.3"]
               "sasl-lib"))
(define build-deps '("rackunit-lib"))
