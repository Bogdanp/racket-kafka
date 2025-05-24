#lang info

(define license 'BSD-3-Clause)
(define version "0.3")
(define collection "sasl")
(define deps '("base"
               "crypto-lib"
               ["sasl-lib" #:version "1.3"]
               "threading-lib"))
(define build-deps '("rackunit-lib"))
