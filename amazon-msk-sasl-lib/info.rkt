#lang info

(define version "0.1")
(define collection "sasl")
(define deps '("base"
               "crypto-lib"
               ["sasl-lib" #:version "1.3"]
               "threading-lib"))
(define build-deps '("rackunit-lib"))
