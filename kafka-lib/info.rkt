#lang info

(define license 'BSD-3-Clause)
(define version "0.12.1")
(define collection "kafka")
(define deps '("base"
               ["binfmt-lib" #:version "0.5"]
               ["libzstd" #:version "1.5.5"]
               ["lz4-lib" #:version "1.4.1"]
               "net-lib"
               "sasl-lib"
               ["snappy-lib" #:version "1.0"]))
(define build-deps '("rackunit-lib"))
