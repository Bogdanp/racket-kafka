#lang info

(define license 'BSD-3-Clause)
(define version "0.10")
(define collection "kafka")
(define deps '("base"
               ["binfmt" #:version "0.4"]
               ["libzstd" #:version "1.5.5"]
               ["lz4-lib" #:version "1.4.1"]
               "net-lib"
               "sasl-lib"
               ["snappy-lib" #:version "1.0"]))
(define build-deps '("rackunit-lib"))
