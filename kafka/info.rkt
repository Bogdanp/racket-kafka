#lang info

(define collection "kafka")
(define deps '(("base" #:version "8.4")
               "kafka-lib"))
(define build-deps '("racket-doc"
                     "sasl-doc"
                     "sasl-lib"
                     "scribble-lib"))
(define implies '("kafka-lib"))
