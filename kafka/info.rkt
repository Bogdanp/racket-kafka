#lang info

(define collection "kafka")
(define deps '("base"
               "kafka-lib"))
(define build-deps '("racket-doc"
                     "sasl-doc"
                     "sasl-lib"
                     "scribble-lib"))
(define implies '("kafka-lib"))
