#lang racket/base

(provide
 (all-defined-out))

(struct exn:fail:schema-registry exn:fail ())
(struct exn:fail:schema-registry:client exn:fail:schema-registry (code message))
(struct exn:fail:schema-registry:server exn:fail:schema-registry (code text))

(define (client-error code message)
  (exn:fail:schema-registry:client
   (format "client error~n code: ~a~n message: ~a" code message)
   (current-continuation-marks)
   code message))

(define (server-error code [text #f])
  (exn:fail:schema-registry:server "server error" (current-continuation-marks) code text))
