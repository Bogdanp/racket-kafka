#lang racket/base

(require (for-syntax racket/base
                     racket/list
                     racket/syntax
                     syntax/parse)
         racket/contract
         racket/pretty
         "../connection.rkt"
         "../error.rkt"
         "../help.rkt"
         (prefix-in proto: "../protocol.bnf")
         "contract.rkt")

(provide
 (all-from-out
  "../connection.rkt"
  "../help.rkt"
  "../protocol.bnf"
  "contract.rkt")
 define-record
 define-request)

(begin-for-syntax
  (define (id-stx->keyword stx)
    (datum->syntax stx (string->keyword (symbol->string (syntax-e stx)))))

  (define-syntax-class record-fld
    (pattern [id:id ctc:expr]
             #:with required? #'#t
             #:with kwd (id-stx->keyword #'id)
             #:with arg #'id)
    (pattern [(id:id def:expr) ctc:expr]
             #:with required? #'#f
             #:with kwd (id-stx->keyword #'id)
             #:with arg #'[id def])))

(define-syntax (define-record stx)
  (syntax-parse stx
    [(_ id:id (fld:record-fld ...))
     #:with id? (format-id #'id "~a?" #'id)
     #:with constructor-id (format-id #'id "make-~a" #'id)
     #:with (constructor-arg ...) (flatten
                                   (for/list ([kwd (in-list (syntax-e #'(fld.kwd ...)))]
                                              [arg (in-list (syntax-e #'(fld.arg ...)))])
                                     (list kwd arg)))
     #:with (required-ctor-arg-ctc ...) (flatten
                                         (for/list ([req? (in-list (syntax->datum #'(fld.required? ...)))]
                                                    [kwd  (in-list (syntax-e #'(fld.kwd ...)))]
                                                    [ctc  (in-list (syntax-e #'(fld.ctc ...)))]
                                                    #:when req?)
                                           (list kwd ctc)))
     #:with (optional-ctor-arg-ctc ...) (flatten
                                         (for/list ([req? (in-list (syntax->datum #'(fld.required? ...)))]
                                                    [kwd  (in-list (syntax-e #'(fld.kwd ...)))]
                                                    [ctc  (in-list (syntax-e #'(fld.ctc ...)))]
                                                    #:unless req?)
                                           (list kwd ctc)))
     #:with (fld-accessor-id ...) (for/list ([fld-id (in-list (syntax-e #'(fld.id ...)))])
                                    (format-id fld-id "~a-~a" #'id fld-id))
     #'(begin
         (provide
          (contract-out
           [struct id ([fld.id fld.ctc] ...)]
           [constructor-id (->* (required-ctor-arg-ctc ...)
                                (optional-ctor-arg-ctc ...)
                                id?)]))
         (struct id (fld.id ...)
           #:transparent
           #:methods gen:custom-write
           [(define write-proc
              (make-record-constructor-printer
               (λ (_) 'constructor-id)
               (λ (_) (list 'fld.kwd ...))
               (λ (r) (list (fld-accessor-id r) ...))))])
         (define (constructor-id constructor-arg ...)
           (id fld.id ...)))]))

(define ((make-record-constructor-printer get-id get-kwds get-flds) r out mode)
  (define (do-print port [col #f])
    (define indent (if col (make-string (add1 col) #\space) " "))
    (define recur
      (case mode
        [(#t) write]
        [(#f) display]
        [(0 1) (λ (d p) (print d p mode))]))
    (define write? (not (integer? mode)))
    (write-string (if write? "#<" "(") port)
    (write (get-id r) port)
    (for ([kwd (in-list (get-kwds r))]
          [fld (in-list (get-flds r))])
      (when col
        (pretty-print-newline port (pretty-print-columns)))
      (write-string indent port)
      (fprintf port "~a " kwd)
      (recur fld port))
    (write-string (if write? ">" ")") port))
  (if (and (pretty-printing)
           (integer? (pretty-print-columns)))
      ((let/ec esc
         (define pretty-port
           (make-tentative-pretty-print-output-port
            out
            (- (pretty-print-columns) 1)
            (lambda ()
              (esc
               (lambda ()
                 (tentative-pretty-print-port-cancel pretty-port)
                 (define-values (_line col _pos)
                   (port-next-location out))
                 (do-print out col))))))
         (do-print pretty-port)
         (tentative-pretty-print-port-transfer pretty-port out)
         void))
      (do-print out))
  (void))

(begin-for-syntax
  (define-syntax-class request-arg
    (pattern id:id)
    (pattern [id:id def:expr])))

(define-syntax (define-request stx)
  (syntax-parse stx
    [(_ id:id
        (arg:request-arg ...)
        {~seq #:code key:number}
        {~seq #:version version-num:number
              {~optional {~seq #:flexible flexible}}
              #:response parser:expr
              {~optional {~seq #:immed-response immed-response-expr}}
              encoder:expr
              decoder:expr} ...+)
     #:with make-evt-id (format-id #'id "make-~a-evt" #'id)
     #:with version-rng-id (format-id #'id "supported-~a-versions" #'id)
     #:with version-rng (let ([versions (map syntax->datum (syntax-e #'(version-num ...)))])
                          (with-syntax ([min-v (apply min versions)]
                                        [max-v (apply max versions)])
                            #'(version-range min-v max-v)))
     #'(begin
         (provide
          make-evt-id
          version-rng-id)
         (define version-rng-id version-rng)
         (define (make-evt-id conn arg ...)
           (case (find-best-version conn key version-rng-id)
             [(version-num)
              (request-evt
               conn decoder
               #:key key
               #:version version-num
               #:data (encoder arg.id ...)
               #:parser parser
               #:flexible? {~? flexible #f}
               #:immed-response {~? (immed-response-expr arg.id ...) #f})] ...
             [else
              (error 'make-evt-id "no supported version")])))]))

(define (request-evt conn proc
                     #:key key
                     #:version ver
                     #:data data
                     #:parser parser-proc
                     #:flexible? flexible?
                     #:immed-response immed-response)
  (handle-evt
   (make-request-evt
    conn
    #:key key
    #:version ver
    #:data data
    #:parser parser-proc
    #:flexible? flexible?
    #:immed-response immed-response)
   (lambda (res)
     (cond
       [immed-response res]
       [else
        (define err-code (or (opt 'ErrorCode_1 res) 0))
        (unless (zero? err-code)
          (raise-server-error
           err-code
           "~a~n  key: ~s~n  version: ~s"
           (error-code-message err-code) key ver))
        (proc res)]))))
