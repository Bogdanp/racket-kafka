#lang racket/base

(require (for-syntax racket/base
                     racket/syntax
                     syntax/parse/pre))

(provide
 define-record)

(define-syntax (define-record stx)
  (define-syntax-class field
    (pattern [id:id {~alt
                     {~optional {~seq #:key key:expr}}
                     {~optional {~seq #:to-key to-key:expr}}
                     {~optional {~seq #:default default:expr}}
                     {~optional {~seq #:decode decode-proc:expr}}
                     {~optional {~seq #:encode encode-proc:expr}}} ...]
             #:with kwd (string->keyword (symbol->string (syntax-e #'id)))
             #:with (ctor-arg ...) #'{~? (kwd [id {~? (decode-proc default) default}])
                                         (kwd id)}))

  (syntax-parse stx
    [(_ id:id (fld:field ...))
     #:with make-id (format-id #'id "make-~a" #'id)
     #:with id? (format-id #'id "~a?" #'id)
     #:with jsexpr->id-id (format-id #'id "jsexpr->~a" #'id)
     #:with id->jsexpr-id (format-id #'id "~a->jsexpr" #'id)
     #:with (fld-accessor ...) (for/list ([fld-id-stx (in-list (syntax-e #'(fld.id ...)))])
                                 (format-id #'id "~a-~a" #'id fld-id-stx))
     #'(begin
         (provide id? make-id fld-accessor ...)
         (struct id (fld.id ...)
           #:transparent)
         (define (make-id fld.ctor-arg ... ...)
           (id fld.id ...))
         (define (jsexpr->id-id e)
           (id
            ({~? fld.decode-proc values}
             {~? (hash-ref e {~? fld.key 'fld.id} fld.default)
                 (hash-ref e {~? fld.key 'fld.id})}) ...))
         (define (id->jsexpr-id v)
           (define ks (list {~? {~? fld.to-key fld.key} 'fld.id} ...))
           (define vs (list ({~? fld.encode-proc values} (fld-accessor v)) ...))
           (for/hasheq ([k (in-list ks)]
                        [v (in-list vs)])
             (values k v))))]))
