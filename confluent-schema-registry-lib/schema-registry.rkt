#lang racket/base

(require (only-in net/http-easy auth-procedure/c)
         racket/contract
         "schema-registry/api.rkt"
         "schema-registry/client.rkt"
         "schema-registry/error.rkt")

(provide
 client?
 exn:fail:schema-registry?
 exn:fail:schema-registry:client?
 exn:fail:schema-registry:server?
 schema-type/c

 (contract-out
  [make-client (->* (string?) (auth-procedure/c) client?)]

  ;; schema
  [Reference? (-> any/c boolean?)]
  [make-Reference (->* (#:name string?
                        #:subject string?
                        #:version exact-integer?)
                       ()
                       Reference?)]
  [Reference-name (-> Reference? string?)]
  [Reference-subject (-> Reference? string?)]
  [Reference-version (-> Reference? exact-integer?)]
  [Schema? (-> any/c boolean?)]
  [make-Schema (->* (#:id exact-integer?
                     #:subject string?
                     #:version exact-integer?
                     #:schema string?
                     #:references (listof Reference?))
                    (#:type schema-type/c)
                    Schema?)]
  [Schema-id (-> Schema? exact-integer?)]
  [Schema-subject (-> Schema? string?)]
  [Schema-version (-> Schema? exact-integer?)]
  [Schema-type (-> Schema? schema-type/c)]
  [Schema-references (-> Schema? (listof Reference?))]

  [get-schema (-> client? exact-integer? hash?)]
  [get-schema-types (-> client? (listof symbol?))]
  [get-schema-versions (-> client? exact-integer? (listof (cons/c string? exact-integer?)))]

  ;; subject
  [get-all-subjects (->* (client?)
                         (#:prefix string?
                          #:include-deleted? boolean?)
                         (listof string?))]
  [get-subject-versions (-> client? string? (listof exact-integer?))]
  [get-subject-version (-> client? string? (or/c 'latest exact-integer?) hash?)]
  [get-subject-version-referenced-by (-> client? string? exact-integer? (listof exact-integer?))]
  [delete-subject (->* (client? string?)
                       (#:permanently? boolean?)
                       (listof exact-integer?))]
  [delete-subject-version (->* (client? string? exact-integer?)
                               (#:permanently? boolean?)
                               exact-integer?)]
  [register-schema (-> client? string? Schema? exact-integer?)]
  [check-schema (-> client? string? Schema? Schema?)]

  ;; mode
  [get-mode (->* (client?)
                 (#:subject string?)
                 mode/c)]
  [set-mode (->* (client? mode/c)
                 (#:subject string?
                  #:force? boolean?)
                 mode/c)]
  [delete-mode (-> client? string? mode/c)]

  ;; config
  [Config? (-> any/c boolean?)]
  [make-Config (->* ()
                    (#:compatibility-level compatibility-level/c)
                    Config?)]
  [Config-compatibility-level (-> Config? compatibility-level/c)]
  [get-config (->* (client?)
                   (#:subject string?)
                   Config?)]
  [set-config (->* (client? Config?)
                   (#:subject string?)
                   Config?)]
  [delete-config (-> client? string? Config?)]))

(define schema-type/c
  (or/c 'avro 'protobuf 'json))

(define mode/c
  (or/c 'import 'readonly 'readwrite))

(define compatibility-level/c
  (or/c 'backward 'backward-transitive 'forward 'forward-transitive 'full 'full-transitive 'none))
