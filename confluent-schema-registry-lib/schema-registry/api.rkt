#lang racket/base

(require json
         racket/string
         threading
         "client.rkt"
         "record.rkt")

;; data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-record Reference
  ([name]
   [subject]
   [version]))

(define-record Schema
  ([id]
   [subject]
   [version]
   [schema]
   [type #:key 'schemaType
         #:default "AVRO"
         #:convert-from variant->symbol
         #:convert-to symbol->variant]
   [references #:default null
               #:convert-from (λ (rs) (map jsexpr->Reference rs))
               #:convert-to (λ (rs) (map Reference->jsexpr rs))]))

;; schema ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; https://docs.confluent.io/platform/current/schema-registry/develop/api.html#schemas

(provide
 get-schema
 get-schema-types
 get-schema-versions)

(define (get-schema c id)
  (jsexpr->Schema
   (get c (format "/schemas/ids/~a" id))))

(define (get-schema-types c)
  (map variant->symbol (get c "/schemas/types/")))

(define (get-schema-versions c id)
  (for/list ([p (in-list (get c (format "/schemas/ids/~a/versions" id)))])
    (cons (hash-ref p 'subject)
          (hash-ref p 'version))))

;; subject ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; https://docs.confluent.io/platform/current/schema-registry/develop/api.html#subjects

(provide
 get-all-subjects
 get-subject-versions
 get-subject-version
 get-subject-version-referenced-by
 delete-subject
 delete-subject-version
 register-schema
 check-schema)

(define (get-all-subjects c
                          #:prefix [subject-prefix #f]
                          #:include-deleted? [include-deleted? #f])
  (get
   #:params (~> (maybe-add-param null include-deleted? 'deleted "true")
                (maybe-add-param subject-prefix 'subjectPrefix subject-prefix))
   c "/subjects"))

(define (get-subject-versions c subject)
  (get c (format "/subjects/~a/versions" subject)))

(define (get-subject-version c subject version-id)
  (jsexpr->Schema
   (get c (format "/subjects/~a/versions/~a" subject version-id))))

(define (get-subject-version-referenced-by c subject version-id)
  (get c (format "/subjects/~a/versions/~a/referencedby" subject version-id)))

(define (delete-subject c subject #:permanently? [permanent? #f])
  (delete
   #:params (maybe-add-param null permanent? 'permanent "true")
   c (format "/subjects/~a" subject)))

(define (delete-subject-version c subject version-id #:permanently? [permanent? #f])
  (delete
   #:params (maybe-add-param null permanent? 'permanent "true")
   c (format "/subjects/~a/versions/~a" subject version-id)))

(define (register-schema c subject schema)
  (define res
    (post
     #:headers (hasheq 'content-type "application/json")
     #:data (jsexpr->bytes (Schema->jsexpr schema))
     c (format "/subjects/~a/versions" subject)))
  (hash-ref res 'id))

(define (check-schema c subject schema)
  (jsexpr->Schema
   (post
    #:headers (hasheq 'content-type "application/json")
    #:data (jsexpr->bytes (Schema->jsexpr schema))
    c (format "/subjects/~a" subject))))


;; mode ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; https://docs.confluent.io/platform/current/schema-registry/develop/api.html#mode

(provide
 get-mode
 set-mode
 delete-mode)

(define (get-mode c #:subject [subject #f])
  (extract-mode (get c (if subject (format "/mode/~a" subject) "/mode"))))

(define (set-mode c mode
                  #:subject [subject #f]
                  #:force? [force? #f])
  (extract-mode
   (put
    #:params (maybe-add-param null force? 'force "true")
    #:data (jsexpr->bytes (hasheq 'mode (symbol->variant mode)))
    c (if subject (format "/mode/~a" subject) "/mode"))))

(define (delete-mode c subject)
  (extract-mode
   (delete c (format "/mode/~a" subject))))


;; config ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; https://docs.confluent.io/platform/current/schema-registry/develop/api.html#config

(provide
 get-config
 set-config
 delete-config)

(define-record Config
  ([compatibility-level #:key 'compatibilityLevel
                        #:to-key 'compatibility
                        #:default "FULL"
                        #:convert-from variant->symbol
                        #:convert-to symbol->variant]))

(define (get-config c #:subject [subject #f])
  (jsexpr->Config (get c (if subject (format "/config/~a" subject) "/config"))))

(define (set-config c conf #:subject [subject #f])
  (jsexpr->Config
   (put
    #:data (jsexpr->bytes (Config->jsexpr conf))
    c (if subject (format "/config/~a" subject) "/config"))))

(define (delete-config c subject)
  (jsexpr->Config (delete c (format "/config/~a" subject))))


;; help ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (extract-mode res)
  (variant->symbol (hash-ref res 'mode)))

(define (maybe-add-param params add? k v)
  (if add? (cons (cons k v) params) params))

(define (variant->symbol t)
  (string->symbol (string-downcase (string-replace t "_" "-"))))

(define (symbol->variant s)
  (string-replace (string-upcase (symbol->string s)) "-" "_"))
