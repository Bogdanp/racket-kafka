#lang racket/base

(require racket/generic
         racket/list
         "client.rkt"
         "error.rkt"
         "help.rkt"
         (prefix-in cproto: "protocol-consumer.bnf")
         "serde.rkt")


;; assignor generics ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 assignor?
 gen:assignor
 assignor-metadata
 assignor-assign)

(define-generics assignor
  [assignor-metadata assignor topics]
  [assignor-assign assignor client metas])


;; member-metadata ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (struct-out metadata))

(struct metadata (member-id version topics data)
  #:transparent)


;; round robin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 round-robin)

(define round-robin
  (let ()
    (struct round-robin ()
      #:methods gen:assignor
      [(define (assignor-metadata _ topics)
         (make-Protocol
          #:name "roundrobin"
          #:metadata (with-output-bytes
                       (cproto:un-MemberMetadata
                        `((Version_1 . 0)
                          (ArrayLen_1 . ,(length topics))
                          (TopicName_1 ,@topics)
                          (Data_1 . #""))))))

       (define (assignor-assign _ c metas)
         (define conn (get-connection c))
         (define topic-metadata
           (sync
            (apply
             make-Metadata-evt conn
             (remove-duplicates
              (flatten (map metadata-topics metas))))))
         (define topic-partitions
           (sort
            (flatten
             (for/list ([t (in-list (Metadata-topics topic-metadata))])
               (define topic (TopicMetadata-name t))
               (define err-code (TopicMetadata-error-code t))
               (unless (zero? err-code)
                 (raise-server-error err-code))
               (sort
                (for/list ([p (in-list TopicMetadata-partitions t)])
                  (define pid (PartitionMetadata-id p))
                  (define err-code (PartitionMetadata-error-code t))
                  (unless (zero? err-code)
                    (raise-server-error err-code))
                  (cons topic pid))
                #:key cdr <)))
            #:key car string<?))
         (define meta-by-id
           (for/hash ([m (in-list metas)])
             (values (metadata-member-id m) m)))
         (define member-ids
           (sort (map metadata-member-id metas) string<?))
         (define-values (_ next-member-id)
           (sequence-generate (in-cycle member-ids)))
         (for/hash ([t&p (in-list topic-partitions)])
           (define topic (car t&p))
           (define member-id
             (let loop ()
               (define id (next-member-id))
               (define topics (hash-ref meta-by-id id))
               (if (member topic topics) id (loop))))
           (values member-id t&p)))])

    (round-robin)))
