#lang racket/base

(require racket/contract
         racket/generic
         racket/list
         "help.rkt"
         (prefix-in cproto: "protocol-consumer.bnf"))


;; assignor generics ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 assignor?
 gen:assignor
 (contract-out
  [assignor-name (-> assignor? string?)]
  [assignor-metadata (-> assignor? (listof string?) bytes?)]
  [assignor-assign (-> assignor?
                       (listof topic-partition?)
                       (non-empty-listof metadata?)
                       (hash/c string? (hash/c string? (listof exact-nonnegative-integer?))))]))

(define-generics assignor
  [assignor-name assignor]
  [assignor-metadata assignor topics]
  [assignor-assign assignor topic-partitions metas])


;; member-metadata ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 (contract-out
  [struct metadata ([member-id string?]
                    [version exact-nonnegative-integer?]
                    [topics (listof string?)]
                    [data bytes?])]
  [struct topic-partition ([topic string?]
                           [pid exact-nonnegative-integer?])]))

(struct metadata (member-id version topics data)
  #:transparent)

(struct topic-partition (topic pid)
  #:transparent)

(define (enc-member-metadata topics [version 0])
  (with-output-bytes
    (cproto:un-MemberMetadata
     `((Version_1 . ,version)
       (ArrayLen_1 . ,(length topics))
       (TopicName_1 . ,topics)
       (Data_1 . #"")))))


;; range ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 range)

(define range
  (let ()
    (struct range ()
      #:methods gen:assignor
      [(define (assignor-name _)
         "range")

       (define (assignor-metadata _ topics)
         (enc-member-metadata topics))

       (define (assignor-assign _ topic-partitions metas)
         (define members-by-topic
           (for*/fold ([members-by-topic (hash)]
                       #:result (for/hash ([(topic mids) (in-hash members-by-topic)])
                                  (values topic (reverse mids))))
                      ([m (in-list metas)]
                       [t (in-list (metadata-topics m))])
             (define mid (metadata-member-id m))
             (hash-update members-by-topic t (λ (ms) (cons mid ms)) null)))
         (for/fold ([assignments (hash)])
                   ([(topic member-ids) (in-hash members-by-topic)])
           (define pids
             (for/list ([t&p (in-list topic-partitions)] #:when (string=? (topic-partition-topic t&p) topic))
               (topic-partition-pid t&p)))
           (define-values (partitions-per-member members-with-extra)
             (quotient/remainder
              (length pids)
              (length member-ids)))
           (for/fold ([assignments assignments])
                     ([(member-id idx) (in-indexed (in-list member-ids))])
             (define pos
               (+ (* idx partitions-per-member)
                  (min idx members-with-extra)))
             (define num
               (if (< idx members-with-extra)
                   (add1 partitions-per-member)
                   partitions-per-member))
             (define partitions
               (take (drop pids pos) num))
             (hash-update assignments member-id (λ (t&ps) (hash-set t&ps topic partitions)) hash))))])

    (range)))

(module+ test
  (require rackunit)
  (check-equal?
   (assignor-assign range null (list (metadata "m1" 0 '("t1" "t2") #"")))
   (hash
    "m1" (hash "t1" null
               "t2" null)))
  (check-equal?
   (assignor-assign
    range
    (list
     (topic-partition "t1" 0))
    (list
     (metadata "m1" 0 '("t1") #"")
     (metadata "m2" 0 '("t1") #"")
     (metadata "m3" 0 '("t1") #"")))
   (hash "m1" (hash "t1" '(0))
         "m2" (hash "t1" null)
         "m3" (hash "t1" null)))
  (check-equal?
   (assignor-assign
    range
    (list
     (topic-partition "t1" 0)
     (topic-partition "t1" 1)
     (topic-partition "t1" 2))
    (list
     (metadata "m1" 0 '("t1") #"")
     (metadata "m2" 0 '("t1") #"")))
   (hash
    "m1" (hash "t1" '(0 1))
    "m2" (hash "t1" '(2))))
  (check-equal?
   (assignor-assign
    range
    (list
     (topic-partition "t1" 0)
     (topic-partition "t1" 1)
     (topic-partition "t1" 2)
     (topic-partition "t2" 0)
     (topic-partition "t2" 1)
     (topic-partition "t2" 2)
     (topic-partition "t3" 0)
     (topic-partition "t3" 1)
     (topic-partition "t3" 2)
     (topic-partition "t3" 3))
    (list
     (metadata "m1" 0 '("t1" "t2") #"")
     (metadata "m2" 0 '("t1" "t2" "t3") #"")))
   (hash
    "m1" (hash
          "t1" '(0 1)
          "t2" '(0 1))
    "m2" (hash
          "t1" '(2)
          "t2" '(2)
          "t3" '(0 1 2 3)))))


;; round robin ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(provide
 round-robin)

(define round-robin
  (let ()
    (struct round-robin ()
      #:methods gen:assignor
      [(define (assignor-name _)
         "roundrobin")

       (define (assignor-metadata _ topics)
         (enc-member-metadata topics))

       (define (assignor-assign _ topic-partitions metas)
         (define meta-by-id
           (for/hash ([m (in-list metas)])
             (values (metadata-member-id m) m)))
         (define member-ids
           (sort (map metadata-member-id metas) string<?))
         (define-values (_ next-member-id)
           (sequence-generate (in-cycle member-ids)))
         (define assignments
           (for/fold ([assignments (hash)])
                     ([t&p (in-list topic-partitions)])
             (define pid (topic-partition-pid t&p))
             (define topic (topic-partition-topic t&p))
             (define member-id
               (let loop ()
                 (define id (next-member-id))
                 (define topics (metadata-topics (hash-ref meta-by-id id)))
                 (if (member topic topics) id (loop))))
             (hash-update
              assignments
              member-id
              (λ (topics)
                (hash-update topics topic (λ (pids) (cons pid pids)) null))
              hash)))
         (for/hash ([(member-id topics) (in-hash assignments)])
           (values member-id (for/hash ([(topic pids) (in-hash topics)])
                               (values topic (reverse pids))))))])

    (round-robin)))

(module+ test
  (check-equal?
   (assignor-assign round-robin null (list (metadata "m1" 0 '("t1" "t2") #"")))
   (hash))
  (check-equal?
   (assignor-assign
    round-robin
    (list
     (topic-partition "t1" 0))
    (list
     (metadata "m1" 0 '("t1") #"")
     (metadata "m2" 0 '("t1") #"")))
   (hash "m1" (hash "t1" '(0))))
  (check-equal?
   (assignor-assign
    round-robin
    (list
     (topic-partition "t1" 0)
     (topic-partition "t1" 1)
     (topic-partition "t1" 2))
    (list
     (metadata "m1" 0 '("t1") #"")
     (metadata "m2" 0 '("t1") #"")))
   (hash
    "m1" (hash "t1" '(0 2))
    "m2" (hash "t1" '(1))))
  (check-equal?
   (assignor-assign
    round-robin
    (list
     (topic-partition "t1" 0)
     (topic-partition "t1" 1)
     (topic-partition "t1" 2)
     (topic-partition "t2" 0)
     (topic-partition "t2" 1)
     (topic-partition "t2" 2)
     (topic-partition "t3" 0)
     (topic-partition "t3" 1)
     (topic-partition "t3" 2)
     (topic-partition "t3" 3))
    (list
     (metadata "m1" 0 '("t1" "t2") #"")
     (metadata "m2" 0 '("t1" "t2" "t3") #"")))
   (hash
    "m1" (hash
          "t1" '(0 2)
          "t2" '(1))
    "m2" (hash
          "t1" '(1)
          "t2" '(0 2)
          "t3" '(0 1 2 3)))))
