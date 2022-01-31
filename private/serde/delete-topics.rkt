#lang racket/base

(require racket/contract
         "core.rkt")

(define-record DeletedTopic
  ([(error-code 0) error-code/c]
   [(error-message #f) (or/c #f string?)]
   [name string?]
   [(uuid #f) (or/c #f bytes?)]
   [(tags #f) (or/c #f tags/c)]))

(define-record DeletedTopics
  ([(throttle-time-ms #f) (or/c #f exact-integer?)]
   [topics (listof DeletedTopic?)]
   [(tags #f) (or/c #f tags/c)]))

(define-request DeleteTopics
  (topics
   [timeout-ms 30000]
   [tags (hasheqv)])
  #:code 20
  #:version 0
  #:response proto:DeleteTopicsResponseV0
  enc-delete-topicsv0
  (lambda (res)
    (define topics
      (for/list ([topic (in-list (ref 'DeleteTopicsResponseDataV0_1 res))])
        (make-DeletedTopic
         #:error-code (ref 'ErrorCode_1 topic)
         #:name (ref 'TopicName_1 topic))))
    (make-DeletedTopics #:topics topics))

  #:version 1
  #:response proto:DeleteTopicsResponseV1
  enc-delete-topicsv0
  dec-delete-topicsv1

  #:version 2
  #:response proto:DeleteTopicsResponseV2
  enc-delete-topicsv0
  dec-delete-topicsv1

  #:version 3
  #:response proto:DeleteTopicsResponseV3
  enc-delete-topicsv0
  dec-delete-topicsv1

  #:version 4
  #:flexible #t
  #:response proto:DeleteTopicsResponseV4
  enc-delete-topicsv4
  dec-delete-topicsv4)

(define (enc-delete-topicsv0 topics timeout-ms tags)
  (unless (zero? (hash-count tags))
    (raise-argument-error 'DeleteTopics "(hasheqv)" tags))
  (with-output-bytes
    (proto:un-DeleteTopicsRequestV0
     `((TopicNames_1 . ((ArrayLen_1  . ,(length topics))
                        (TopicName_1 . ,topics)))
       (TimeoutMs_1 . ,timeout-ms)))))

(define (enc-delete-topicsv4 topics timeout-ms tags)
  (with-output-bytes
    (proto:un-DeleteTopicsRequestV4
     `((CompactTopicNames_1 . ((CompactArrayLen_1  . ,(length topics))
                               (CompactTopicName_1 . ,topics)))
       (TimeoutMs_1 . ,timeout-ms)
       (Tags_1 . ,tags)))))

(define (dec-delete-topicsv1 res)
  (define topics
    (for/list ([topic (in-list (ref 'DeleteTopicsResponseDataV0_1 'DeleteTopicsResponsesV0_1 res))])
      (make-DeletedTopic
       #:error-code (ref 'ErrorCode_1 topic)
       #:name (ref 'TopicName_1 topic))))
  (make-DeletedTopics
   #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
   #:topics topics))

(define (dec-delete-topicsv4 res)
  (define topics
    (for/list ([topic (in-list (ref 'DeleteTopicsResponseDataV4_1 'DeleteTopicsResponsesV4_1 res))])
      (make-DeletedTopic
       #:error-code (ref 'ErrorCode_1 topic)
       #:name (ref 'CompactTopicName_1 topic)
       #:tags (ref 'Tags_1 topic))))
  (make-DeletedTopics
   #:throttle-time-ms (ref 'ThrottleTimeMs_1 res)
   #:topics topics
   #:tags (ref 'Tags_1 res)))
