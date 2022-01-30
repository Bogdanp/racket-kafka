#lang racket/base

(require racket/contract
         "core.rkt")

(define-record DeletedTopic
  ([error-code error-code/c]
   [error-message (or/c #f string?)]
   [name string?]
   [uuid (or/c #f bytes?)]
   [tags tags/c]))

(define-record DeletedTopics
  ([throttle-time-ms (or/c #f exact-integer?)]
   [topics (listof DeletedTopic?)]
   [tags tags/c]))

(define-request DeleteTopics
  (topics
   [timeout-ms 30000])
  #:code 20
  #:version 0 proto:DeleteTopicsResponseV0
  (lambda (topics timeout-ms)
    (with-output-bytes
      (proto:un-DeleteTopicsRequestV0
       `((TopicNames_1 . ((ArrayLen_1  . ,(length topics))
                          (TopicName_1 . ,topics)))
         (TimeoutMs_1 . ,timeout-ms)))))
  (lambda (res)
    (define topics
      (for/list ([topic (in-list (ref 'DeleteTopicsResponseDataV0_1 res))])
        (define err-code (ref 'ErrorCode_1 topic))
        (define name (ref 'TopicName_1 topic))
        (DeletedTopic err-code #f name #f (hasheqv))))
    (DeletedTopics #f topics (hasheqv))))
