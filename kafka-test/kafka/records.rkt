#lang racket/base

(require binfmt/runtime/res
         kafka/private/batch
         kafka/private/batch-native
         racket/port
         racket/runtime-path
         rackunit)

(define-runtime-path truncated-records.bin
  "fixtures/truncated-records.bin")
(define-runtime-path zstd-frame-no-length.bin
  "fixtures/zstd-frame-no-length.bin")

(define (check-read-truncated-records in)
  (define res (Records in))
  (check-true (ok? res))
  (check-equal? (length (ok-v res)) 6158)
  (check-equal?
   (for/sum ([b (in-list (ok-v res))])
     (batch-size b))
   6230))

(test-case "read truncated records"
  (test-case "from file"
    (call-with-input-file truncated-records.bin
      (lambda (in)
        (check-read-truncated-records in))))
  (test-case "from pipe"
    (call-with-input-file truncated-records.bin
      (lambda (file-in)
        (define-values (in out)
          (make-pipe))
        (thread
         (lambda ()
           (copy-port file-in out)
           (close-output-port out)))
        (check-read-truncated-records in)))))

(test-case "read zstd-compressed data where the decompressed length is unknown"
  (call-with-input-file zstd-frame-no-length.bin
    (lambda (in)
      (define res (read-batch in))
      (check-not-false res)
      (check-equal? (vector-length (batch-records res)) 1000))))
