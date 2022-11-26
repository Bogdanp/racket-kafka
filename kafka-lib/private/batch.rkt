#lang racket/base

(require file/gunzip
         file/gzip
         file/lz4
         racket/port
         (prefix-in proto: "batch.bnf")
         "crc.rkt"
         "help.rkt"
         "record.rkt")

(provide
 batch?
 make-batch
 batch-compression
 batch-len
 batch-size
 batch-append!
 batch-records
 write-batch
 read-batch)

(struct batch
  (base-offset
   partition-leader-epoch
   attributes
   last-offset-delta
   [first-timestamp #:mutable]
   [max-timestamp #:mutable]
   producer-id
   producer-epoch
   base-sequence
   [size #:mutable]
   buf
   data
   data-out
   [records #:mutable]))

(define (make-batch #:compression [compression 'none])
  (define buf (make-record-data))
  (define data (make-record-data))
  (define-values (attributes data-out)
    (case compression
      [(none) (values #x00 (open-output-record-data data))]
      [(gzip) (values #x01 (open-output-record-data/gzip data))]
      [else (raise-arguments-error 'make-batch "(or/c 'none 'gzip)" "compression" compression)]))
  (batch
   0 ;; base-offset
   0 ;; partition-leader-epoch
   attributes
   0 ;; last-offset-delta
   #f ;; first-timestamp
   0 ;; max-timestamp
   0 ;; producer-id
   0 ;; producer-epoch
   0 ;; base-sequence
   0 ;; size
   buf
   data
   data-out
   #f ;; records
   ))

(define (batch-compression b)
  (case (bitwise-and (batch-attributes b) #b111)
    [(#b000) 'none]
    [(#b001) 'gzip]
    [(#b010) 'snappy]
    [(#b011) 'lz4]
    [(#b100) 'zstd]
    [else (error 'batch-compression "unknown compression")]))

(define (batch-len b)
  (record-data-len (batch-data b)))

(define (batch-append! b k v
                       #:headers [headers (hash)]
                       #:timestamp [timestamp (current-milliseconds)])
  (unless (batch-first-timestamp b)
    (set-batch-first-timestamp! b timestamp))
  (when (< (batch-max-timestamp b) timestamp)
    (set-batch-max-timestamp! b timestamp))

  (define buf (batch-buf b))
  (define buf-out (open-output-record-data buf))
  (reset-record-data! buf)
  (proto:un-Attributes 0 buf-out)
  (proto:un-TimestampDelta (- timestamp (batch-first-timestamp b)) buf-out)
  (proto:un-OffsetDelta (batch-size b) buf-out)
  (proto:un-Key k buf-out)
  (proto:un-Value v buf-out)
  (proto:un-Headers
   `((HeadersLen_1 . ,(hash-count headers))
     (Header_1 . ,(for/list ([(k v) (in-hash headers)])
                    `((Key_1   . ,k)
                      (Value_1 . ,v)))))
   buf-out)

  (define data-out (batch-data-out b))
  (define record-len
    (record-data-len buf))
  (proto:un-Length record-len data-out)
  (copy-record-data buf data-out)
  (set-batch-size! b (add1 (batch-size b))))

(define (write-batch b out)
  (close-output-port (batch-data-out b))

  (define buf (batch-buf b))
  (define buf-out (open-output-record-data buf))
  (reset-record-data! buf)
  (proto:un-PartitionLeaderEpoch 0 buf-out)
  (proto:un-Magic 2 buf-out)
  (proto:un-CRC 0 buf-out)
  (proto:un-BatchAttributes (batch-attributes b) buf-out)
  (proto:un-LastOffsetDelta (sub1 (batch-size b)) buf-out)
  (proto:un-FirstTimestamp (batch-first-timestamp b) buf-out)
  (proto:un-MaxTimestamp (batch-max-timestamp b) buf-out)
  (proto:un-ProducerID -1 buf-out)
  (proto:un-ProducerEpoch -1 buf-out)
  (proto:un-BaseSequence -1 buf-out)
  (proto:un-RecordCount (batch-size b) buf-out)

  (define data
    (batch-data b))
  (define len
    (+ (record-data-len buf)
       (record-data-len data)))
  (proto:un-BaseOffset 0 out)
  (proto:un-BatchLength len out)

  ;; buf[0...3]: PartitionLeaderEpoch
  ;; buf[4...4]: Magic
  ;; buf[5...8]: CRC
  ;; buf[9...n]: CRC-able data
  (define crc-bytes
    (let* ([crc (crc (record-data-bs buf) 9 (record-data-len buf))]
           [crc (crc-update crc (record-data-bs data) 0 (record-data-len data))])
      (call-with-output-bytes
       (lambda (crc-out)
         (proto:un-CRC crc crc-out)))))
  (record-data-set! buf 5 crc-bytes)
  (copy-record-data buf out)
  (copy-record-data data out))

(module+ test
  (require rackunit)

  (test-case "write"
    (define b (make-batch))
    (batch-append! b #"a" #"1" #:timestamp 5)
    (batch-append! b #"b" #"2" #:timestamp 20)
    (define batch-bs
      (call-with-output-bytes
       (lambda (out)
         (write-batch b out))))
    (define batch-bs-in
      (open-input-bytes batch-bs))
    (check-equal?
     (proto:RecordBatch batch-bs-in)
     '((BaseOffset_1 . 0)
       (BatchLength_1 . 67)
       (PartitionLeaderEpoch_1 . 0)
       (Magic_1 . 2)
       (CRC_1 . 4119354850)
       (BatchAttributes_1 . 0)
       (LastOffsetDelta_1 . 1)
       (FirstTimestamp_1 . 5)
       (MaxTimestamp_1 . 20)
       (ProducerID_1 . -1)
       (ProducerEpoch_1 . -1)
       (BaseSequence_1 . -1)
       (RecordCount_1 . 2)))
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 8)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 0)
       (OffsetDelta_1 . 0)
       (Key_1 . #"a")
       (Value_1 . #"1")
       (Headers_1 (HeadersLen_1 . 0) (Header_1))))
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 8)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 15)
       (OffsetDelta_1 . 1)
       (Key_1 . #"b")
       (Value_1 . #"2")
       (Headers_1 (HeadersLen_1 . 0) (Header_1)))))

  (test-case "large writes"
    (define b (make-batch))
    (batch-append! b #"a" (make-bytes (* 10 1024 1024) 65))
    (batch-append! b #"b" (make-bytes (* 10 1024 1024) 65))
    (check-true (> (batch-len b) (* 20 1024 1024))))

  (test-case "write nulls"
    (define b (make-batch))
    (batch-append! b #f #"1" #:timestamp 5)
    (batch-append! b #"b" #f #:timestamp 20)
    (define batch-bs
      (call-with-output-bytes
       (lambda (out)
         (write-batch b out))))
    (define batch-bs-in
      (open-input-bytes batch-bs))
    (check-equal?
     (proto:RecordBatch batch-bs-in)
     '((BaseOffset_1 . 0)
       (BatchLength_1 . 65)
       (PartitionLeaderEpoch_1 . 0)
       (Magic_1 . 2)
       (CRC_1 . 2331736347)
       (BatchAttributes_1 . 0)
       (LastOffsetDelta_1 . 1)
       (FirstTimestamp_1 . 5)
       (MaxTimestamp_1 . 20)
       (ProducerID_1 . -1)
       (ProducerEpoch_1 . -1)
       (BaseSequence_1 . -1)
       (RecordCount_1 . 2)))
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 7)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 0)
       (OffsetDelta_1 . 0)
       (Key_1 . #f)
       (Value_1 . #"1")
       (Headers_1 (HeadersLen_1 . 0) (Header_1))))
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 7)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 15)
       (OffsetDelta_1 . 1)
       (Key_1 . #"b")
       (Value_1 . #f)
       (Headers_1 (HeadersLen_1 . 0) (Header_1))))))

(define header-len 49)
(define (read-batch in)
  (define header (proto:RecordBatch in))
  (define data-len
    (- (ref 'BatchLength_1 header) header-len))
  (define the-batch
    (batch
     (ref 'BaseOffset_1 header)
     (ref 'PartitionLeaderEpoch_1 header)
     (ref 'BatchAttributes_1 header)
     (ref 'LastOffsetDelta_1 header)
     (ref 'FirstTimestamp_1 header)
     (ref 'MaxTimestamp_1 header)
     (ref 'ProducerID_1 header)
     (ref 'ProducerEpoch_1 header)
     (ref 'BaseSequence_1 header)
     (ref 'RecordCount_1 header)
     #f ;; buf
     #f ;; data
     #f ;; data-out
     #f ;; records
     ))

  (define data-in (make-limited-input-port in data-len #f))
  (define compression
    (batch-compression the-batch))
  (define records-in
    (case compression
      [(none) data-in]
      [(gzip)
       (define out (open-output-bytes))
       (gunzip-through-ports data-in out)
       (open-input-bytes (get-output-bytes out))]
      [(lz4)
       (define out (open-output-bytes))
       (lz4-decompress-through-ports data-in out)
       (open-input-bytes (get-output-bytes out))]
      [else
       (error 'read-batch "unsupported compression type: ~a" compression)]))

  (define base-offset (batch-base-offset the-batch))
  (define base-timestamp (batch-first-timestamp the-batch))
  (define size (batch-size the-batch))
  (define records
    (for/vector #:length size ([_ (in-range size)])
      (define rec (proto:Record records-in))
      (parse-record rec base-offset base-timestamp)))
  (begin0 the-batch
    (set-batch-records! the-batch records)))

(module+ test
  (test-case "read uncompressed"
    (define b0 (make-batch))
    (batch-append! b0 #"a" #"1" #:timestamp 0)
    (define in
      (open-input-bytes
       (call-with-output-bytes
        (lambda (out)
          (write-batch b0 out)))))
    (define b1 (read-batch in))
    (check-equal? (batch-compression b1) 'none)
    (check-equal? (batch-size b1) 1)
    (check-equal? (record-key (vector-ref (batch-records b1) 0)) #"a")
    (check-equal? (record-value (vector-ref (batch-records b1) 0)) #"1"))

  (test-case "read gzipped"
    (define b0 (make-batch #:compression 'gzip))
    (batch-append! b0 #"a" #"1" #:timestamp 0)
    (define in
      (open-input-bytes
       (call-with-output-bytes
        (lambda (out)
          (write-batch b0 out)))))
    (define b1 (read-batch in))
    (check-equal? (batch-compression b1) 'gzip)
    (check-equal? (batch-size b1) 1)
    (check-equal? (record-key (vector-ref (batch-records b1) 0)) #"a")
    (check-equal? (record-value (vector-ref (batch-records b1) 0)) #"1")))


;; record-data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct record-data (len bs)
  #:mutable)

(define (make-record-data [cap (* 16 1024)])
  (record-data 0 (make-bytes cap)))

(define (record-data-set! rd k src)
  (define dst (record-data-bs rd))
  (for ([(b idx) (in-indexed (in-bytes src))])
    (bytes-set! dst (+ k idx) b)))

(define (reset-record-data! rd)
  (set-record-data-len! rd 0))

(define (append-record-data! rd src [start 0] [end (bytes-length src)])
  (define dst (record-data-bs rd))
  (define len (record-data-len rd))
  (define cap (bytes-length dst))
  (define needed (- end start))
  (define available (- cap len))
  (cond
    [(>= available needed)
     (bytes-copy! dst len src start end)
     (set-record-data-len! rd (+ len needed))]

    [else
     (define bs (make-bytes (+ needed (* cap 2))))
     (bytes-copy! bs 0 dst 0 len)
     (bytes-copy! bs len src start end)
     (set-record-data-bs! rd bs)
     (set-record-data-len! rd (+ len needed))]))

(define (open-output-record-data rd)
  (make-output-port
   'record-data
   always-evt
   (λ (bs start end _flush? _enable-breaks?)
     (begin0 (- end start)
       (append-record-data! rd bs start end)))
   void))

(define (open-output-record-data/gzip rd)
  (define rd-out (open-output-record-data rd))
  (define-values (in out)
    (make-pipe))
  (define thd
    (thread
     (lambda ()
       (gzip-through-ports in rd-out #f (current-seconds)))))
  (make-output-port
   'record-data/gzip
   always-evt
   out
   (lambda ()
     (close-output-port out)
     (void (sync thd)))))

(define (copy-record-data rd out)
  (write-bytes (record-data-bs rd) out 0 (record-data-len rd)))
