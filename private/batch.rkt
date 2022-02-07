#lang racket/base

(require file/gzip
         racket/port
         (prefix-in proto: "batch.bnf")
         "crc.rkt")

(provide
 batch?
 make-batch
 batch-size
 batch-append!
 write-batch)

(struct batch
  (base-offset
   partition-leader-epoch
   attributes
   last-offset-delta
   first-timestamp
   max-timestamp
   producer-id
   producer-epoch
   base-sequence
   [size #:mutable]
   buf
   data
   data-out))

(define (make-batch #:compression [compression 'none])
  (define buf (make-record-data))
  (define data (make-record-data))
  (define-values (attributes data-out)
    (case compression
      [(none) (values #x00 (open-output-record-data data))]
      [(gzip) (values #x01 (open-output-record-data/gzip data))]
      [else (raise-arguments-error 'make-batch "(or/c 'none 'gzip)" "compression" compression)]))
  (batch
   0 ;; base offset
   0 ;; partition leader epoch
   attributes
   0 ;; last-offset-delta
   0 ;; first-timestamp
   0 ;; max-timestamp
   0 ;; producer-id
   0 ;; producer-epoch
   0 ;; base-sequence
   0 ;; size
   buf
   data
   data-out))

(define (batch-compression b)
  (case (bitwise-and (batch-attributes b) #b111)
    [(#b000) 'none]
    [(#b001) 'gzip]
    [(#b010) 'snappy]
    [(#b011) 'lz4]
    [(#b100) 'zstd]
    [else (error 'batch-compression "unknown compression")]))

(define (batch-append! b k v)
  (define buf (batch-buf b))
  (define buf-out (open-output-record-data buf))
  (reset-record-data! buf)
  (proto:un-Attributes 0 buf-out)
  (proto:un-TimestampDelta 0 buf-out)
  (proto:un-OffsetDelta 0 buf-out)
  (proto:un-Key k buf-out)
  (proto:un-Value v buf-out)
  (proto:un-Headers
   `((varint32_1 . 0)
     (Header_1 . ()))
   buf-out)

  (define data-out (batch-data-out b))
  (define record-len
    (record-data-len buf))
  (proto:un-Length record-len data-out)
  (copy-record-data buf data-out)
  (set-batch-size! b (add1 (batch-size b))))

(define (write-batch b out)
  (define buf (batch-buf b))
  (define buf-out (open-output-record-data buf))
  (reset-record-data! buf)
  (proto:un-PartitionLeaderEpoch 0 buf-out)
  (proto:un-Magic 2 buf-out)
  (proto:un-CRC 0 buf-out)
  (proto:un-BatchAttributes (batch-attributes b) buf-out)
  (proto:un-LastOffsetDelta 0 buf-out)
  (proto:un-FirstTimestamp 0 buf-out)
  (proto:un-MaxTimestamp 0 buf-out)
  (proto:un-ProducerID 0 buf-out)
  (proto:un-ProducerEpoch 0 buf-out)
  (proto:un-BaseSequence 0 buf-out)
  (case (batch-compression b)
    [(none) (proto:un-RecordCount (batch-size b) buf-out)]
    [else (void)])

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
    (let* ([crc 0]
           [crc (crc-update crc (record-data-bs buf)  9 (record-data-len buf))]
           [crc (crc-update crc (record-data-bs data) 0 (record-data-len data))])
      (call-with-output-bytes
       (lambda (crc-out)
         (proto:un-CRC crc crc-out)))))
  (record-data-set! buf 5 crc-bytes)
  (copy-record-data buf out)
  (copy-record-data data out))

(module+ test
  (require rackunit)

  (test-case "round trip"
    (define b (make-batch))
    (batch-append! b #"a" #"1")
    (batch-append! b #"b" #"2")
    (define batch-bs
      (call-with-output-bytes
       (lambda (out)
         (write-batch b out))))
    (define batch-bs-in
      (open-input-bytes batch-bs))
    (check-equal?
     (proto:RecordBatch batch-bs-in)
     '((BaseOffset_1 . 0)
       (BatchLength_1 . 64)
       (PartitionLeaderEpoch_1 . 0)
       (Magic_1 . 2)
       (CRC_1 . 3353461548)
       (BatchAttributes_1 . 0)
       (LastOffsetDelta_1 . 0)
       (FirstTimestamp_1 . 0)
       (MaxTimestamp_1 . 0)
       (ProducerID_1 . 0)
       (ProducerEpoch_1 . 0)
       (BaseSequence_1 . 0)))
    (check-equal?
     (proto:RecordCount batch-bs-in)
     2)
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 8)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 0)
       (OffsetDelta_1 . 0)
       (Key_1 . #"a")
       (Value_1 . #"1")
       (Headers_1 (varint32_1 . 0) (Header_1))))
    (check-equal?
     (proto:Record batch-bs-in)
     '((Length_1 . 8)
       (Attributes_1 . 0)
       (TimestampDelta_1 . 0)
       (OffsetDelta_1 . 0)
       (Key_1 . #"b")
       (Value_1 . #"2")
       (Headers_1 (varint32_1 . 0) (Header_1))))))


;; record-data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(struct record-data (len bs)
  #:mutable)

(define (make-record-data [cap (* 16 1024)])
  (record-data 0 (make-bytes cap)))

(define (record-data-bytes rd)
  (subbytes (record-data-bs rd) 0 (record-data-len rd)))

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
     (bytes-copy! bs dst 0 len)
     (bytes-copy! bs len src start end)
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
  (begin0 out
    (thread
     (lambda ()
       (gzip-through-ports in rd-out #f (current-seconds))))))

(define (copy-record-data rd out)
  (write-bytes (record-data-bs rd) out 0 (record-data-len rd)))
