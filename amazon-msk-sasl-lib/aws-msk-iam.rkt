#lang racket/base

(require crypto
         crypto/libcrypto
         json
         racket/format
         racket/port
         racket/string
         sasl/private/base
         threading)

(provide
 make-aws-msk-iam-ctx)

(struct aws-msk-iam-ctx sasl-ctx ())

;; https://github.com/aws/aws-msk-iam-auth/blob/2a0e1ef5cb9c08f4526b5642be060b5bfb84d50b/src/main/java/software/amazon/msk/auth/iam/internals/IAMSaslClient.java#L85
(define (make-aws-msk-iam-ctx #:region region
                              #:access-key-id access-key-id
                              #:secret-access-key secret-access-key
                              #:server-name server-name)
  (define payload
    (make-authentication-payload
     #:region region
     #:access-key-id access-key-id
     #:secret-access-key secret-access-key
     #:server-name server-name))
  (aws-msk-iam-ctx payload aws-msk-iam-receive))

(define (aws-msk-iam-receive ctx msg)
  (if (bytes=? msg #"")
      (set-sasl! ctx #f 'error)
      (set-sasl! ctx #f 'done)))


;; AWSV4 Auth ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; https://github.com/aws/aws-msk-iam-auth/blob/2a0e1ef5cb9c08f4526b5642be060b5bfb84d50b/src/main/java/software/amazon/msk/auth/iam/internals/AWS4SignedPayloadGenerator.java
;; https://github.com/aws/aws-msk-iam-auth/blob/2a0e1ef5cb9c08f4526b5642be060b5bfb84d50b/src/main/java/software/amazon/msk/auth/iam/internals/AuthenticationRequestParams.java
(define (make-authentication-payload #:region region
                                     #:access-key-id access-key-id
                                     #:secret-access-key secret-access-key
                                     #:server-name server-name)
  (define presigned-params
    (make-presigned-request
     #:region region
     #:access-key-id access-key-id
     #:secret-access-key secret-access-key
     #:scope "kafka-cluster"
     #:expires-in (* 15 60)
     #:params (hasheq 'Action "kafka-cluster:Connect")
     #:headers (hasheq 'Host server-name)
     #:payload-signature (sha256-hex-bytes #"")))
  (define augmented-params
    (~> presigned-params
        (hash-set 'host server-name)
        (hash-set 'version "2020_10_22")
        (hash-set 'user-agent "racket-kafka")))
  (jsexpr->bytes
   (for/hasheq ([(k v) (in-hash augmented-params)])
     (values (string->symbol (string-downcase (~a k))) v))))

;; https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
(define (make-presigned-request #:region region
                                #:access-key-id access-key-id
                                #:secret-access-key secret-access-key
                                #:date [the-date (seconds->date (current-seconds) #f)]
                                #:expires-in [expires 3600]
                                #:scope scope
                                #:method [method "GET"]
                                #:uri [uri "/"]
                                #:params [params (hasheq)]
                                #:headers [headers (hasheq)]
                                #:payload-signature [payload-signature #"UNSIGNED-PAYLOAD"])
  (define credentials
    (string-join
     (list access-key-id (~date the-date) region scope "aws4_request") "/"))
  (define params*
    (~> params
        (hash-set 'X-Amz-Algorithm "AWS4-HMAC-SHA256")
        (hash-set 'X-Amz-Credential credentials)
        (hash-set 'X-Amz-Date (~datetime the-date))
        (hash-set 'X-Amz-Expires (~a expires))
        (hash-set 'X-Amz-SignedHeaders (make-canonical-headers headers))))
  (define request
    (make-canonical-request method uri params* headers payload-signature))
  (define string-to-sign
    (make-string-to-sign the-date region scope request))
  (define signing-key
    (make-signing-key secret-access-key the-date region scope))
  (define signature
    (bytes->string/utf-8
     (make-signature signing-key string-to-sign)))
  (hash-set params* 'X-Amz-Signature signature))

;; https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
(define (make-signature signing-key string-to-sign)
  (call-with-hmac-context
   (lambda (hmac)
     (bytes->hex-bytes
      (hmac signing-key string-to-sign)))))

(define (make-signing-key secret-access-key the-date region scope)
  (call-with-hmac-context
   (lambda (hmac)
     (~> (~a "AWS4" secret-access-key)
         (string->bytes/utf-8)
         (hmac (~date the-date))
         (hmac region)
         (hmac scope)
         (hmac "aws4_request")))))

(define (call-with-hmac-context proc)
  (parameterize ([crypto-factories (list libcrypto-factory)])
    (define (hmac-proc k d)
      (hmac 'sha256 k d))
    (proc hmac-proc)))

;; https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
(define (make-string-to-sign the-date region scope canonical-request)
  (call-with-output-bytes
   (lambda (out)
     (displayln "AWS4-HMAC-SHA256" out)
     (displayln (~datetime the-date) out)
     (displayln (string-join (list (~date the-date) region scope "aws4_request") "/") out)
     (display (sha256-hex-bytes canonical-request) out))))

(define (~date d)
  (~a (date-year d)
      (pad (date-month d))
      (pad (date-day d))))

(define (~datetime d)
  (~a (~date d)
      "T"
      (pad (date-hour d))
      (pad (date-minute d))
      (pad (date-second d))
      "Z"))

(define (pad n)
  (if (< n 10)
      (~a "0" n)
      n))

;; https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
(define (make-canonical-request method uri params headers [payload-signature (sha256-hex-bytes #"")])
  (call-with-output-bytes
   (lambda (out)
     (displayln method out)
     (displayln uri out)
     (for* ([(k idx) (in-indexed (in-list (sort (hash-keys params) symbol<?)))]
            [v (in-value (hash-ref params k))])
       (unless (zero? idx)
         (display "&" out))
       (display (urlencode (symbol->string k)) out)
       (display "=" out)
       (display (urlencode v) out))
     (newline out)
     (for* ([k (in-list (sort (hash-keys headers) symbol<?))]
            [v (in-value (hash-ref headers k))])
       (display (string-downcase (symbol->string k)) out)
       (display ":" out)
       (displayln (string-trim v) out))
     (newline out)
     (displayln (make-canonical-headers headers) out)
     (display payload-signature out))))

(define (make-canonical-headers headers)
  (string-join
   (for/list ([k (in-list (sort (hash-keys headers) symbol<?))])
     (string-downcase (symbol->string k)))
   ";"))

(define (sha256-hex-bytes s)
  (bytes->hex-bytes
   (sha256-bytes (open-input-bytes s))))

(define (bytes->hex-bytes bs)
  (string->bytes/utf-8
   (string-downcase
    (bytes->hex-string bs))))

(define (urlencode s)
  (call-with-output-string
   (lambda (out)
     (for ([b (in-bytes (string->bytes/utf-8 s))])
       (cond
         [(or (and (>= b 48) (<= b 57))  ;; 0-9
              (and (>= b 65) (<= b 90))  ;; a-z
              (and (>= b 97) (<= b 122)) ;; A-Z
              (memv b '(45 46 95 128)))  ;; - . _ ~
          (write-char (integer->char b) out)]
         [else
          (write-char #\% out)
          (when (< b #x10)
            (write-char #\0 out))
          (write-string (string-upcase (number->string b 16)) out)])))))

(module+ test
  (require racket/date
           rackunit)

  (define canon
    (make-canonical-request
     "GET" "/"
     (hasheq
      'Action "ListUsers"
      'Version "2010-05-08")
     (hasheq
      'Content-Type "application/x-www-form-urlencoded; charset=utf-8"
      'Host "iam.amazonaws.com"
      'X-Amz-Date "20150830T123600Z")))

  (check-equal?
   (bytes->string/utf-8 canon)
   #<<EOF
GET
/
Action=ListUsers&Version=2010-05-08
content-type:application/x-www-form-urlencoded; charset=utf-8
host:iam.amazonaws.com
x-amz-date:20150830T123600Z

content-type;host;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
EOF
   )

  (define string-to-sign
    (make-string-to-sign
     (seconds->date (find-seconds 0 36 12 30 8 2015 #f) #f)
     "us-east-1"
     "iam"
     canon))
  (check-equal?
   (bytes->string/utf-8 string-to-sign)
   #<<EOF
AWS4-HMAC-SHA256
20150830T123600Z
20150830/us-east-1/iam/aws4_request
f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59
EOF
   )

  (define signing-key
    (make-signing-key
     "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
     (seconds->date (find-seconds 0 36 12 30 8 2015 #f) #f)
     "us-east-1"
     "iam"))
  (check-equal?
   (bytes->string/utf-8 (bytes->hex-bytes signing-key))
   "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9")

  (define signed-canon
    (make-signature signing-key string-to-sign))
  (check-equal?
   (bytes->string/utf-8 signed-canon)
   "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7")

  (define psr
    (make-presigned-request
     #:region "us-east-1"
     #:access-key-id "AKIAIOSFODNN7EXAMPLE"
     #:secret-access-key "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
     #:date (seconds->date (find-seconds 0 0 0 24 5 2013 #f) #f)
     #:expires-in 86400
     #:scope "s3"
     #:uri "/test.txt"
     #:headers (hasheq 'Host "examplebucket.s3.amazonaws.com")))
  (check-equal?
   (hash-ref psr 'X-Amz-Signature)
   "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"))
