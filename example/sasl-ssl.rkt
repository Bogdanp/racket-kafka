#lang racket/base

(require kafka
         openssl
         racket/cmdline
         sasl/plain)

(define-values (key-path cert-path)
  (let ([key-path #f]
        [cert-path #f])
    (command-line
     #:once-each
     [("-k" "--key")
      KEY_PATH
      "the SSL private key (in PEM format)"
      (set! key-path KEY_PATH)]
     [("-c" "--cert")
      CERT_PATH
      "the SSL certificate chain"
      (set! cert-path CERT_PATH)]
     #:args []
     (unless (and key-path cert-path)
       (error "both the --key and --cert arguments are required"))
     (values key-path cert-path))))

(define c
  (make-client
   #:bootstrap-host "127.0.0.1"
   #:bootstrap-port 9092
   #:sasl-mechanism&ctx `(plain ,(plain-client-message "client" "client-secret"))
   #:ssl-ctx (ssl-make-client-context
              #:private-key `(pem ,key-path)
              #:certificate-chain cert-path
              'auto)))

(get-metadata c)
