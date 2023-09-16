#lang racket/base

(require kafka
         openssl
         racket/cmdline)

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
   #:bootstrap-host "broker"
   #:bootstrap-port 9092
   #:ssl-ctx (ssl-make-client-context
              #:private-key `(pem ,key-path)
              #:certificate-chain cert-path
              'auto)
   #:proxy (make-http-proxy "127.0.0.1" 1080)))
(get-metadata c)
(disconnect-all c)
