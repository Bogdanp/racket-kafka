#lang racket/base

(require (for-syntax racket/base
                     racket/fixnum)
         racket/fixnum)

(provide
 crc
 crc-update)

(begin-for-syntax
  (unless (> (most-positive-fixnum) #xFFFFFFFF)
    (error 'crc "the CRC implementation requires fixnums to be at least 32bit wide")))

(define mask #xFFFFFFFF)
(define table
  (fxvector
   #x00000000 #xf26b8303 #xe13b70f7 #x1350f3f4
   #xc79a971f #x35f1141c #x26a1e7e8 #xd4ca64eb
   #x8ad958cf #x78b2dbcc #x6be22838 #x9989ab3b
   #x4d43cfd0 #xbf284cd3 #xac78bf27 #x5e133c24
   #x105ec76f #xe235446c #xf165b798 #x030e349b
   #xd7c45070 #x25afd373 #x36ff2087 #xc494a384
   #x9a879fa0 #x68ec1ca3 #x7bbcef57 #x89d76c54
   #x5d1d08bf #xaf768bbc #xbc267848 #x4e4dfb4b
   #x20bd8ede #xd2d60ddd #xc186fe29 #x33ed7d2a
   #xe72719c1 #x154c9ac2 #x061c6936 #xf477ea35
   #xaa64d611 #x580f5512 #x4b5fa6e6 #xb93425e5
   #x6dfe410e #x9f95c20d #x8cc531f9 #x7eaeb2fa
   #x30e349b1 #xc288cab2 #xd1d83946 #x23b3ba45
   #xf779deae #x05125dad #x1642ae59 #xe4292d5a
   #xba3a117e #x4851927d #x5b016189 #xa96ae28a
   #x7da08661 #x8fcb0562 #x9c9bf696 #x6ef07595
   #x417b1dbc #xb3109ebf #xa0406d4b #x522bee48
   #x86e18aa3 #x748a09a0 #x67dafa54 #x95b17957
   #xcba24573 #x39c9c670 #x2a993584 #xd8f2b687
   #x0c38d26c #xfe53516f #xed03a29b #x1f682198
   #x5125dad3 #xa34e59d0 #xb01eaa24 #x42752927
   #x96bf4dcc #x64d4cecf #x77843d3b #x85efbe38
   #xdbfc821c #x2997011f #x3ac7f2eb #xc8ac71e8
   #x1c661503 #xee0d9600 #xfd5d65f4 #x0f36e6f7
   #x61c69362 #x93ad1061 #x80fde395 #x72966096
   #xa65c047d #x5437877e #x4767748a #xb50cf789
   #xeb1fcbad #x197448ae #x0a24bb5a #xf84f3859
   #x2c855cb2 #xdeeedfb1 #xcdbe2c45 #x3fd5af46
   #x7198540d #x83f3d70e #x90a324fa #x62c8a7f9
   #xb602c312 #x44694011 #x5739b3e5 #xa55230e6
   #xfb410cc2 #x092a8fc1 #x1a7a7c35 #xe811ff36
   #x3cdb9bdd #xceb018de #xdde0eb2a #x2f8b6829
   #x82f63b78 #x709db87b #x63cd4b8f #x91a6c88c
   #x456cac67 #xb7072f64 #xa457dc90 #x563c5f93
   #x082f63b7 #xfa44e0b4 #xe9141340 #x1b7f9043
   #xcfb5f4a8 #x3dde77ab #x2e8e845f #xdce5075c
   #x92a8fc17 #x60c37f14 #x73938ce0 #x81f80fe3
   #x55326b08 #xa759e80b #xb4091bff #x466298fc
   #x1871a4d8 #xea1a27db #xf94ad42f #x0b21572c
   #xdfeb33c7 #x2d80b0c4 #x3ed04330 #xccbbc033
   #xa24bb5a6 #x502036a5 #x4370c551 #xb11b4652
   #x65d122b9 #x97baa1ba #x84ea524e #x7681d14d
   #x2892ed69 #xdaf96e6a #xc9a99d9e #x3bc21e9d
   #xef087a76 #x1d63f975 #x0e330a81 #xfc588982
   #xb21572c9 #x407ef1ca #x532e023e #xa145813d
   #x758fe5d6 #x87e466d5 #x94b49521 #x66df1622
   #x38cc2a06 #xcaa7a905 #xd9f75af1 #x2b9cd9f2
   #xff56bd19 #x0d3d3e1a #x1e6dcdee #xec064eed
   #xc38d26c4 #x31e6a5c7 #x22b65633 #xd0ddd530
   #x0417b1db #xf67c32d8 #xe52cc12c #x1747422f
   #x49547e0b #xbb3ffd08 #xa86f0efc #x5a048dff
   #x8ecee914 #x7ca56a17 #x6ff599e3 #x9d9e1ae0
   #xd3d3e1ab #x21b862a8 #x32e8915c #xc083125f
   #x144976b4 #xe622f5b7 #xf5720643 #x07198540
   #x590ab964 #xab613a67 #xb831c993 #x4a5a4a90
   #x9e902e7b #x6cfbad78 #x7fab5e8c #x8dc0dd8f
   #xe330a81a #x115b2b19 #x020bd8ed #xf0605bee
   #x24aa3f05 #xd6c1bc06 #xc5914ff2 #x37faccf1
   #x69e9f0d5 #x9b8273d6 #x88d28022 #x7ab90321
   #xae7367ca #x5c18e4c9 #x4f48173d #xbd23943e
   #xf36e6f75 #x0105ec76 #x12551f82 #xe03e9c81
   #x34f4f86a #xc69f7b69 #xd5cf889d #x27a40b9e
   #x79b737ba #x8bdcb4b9 #x988c474d #x6ae7c44e
   #xbe2da0a5 #x4c4623a6 #x5f16d052 #xad7d5351))

(define (crc-update n bs [start 0] [end (bytes-length bs)])
  (for/fold ([n (fxxor n mask)] #:result (fxxor n mask))
            ([b (in-bytes bs start end)])
    (define idx (fxand (fxxor n b) #xFF))
    (define v (fxvector-ref table idx))
    (fxand (fxxor v (fxrshift n 8)) mask)))

(define (crc bs [start 0] [end (bytes-length bs)])
  (crc-update 0 bs start end))

(module+ test
  (require rackunit)
  (check-equal? (crc #"hello") 2591144780)
  (check-equal? (crc #"  hello " 2 7) 2591144780)
  (check-equal?
   (crc
    (subbytes
     (bytes-append
      #"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3a"
      #"\x00\x00\x00\x00\x02\xff\x5e\x0a\x7f\x00\x00\x00"
      #"\x00\x00\x00\x00\x00\x01\x7e\xe7\xeb\xbf\xcd\x00"
      #"\x00\x01\x7e\xe7\xeb\xbf\xcd\xff\xff\xff\xff\xff"
      #"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00"
      #"\x01\x10\x00\x00\x00\x02\x61\x02\x61\x00")
     21))
   4284353151))
