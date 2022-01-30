#lang racket/base

(provide
 exn:fail:kafka?
 exn:fail:kafka-code
 kafka-error
 raise-kafka-error)

(struct exn:fail:kafka exn:fail (code))

(define kafka-error
  (case-lambda
    [(code)
     (kafka-error code (error-code-message code))]
    [(code message . args)
     (exn:fail:kafka (apply format message args) (current-continuation-marks) code)]))

(define (raise-kafka-error . args)
  (raise (apply kafka-error args)))

(define-syntax-rule (define-error-codes id [code message] ...)
  (define (id c)
    (case c
      [(code) message] ...
      [else "unknown error code"])))

(define-error-codes error-code-message
  [-1  "unknown server error"]
  [0   "no error"]
  [1   "offset out of range"]
  [2   "corrupt message"]
  [3   "unknown topic or partition"]
  [4   "invalid fetch size"]
  [5   "leader not available"]
  [6   "not leader or follower"]
  [7   "request timed out"]
  [8   "broker not available"]
  [9   "replica not available"]
  [10  "message too large"]
  [11  "stale controller epoch"]
  [12  "offset metadata too large"]
  [13  "network exception"]
  [14  "coordinator load in progress"]
  [15  "coordinator not available"]
  [16  "not coordinator"]
  [17  "invalid topic exception"]
  [18  "record list too large"]
  [19  "not enough replicas"]
  [20  "not enough replicas after append"]
  [21  "invalid required acks"]
  [22  "illegal generation"]
  [23  "inconsistent group protocol"]
  [24  "invalid group id"]
  [25  "unknown member id"]
  [26  "invalid session timeout"]
  [27  "rebalance in progress"]
  [28  "invalid commit offset size"]
  [29  "topic authorization failed"]
  [30  "group authorization failed"]
  [31  "cluster authorization failed"]
  [32  "invalid timestamp"]
  [33  "unsupported SASL mechanism"]
  [34  "illegal SASL state"]
  [35  "unsupported version"]
  [36  "topic already exists"]
  [37  "invalid partitions"]
  [38  "invalid replication factor"]
  [39  "invalid replica assignment"]
  [40  "invalid config"]
  [41  "not controller"]
  [42  "invalid request"]
  [43  "unsupported for message format"]
  [44  "policy violation"]
  [45  "out of order sequence number"]
  [46  "duplicate sequence number"]
  [47  "invalid producer epoch"]
  [48  "invalid txn state"]
  [49  "invalid producer id mapping"]
  [50  "invalid transaction timeout"]
  [51  "concurrent transactions"]
  [52  "transaction coordinator fenced"]
  [53  "transaction id authorization failed"]
  [54  "security disabled"]
  [55  "operation not attempted"]
  [56  "kafka storage error"]
  [57  "log dir not found"]
  [58  "SASL authentication failed"]
  [59  "unknown producer id"]
  [60  "reassignment in progress"]
  [61  "delegation token authentication disabled"]
  [62  "delegation token not found"]
  [63  "delegation token owner mismatch"]
  [64  "delegation token request not allowed"]
  [65  "delegation token authorization failed"]
  [66  "delegation token expired"]
  [67  "invalid principal type"]
  [68  "non-empty group"]
  [69  "group id not found"]
  [70  "fetch session id not found"]
  [71  "invalid fetch session id epoch"]
  [72  "listener not found"]
  [73  "topic deletion disabled"]
  [74  "fenced leader epoch"]
  [75  "unknown leader epoch"]
  [76  "unsupported compression type"]
  [77  "stale broker epoch"]
  [78  "offset not available"]
  [79  "member id required"]
  [80  "preferred leader not available"]
  [81  "group max size reached"]
  [82  "fenced instance id"]
  [83  "eligibile leaders not available"]
  [84  "election not needed"]
  [85  "no reassignment in progress"]
  [86  "group subscribed to topic"]
  [87  "invalid record"]
  [88  "unstable offset commit"]
  [89  "throttling quota exceeded"]
  [90  "producer fenced"]
  [91  "resource not found"]
  [92  "duplicate resource"]
  [93  "unacceptable credential"]
  [94  "inconsistent voter set"]
  [95  "invalid update version"]
  [96  "feature update failed"]
  [97  "principal deserialization failure"]
  [98  "snapshot not found"]
  [99  "position out of range"]
  [100 "unknown topic id"]
  [101 "duplicate broker registration"]
  [102 "broker id not registered"]
  [103 "inconsistent topic id"]
  [104 "inconsistent clusetr id"]
  [105 "transaction id not found"]
  [106 "fetch session topic id error"])
