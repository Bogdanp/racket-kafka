#lang scribble/manual

@(require (for-label kafka
                     kafka/consumer
                     kafka/producer
                     openssl
                     racket/base
                     racket/contract/base
                     racket/string
                     sasl))

@title{Apache Kafka}
@author[(author+email "Bogdan Popa" "bogdan@defn.io")]

This package provides a client for Apache Kafka versions 0.11 and up.
It is a work in progress, so expect breaking changes.

@section{Client}
@defmodule[kafka]

@deftech{Clients} transparently pool connections to brokers within a
cluster.  Connections are leased from the pool in order of least
in-progress requests.  Reconnections are handled transparently, and
connection errors bubble up to the caller.  Despite being thread-safe,
clients may not be shared between @tech{consumers}.

@defproc[(client? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{client}.
}

@defproc[(make-client [#:id id non-empty-string? "racket-kafka"]
                      [#:bootstrap-host host string? "127.0.0.1"]
                      [#:bootstrap-port port (integer-in 0 65535) 9092]
                      [#:sasl-mechanism&ctx sasl-ctx (or/c #f
                                                           (list/c 'plain string?)
                                                           (list/c symbol? sasl-ctx-proc/c)) #f]
                      [#:ssl-ctx ssl-ctx (or/c #f ssl-client-context?) #f]
                      [#:proxy proxy (or/c #f proxy?) #f]) client?]{

  Connects to a Kafka cluster via the server at @racket[host] and
  @racket[port] and returns a @tech{client}.

  When a @racket[sasl-ctx] is provided, it is used to authenticate the
  connection to the bootstrap host as well as any subsequent
  connections made to other nodes in the cluster.

  When an @racket[ssl-ctx] is provided, it is used to encrypt all
  connections.
}

@defproc[(disconnect-all [c client?]) void?]{
  Closes all open connections owned by @racket[c].
}

@defthing[sasl-ctx-proc/c (-> string? (integer-in 0 65535) sasl-ctx?)]{
  The contract for SASL context factories.  The first argument is the
  host being authenticated against and the second is the port.  See
  @filepath{example/amazon-msk-auth.rkt} for an example.
}

@subsection{Proxies}

@deftech{Proxies} act as intermediaries between clients and brokers.

@defproc[(proxy? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{proxy}.
}

@defproc[(make-http-proxy [host string?]
                          [port (integer-in 1 65535)]) proxy?]{
  Returns a proxy that connects to the given host and port using the
  @tt{HTTP/1.1 CONNECT} protocol.
}

@subsection{Errors}

@deftogether[(
  @defproc[(exn:fail:kafka? [v any/c]) boolean?]
  @defproc[(exn:fail:kafka:client? [v any/c]) boolean?]
  @defproc[(exn:fail:kafka:server? [v any/c]) boolean?]
)]{
  Predicates for the various kinds of errors that may be raised.
}

@subsection{Topic Management}

@defproc[(create-topics [c client?]
                        [t CreateTopic?] ...+) CreatedTopics?]{

  Creates the given topics on the broker if they don't already exist.

  When given a set of topics, some of them may succeed, and some may
  fail.  It's up to the caller to inspect the error codes on the
  returned @racket[CreatedTopic]s.
}

@defproc[(delete-topics [c client?]
                        [t string?] ...+) DeletedTopics?]{

  Deletes the given set of topics if they exist.
}

@deftogether[(
  @defstruct[CreateTopic ([name string?]
                          [partitions exact-positive-integer?])
                         #:omit-constructor]
  @defproc[(make-CreateTopic [#:name name string?]
                             [#:partitions partitions exact-positive-integer?]
                             [#:replication-factor factor (or/c -1 exact-positive-integer?) -1]
                             [#:assignments assignments (hash/c exact-nonnegative-integer? (listof exact-nonnegative-integer?)) (hasheqv)]
                             [#:configs configs (hash/c string? string?) (hash)]) CreateTopic?]
)]{
  Structs representing new topic configuration to be passed to
  @racket[create-topics].
}

@deftogether[(
  @defstruct[CreatedTopics ([topics (listof CreatedTopic?)]) #:omit-constructor]
  @defstruct[CreatedTopic ([error-code exact-nonnegative-integer?]
                           [error-message (or/c #f string?)]
                           [name string?])
                          #:omit-constructor]
)]{
  Structs representing the results of calling @racket[create-topics].
}

@deftogether[(
  @defstruct[DeletedTopics ([throttle-time-ms (or/c exact-nonnegative-integer?)]
                            [topics (listof DeletedTopic?)]
                            [tags (or/c #f tags/c)])
                           #:omit-constructor]
  @defstruct[DeletedTopic ([error-code error-code/c]
                           [error-message (or/c #f string?)]
                           [name string?]
                           [uuid (or/c #f bytes?)]
                           [tags (or/c #f tags/c)])
                          #:omit-constructor]
)]{
  Structs representing the results of calling @racket[delete-topics].
}

@subsection{Record Results}

@deftech{Record results} represent the results of publishing
individual records.

@defstruct[RecordResult ([topic string?]
                         [partition ProduceResponsePartition?])
           #:omit-constructor]{

  Represents a @tech{record result}.
}

@defstruct[ProduceResponsePartition ([id exact-nonnegative-integer?]
                                     [error-code exact-nonnegative-integer?]
                                     [offset exact-nonnegative-integer?])
           #:omit-constructor]{

  Details about the partition a record was published to.  If the
  @racket[error-code] is non-zero, there was an error and the record
  was not published.
}

@subsection{Contracts}

@defthing[error-code/c exact-nonnegative-integer?]{
  The contract for error codes.
}

@defthing[tags/c hash?]{
  The contract for tags.
}


@section{Consumer}
@defmodule[kafka/consumer]

@deftech{Consumers} form @tech{consumer groups} to subscribe to topics
and retrieve records.  As the name implies, @deftech{consumer groups}
group consumers together so that topic partitions may be spread out
across the members of the group.

Consumers are not thread-safe.

@defproc[(consumer? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{consumer}.
}

@defproc[(make-consumer [client client?]
                        [group-id string?]
                        [topic string?] ...+
                        [#:reset-strategy strategy (or/c 'earliest 'latest) 'earliest]
                        [#:session-timeout-ms session-timeout-ms exact-nonnegative-integer? 30000]) consumer?]{

  Creates a new @tech{consumer}.  The new consumer joins the
  @tech{consumer group} named @racket[group-id] and subscribes to the
  given set of @racket[topic]s.  If there are any existing consumers
  in the joined group, this may trigger a group rebalance.  Should the
  consumer be picked as the leader by the group coordinator, it
  handles assigning topics & partitions to all of the members of the
  group.

  The @racket[#:reset-strategy] argument controls what the consumer's
  initial offsets for newly-assigned partitions are going to be.  When
  this value is @racket['earliest], the consumer will receive records
  starting from the beginning of each partition.  When this value is
  @racket['latest], it will receive records starting from the time
  that it subscribes to each topic.
}

@(define sync-evt
  (tech #:doc '(lib "scribblings/reference/reference.scrbl") "synchronizable event"))

@defproc[(consume-evt [c consumer?]
                      [timeout-ms exact-nonnegative-integer? 1000])
         (evt/c
          (or/c
           (values 'rebalance (hash/c string? (hash/c integer? integer?)))
           (values 'records (vectorof record?))))]{

  Returns a @sync-evt that represents the result of consuming data
  from the topics @racket[c] is subscribed to.  The synchronization
  result is a pair of values representing the result type and its
  associated data.

  When a consumer leaves or joins the @tech{consumer group}, the event
  will synchronize to a @racket['rebalance] result.  In that case, the
  consumer will automatically re-join the group and discard any
  un-committed offsets.  The associated data is a hash from topic
  names to hashes of partition ids to offsets.  When a rebalance
  happens, you must take care not to commit any old offsets (i.e. you
  must issue a new @racket[consume-evt] before making any calls to
  @racket[consumer-commit]).

  When either the timeout passes or new records become available on
  the broker, the event will synchronize to a @racket['records] result
  whose associated data will be a vector of @tech{records}.

  More result types may be added in the future.

  The @racket[timeout-ms] argument controls how long the server-side
  may wait before returning a response.  If there are no records in
  between the time this function is called and when the timeout
  passes, an empty vector or records will be returned.  The other end
  may not necessarily respect the timeout value, and may return
  immediately when there are no more records.
}

@defproc[(consumer-commit [c consumer?]) void?]{
  Commits the topic-partition offsets consumed so far.

  Call this function after you have successfully processed a batch of
  records received from @racket[consume-evt].  If you forget to call
  this function, or if the consumer crashes in between calling
  @racket[consume-evt] and calling this function, another consumer in
  the group will eventually receive that same batch again.
}

@defproc[(consumer-stop [c consumer?]) void?]{
  Gracefully stops the consumer and removes it from its consumer
  group.  The consumer may not be used after this point.
}

@subsection{Records}

@deftech{Records} represent individual key-value pairs on a topic.

@defproc[(record? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{record}.
}

@defproc[(record-partition-id [r record?]) exact-nonnegative-integer?]{
  Returns the id of the partition the record belongs to.
}

@defproc[(record-offset [r record?]) exact-nonnegative-integer?]{
  Returns the record's partition offset.
}

@defproc[(record-key [r record?]) (or/c #f bytes?)]{
  Returns the record's key.
}

@defproc[(record-value [r record?]) bytes?]{
  Returns the record's value.
}

@defproc[(record-headers [r record?]) (hash/c string? bytes?)]{
  Returns the record's headers.
}

@subsection[#:tag "consumer-limitations"]{Limitations}

Consumers have several limitations at the moment, some of which will
be addressed in future versions.

@subsubsection[#:tag "consumer-limitations-compression"]{Compression}

At the moment, only @racket['gzip] is supported when producing records
and @racket['gzip], @racket['lz4], @racket['snappy] and @racket['zstd]
are supported when consuming records.  Fetching a batch of records
that is compressed using any other method will fail silently.

@subsubsection[#:tag "conusmer-limitations-assignment"]{Group Assignment}

@(define client-side-assignment-proposal
  (link "https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal"
        "client-side assignment"))

Only brokers that implement @client-side-assignment-proposal are
supported (Apache Kafka versions 0.11 and up).  At the moment, only
the range and round-robin group assignment strategies are implemented.

@subsubsection[#:tag "consumer-limitations-err-detection"]{Error Detection}

Batches retrieved from the broker contain a CRC code for error
detection, but the library does not validate these at the moment.


@section{Producer}
@defmodule[kafka/producer]

@deftech{Producers} publish data to one or more topics.  They batch
data internally by topic & partition, and they are thread-safe.

@defproc[(producer? [v any/c]) boolean?]{
  Returns @racket[#t] when @racket[v] is a @tech{producer}.
}

@defproc[(make-producer [c client?]
                        [#:acks acks (or/c 'none 'leader 'full) 'leader]
                        [#:compression compression (or/c 'none 'gzip) 'gzip]
                        [#:flush-interval interval exact-positive-integer? 60000]
                        [#:max-batch-bytes max-bytes exact-positive-integer? (* 100 1024 1024)]
                        [#:max-batch-size max-size exact-positive-integer? 1000]) producer?]{

  Returns a @tech{producer}.

  Data is batched internally by topic & partition.  Within each batch,
  the data is compressed according to the @racket[#:compression]
  method.

  The producer automatically flushes its data every
  @racket[#:flush-interval] milliseconds, whenever the total size of
  all its batches exceeds @racket[#:max-batch-bytes], or whenever the
  total number of records contained in all of its batches exceeds
  @racket[#:max-batch-size], whichever condition is true first.

  During a flush, calling @racket[produce] on a producer blocks until
  the flush completes.
}

@defproc[(produce [p producer?]
                  [topic string?]
                  [key (or/c #f bytes?)]
                  [value (or/c #f bytes?)]
                  [#:partition partition exact-nonnegative-integer? 0]
                  [#:headers headers (hash/c string? (or/c #f bytes?)) (hash)]) evt?]{

  Returns a @sync-evt that is ready for synchronization after a new
  record has been written to the @racket[partition] belonging to
  @racket[topic].  The event's synchronization result will be a
  @tech{record result}.

  Typically, you would call this function in a loop to produce a set
  of data, collect the results then @racket[sync] them to ensure
  they've been written to the log.

  Each result event may only be synced at most once.
}

@defproc[(producer-flush [p producer?]) void?]{
  Flushes any pending batches in @racket[p].
}

@defproc[(producer-stop [p producer?]) void?]{
  Gracefully stops @racket[p] after flushing any pending data to the
  broker.  The producer may no longer be used after this is called.
}

@subsection[#:tag "producer-limitations"]{Limitations}
@subsubsection[#:tag "producer-compression"]{Compression}

Kafka supports @tt{snappy}, @tt{lz4}, and @tt{zstd} compression in
addition to @tt{gzip}, but this library only supports @tt{gzip} at the
moment.
