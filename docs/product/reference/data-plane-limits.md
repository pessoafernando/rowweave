---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Data plane and socket boundary: behaviour and certification limits

This page states what the `0.2.0` artifact actually does in the shuffle data
plane and at the operating-system socket boundary, and separates that from what
the test suite has *not* certified. Every claim points at a source line or a test
class in this repository. Nothing here is a throughput, latency or scale promise:
see [Roadmap](../../../ROADMAP.md) for open work and
[capability matrix](capability-matrix.md) for supported behaviour.

## 1. Physical connection model

One physical TCP connection per remote worker endpoint carries every logical
stream to that endpoint; credit, failure and end-of-stream are routed by target
id, and the socket is closed only after the last logical handle releases it
(`rowweave-core/src/main/java/io/rowweave/sched/net/MuxConnectionPool.java:50-51`,
`acquire` at `:342` and `:352`, `release` at `:520`). Logical streams are counted,
not capped (`:448`, `:744`). Each closed connection emits one aggregate
`ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME` record with protocol, connect time,
logical stream count, record count, wire bytes, active data nanos and lifetime
(`:1062-1069`); the field list and its interpretation are in
[troubleshooting](../operations/troubleshooting.md) and
[cluster architecture](../architecture/cluster.md).

On the control plane, RPC transports are cached per endpoint, evicted and
retried once on failure behind an endpoint-generation fence
(`io/rowweave/cluster/rpc/RpcTransportCache.java:16-17`, `:67`), and lane pools
round-robin over a bounded set of lanes with an explicit invalidate path
(`io/rowweave/cluster/rpc/RpcTransportLanePool.java:36`, `:40`).

Tests: `MuxConnectionPoolTest`, `MuxConnectionPoolFanoutTest`,
`MuxConnectionPoolLifecycleTest`, `RpcTransportCacheTest`,
`RpcTransportLanePoolTest`, `OutboundShuffleCohortRegistryTest`,
`DataInboundAuthorizationRegistryTest`, and across real processes
`MuxFanoutProcessCertificationTest`.

## 2. Flow control, credit and resource bounds

Wire credit is acquired per target before a staged write
(`MuxConnectionPool.java:680`, `:1117`, `:1167`) and returned outside the queue
lock once the consumer polls the record
(`io/rowweave/sched/MultiProducerChannel.java:33`). Receivers batch credit
replies at half the negotiated window, flush on an empty queue, and send
zero-credit keepalives on an idle connection
(`io/rowweave/sched/net/MultiSocketChannelSource.java:45-50`, `:293`). Outbound
staging is clamped between 4 KiB and 64 KiB regardless of the requested window
(`MuxConnectionPool.java:762`), and a new physical connection must pass an
admission deadline or an interrupt before it handshakes (`:763`).

Ingress is bounded the same way: concurrent handshakes are gated by a fair
semaphore (`io/rowweave/sched/net/DataPlaneHandshakeAdmission.java:211`, `:222`,
`:254`) whose default is four (`io/rowweave/cluster/DataPlaneConfig.java:23`,
`:187`), and the default per-stage ingress credit is sized so a legitimate sender
cannot hit a frame cap before credit
(`io/rowweave/sched/net/ShuffleIngressAdmissionConfig.java:18`). Thread and
connection budgets per process are arithmetic, not estimates
(`io/rowweave/cluster/ProcessThreadCapacityModel.java:46`, `:68`, `:75`, `:80`,
`:88`), a shared connection closes its physical socket once the last logical
handle on it is released (`MuxConnectionPool.java:51`, `:556`). On the
non-authenticated path that release is purely reference counted and waits for
nothing else; an authenticated cohort additionally keeps one reference of its
own (`:444`), so its socket stays open until credit drains, as described in the
next paragraph. What is certified per target on a shared connection is that a
returned credit wakes only the stream whose consumer drained it
(`rowweave-core/src/test/java/io/rowweave/sched/net/MuxConnectionPoolFanoutTest.java`),
and the thread-creation inventory test fails if a new `Thread` appears in
production code without being accounted for
(`io/rowweave/cluster/ThreadCreationInventoryArchitectureTest.java:19`, `:56`).

The retirement gate mentioned above is reachable only under the `MTLS` data
plane: an authenticated shuffle cohort holds its connection until every byte of
outstanding credit is back (`MuxConnectionPool.java:542`, `:570`, released via
`:547` and `:584`, closed from the credit callback at `:588-601`). No test in
this release covers that gate, and the wire-credit upper bound
(`availableBytes <= window`) has no test-observable hook either
(`MuxConnectionPool.java:1156-1161`), so both are listed below as open
verification work rather than certified behaviour.

A failure on any connection of a multi-connection source closes all of them, so
every producer credit thread observes the poison instead of parking forever
(`MultiSocketChannelSource.java:268-269`).

### 2.1 Published data-plane ceilings

The values above are configuration defaults or fixed constants, each declared in
one place. They are the bounds a deployment can size against; the property names
are documented in [configuration](configuration.md).

| Bound | Value | Declared at |
|---|---:|---|
| Concurrent handshakes (default) | 4 | `io/rowweave/cluster/DataPlaneConfig.java:23` |
| Handshake timeout (default) | 10000 ms | `io/rowweave/cluster/DataPlaneConfig.java:24` |
| Connect timeout (default) | 10000 ms | `io/rowweave/cluster/DataPlaneConfig.java:25` |
| Handshake resident budget (default) | 24 MiB | `io/rowweave/cluster/DataPlaneConfig.java:26` |
| Authentication frame | fixed 1024 bytes | `io/rowweave/cluster/DataPlaneConfig.java:27` |
| Outbound staging clamp | 4 KiB to 64 KiB | `io/rowweave/sched/net/MuxConnectionPool.java:762` |
| Curator session thread reserve | 16 | `io/rowweave/cluster/ProcessThreadCapacityModel.java:15` |
| Stream row frame | 4 MiB | `io/rowweave/stream/api/StreamDataRowCodec.java:8` |

The transport switch itself is a single property, `rowweave.shuffle.mux`,
defaulting to enabled
(`io/rowweave/cluster/ShuffleTransportConfig.java:8`, `:18-19`). A property
named `rowweave.shuffle.securityProfile` is forbidden rather than configured:
the data plane inherits the admitted control identity
(`io/rowweave/cluster/DataPlaneConfig.java:21`).

## 3. Fan-out: what bounds logical streams per connection

The pool itself imposes no upper bound. The number of logical streams sharing one
connection equals the number of remote targets the compiled plan routes to that
endpoint; that set is fixed by the grant projection, which refuses a plan whose
remote route closure is not fully consumed
(`io/rowweave/cluster/ShuffleGrantPlan.java:129`), and each stream is separately
authorized and credit-accounted on the receiving worker
(`io/rowweave/sched/net/DataInboundAuthorizationRegistry.java:63-68`, `:183`).

A multi-process gate on one host with two worker JVMs asserts **at least four
logical streams on one physical connection** and fails if the measured value is
lower; the reference run reports eight streams on one connection with two
connections in the whole job (parallelism 8 with two source splits gives four
remote targets per worker). The gate reads `logicalStreams` from the connection
lifetime record instead of inferring it
(`rowweave-core/src/test/java/io/rowweave/cluster/MuxFanoutProcessCertificationTest.java`),
and caps the number of connections so reuse must be what carries the extra
streams: connection count therefore does not scale with target count. The eight
and two figures describe that run, not the bound the suite enforces.

One placement rule explains why earlier profiles saw fewer streams per
connection: the terminal stage of a graph always converges to a single instance
(`io/rowweave/cluster/StageAssignment.java:86-88`), so a keyed shuffle chained
straight into an output runs as one instance and multiplexes nothing. The
measured eight-stream figure comes from a graph that keeps a stage boundary after
the shuffle. The multi-process mutual-TLS profile in the
[capability matrix](capability-matrix.md) still reports two logical streams per
connection, and cross-host fan-out is not certified at all.

## 4. Task assignment concurrency

Assignment is parallel by default: `rowweave.cluster.assign.parallelism` is four
and a non-positive value is rejected at startup
(`io/rowweave/cluster/MasterServer.java:533-535`), with a stage-assignment
dispatcher, a dedicated assignment lane pool and one long-lived control
connection per worker rather than a connection per assign.

The concurrency boundary is a fair read-write lock used as a *claim* fence
(`MasterServer.java:377-379`): concurrent assignments hold the read lock together
and overlap freely. Two consequences are worth stating precisely.

* In the ephemeral assignment path the read lock is held until the `ASSIGN`
  acknowledgement is validated (`:11954` through `:11986`), so a stalled worker
  keeps a read lock until its own assignment deadline fires; readers do not block
  each other.
* In the whole-graph path the read lock is released before the job result is
  awaited (`:12458`, `:12494`, `:12496`).

Cancellation and run-deadline evaluation take the write lock of the sibling
dispatch fence (`:1895`, `:1937`), and `close()` takes the write lock of the
assignment fence (`:12604-12605`). Those are deliberate linearization points, not
missed parallelism: a cancel excludes new claims, and a closing master waits for
assignment claims already in flight.

Tests: `StageAssignmentDispatcherTest`, `MasterAssignmentWavePerformanceTest`,
`MasterMuxFullBatchTest`, `MasterDynamicWorkerLivenessTest`,
`ProcessThreadCapacityModelTest`.

## 5. Socket lifecycle at the operating-system boundary

**Orderly shutdown is a half-close, not a `close()`.** The sink flushes
end-of-stream, calls `shutdownOutput()` so the peer sees `FIN`, waits for the peer
to drain and close, then closes; a peer that already closed after receiving the
sentinel is treated as benign, while a timeout is still a failure
(`io/rowweave/sched/net/SocketChannelSink.java:466`, `:479-482`, `:514`). The mux
connection does the same (`MuxConnectionPool.java:1043`).

**Mid-stream failure may surface as `RST`, and that is covered by a contract.**
The receiver must observe an error — either an explicit error frame or the
exception from the reset — and must never treat a truncated stream as a normal
end of stream, because a silently short shuffle would report a successful job
(`io/rowweave/sched/net/SocketFailPathInjectionTest.java:19-24`, `:43`, `:63`).
Physical-connection loss poisons every logical stream on it
(`MuxConnectionPool.java:843`, `:930`, `:986`, `:1290`).

**Idle detection is deliberately asymmetric.** A reader-idle timer is armed
where the reading thread never blocks: the sink's credit reader arms one when
`soTimeoutMs > 0` and poisons the stream on expiry
(`io/rowweave/sched/net/SocketChannelSink.java:133-134`, `:574`), and the mux
data pipeline does the same for both plain and TLS-wrapped pipelines
(`MuxConnectionPool.java:870-872`, `:880-882`, `:1297`). The single-connection
receiver deliberately does not: its handler runs on its own executor and parks in
`queue.send()` when the queue is full, which pauses auto-read, so bytes already
sitting in the socket buffer would not reset the timer and a reader-idle timeout
would fire on a healthy connection. Dead senders are detected through `FIN`/`RST`
and `channelInactive`, plus the symmetric sender-side credit-read timeout
(`io/rowweave/sched/net/SocketChannelSource.java:110-114`). Both halves of that
choice are tested: a zombie peer releases a parked producer through the
sender-side timeout (`io/rowweave/sched/net/SocketChannelKeepaliveTest.java:40`),
and the same topology under slow backpressure is not falsely killed
(`SocketChannelKeepaliveTest.java:75`). `soTimeoutMs` on the receiver class is
therefore a reserved parameter: it does not arm a read timeout and does
not drive the keepalive schedule, which is controlled by `keepaliveMs`
(`SocketChannelSource.java:275-281`).

**`InputStream.available()` is not a portability surface here.** Every use of it
in shipped code reads from a `ByteArrayInputStream` that already holds a complete
frame, where the semantics are exact and identical on every operating system
(`io/rowweave/BatchSourceSplitDescriptorCodec.java:75`, `:94`, `:125`;
`io/rowweave/plan/PlanGraph.java:620-622`;
`io/rowweave/plan/TrustedObjectInputStream.java:112`, `:127`;
`io/rowweave/connector/postgres/PostgresCodecSupport.java:64-65`, `:79`;
`io/rowweave/connector/s3/S3CodecSupport.java:76`). No shuffle, RPC or connector
data path calls `available()` on a socket stream.

**Only one shipped decision is operating-system conditional**, and it is a TLS
build-identity gate, not socket behaviour
(`io/rowweave/cluster/CertifiedTlsBuildMatrixTest.java:21-29`). Continuous
integration runs the unit and multi-process cluster suites on both Linux and
macOS as a guardrail, but there is no versioned per-operating-system behaviour
matrix yet; that is open work.

## 6. Transport selection

`rowweave.shuffle.mux` defaults to **enabled**, and setting it to `false` is the
rollback to one port per stage (`io/rowweave/cluster/ShuffleTransportConfig.java:8`,
`:14`, `:18-19`). Under `MTLS`, mux acquisition requires an authenticated
endpoint sharing the negotiated TLS context, plus local identity and admission
(`MuxConnectionPool.java:214`, `:346`).

## 7. Certified versus uncertified

| Area | Certified by in this release | Not certified |
|---|---|---|
| Connection reuse | single-host multi-process suite enforces at least four logical streams on one connection from the measured lifetime record (reference run: 8 streams, 2 connections job-wide) | reuse across hosts, or under loss, reordering and asymmetric bandwidth |
| Flow control | per-target credit whose return wakes only the drained stream, batched replies, keepalive, admission deadline, frame-cap preflight, thread arithmetic | credit behaviour across a real network with loss or reordering; the `MTLS` cohort's credit-drain retirement gate; the `availableBytes <= window` credit upper bound |
| Assignment | parallel dispatch, claim fence semantics, cancel/close linearization | assignment throughput on heterogeneous hardware |
| Socket lifecycle | `FIN` half-close ordering, `RST`-versus-error contract, idle-timeout placement, close-return completeness | per-operating-system socket option differences |
| Transport choice | mux on by default in the tested suite; legacy path stays reachable | mux across hosts; legacy path in long runs |

The cluster suite runs several JVMs on one host over loopback and real sockets.
Those numbers are same-host regression profiles. They support no cross-host
throughput or latency claim, and no claim about network partitions, packet loss
or asymmetric bandwidth. Open work is tracked in
[Roadmap](../../../ROADMAP.md).
