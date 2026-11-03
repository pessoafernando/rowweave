package io.rowweave.cluster;

import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.ops.join.JoinPhysicalStrategy;
import io.rowweave.plan.PlanGraph;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleInboundAuthorization;

import java.io.Serializable;
import java.util.List;

/**
 * The command that makes a master run one stage: built by the master, serialised over the control
 * plane RPC, and handed by {@link WorkerServer} to {@link StageTask} to execute on a thread inside
 * the worker's process.
 *
 * <p><b>Two-phase startup</b>: every role that listens, CONSUMER, KEYED_OP, SINK or JOIN_CONSUMER,
 * is started first, binds, and reports the port it actually got on stdout. The driver uses those
 * ports to fill in the target endpoints of the producers and routers and only then starts them.
 * Listening always precedes connecting, so a producer can never race a consumer that has not bound.
 *
 * <p>PRODUCER and CONSUMER are point to point over a single connection.
 * <p>ROUTER reads a Source, partitions by key and connects to P downstreams. KEYED_OP accepts M
 * connections, runs a plan fragment and connects onwards. SINK accepts M connections, runs the tail
 * chain, which may end in a total-order sort, and writes the Sink.
 * <p>JOIN_CONSUMER accepts two inbound edges, left and right, each bound separately and each
 * reporting its own port, runs the join under the strategy chosen explicitly, and connects onwards.
 */
public final class StageCommand implements Serializable {

    private static final long serialVersionUID = 10L; // source ownership routing context

    public enum Role { PRODUCER, CONSUMER, ROUTER, KEYED_OP, SINK, WHOLE_GRAPH, JOIN_CONSUMER, SOURCE_STAGE }

    private final Role role;
    private final DataSchema dataSchema;  // this stage's inbound row schema; for JOIN_CONSUMER, the schema of its left edge
    private final int window;  // row-level credit window, the same value on both sides

    // PRODUCER / ROUTER: the input this stage reads.
    private final Source input;
    private transient Source runtimeInput;
    // CONSUMER / SINK: where this stage writes, locally on its own worker.
    private final Sink output;
    private transient Sink runtimeOutput;
    // PRODUCER: the single target it sends to.
    private final String host;
    private final int port;
    // ROUTER: the P downstream endpoints in partition order, plus the fields partitioned by hash.
    private final List<ShuffleEndpoint> downstream;
    private final String[] keyFields;
    // ROUTER: a join routes on a numerically normalised key, so equal values on the two sides land
    // in the same partition; groupBy and distinct use the plain hash and leave this false.
    private final boolean numericNormalized;
    // KEYED_OP / SINK: connections accepted, which is M, the number of upstream router or keyed-op
    // instances; JOIN_CONSUMER: the connection count on its left inbound edge.
    private final int inboundConnections;
    // KEYED_OP: the plan fragment it runs, a placeholder source at index 0 followed by an operator
    // chain, plus the one endpoint it converges into downstream. PlanGraph is not Java-Serializable
    // and carries its own toBytes and fromBytes wire codec, so it travels as bytes and is rebuilt
    // on the worker side.
    private byte[] fragmentBytes;
    // Public-v1 whole-graph assignment carries only the checksummed PlanArtifact frame.
    private byte[] portableArtifactBytes;
    // Public-v1 distributed assignment selects one physical stage after Worker materialization.
    private PortableStageSpec portableStageSpec;
    private final ShuffleEndpoint sinkEndpoint;
    // SINK: the tail fragment it runs: placeholder source at index 0, an optional total-order sort,
    // then write the output.
    private byte[] sinkFragmentBytes;
    private transient volatile PlanGraph decodedFragment;
    private transient volatile PlanGraph decodedSinkFragment;
    private transient volatile PlanGraph portableRuntimeGraph;
    private transient java.util.function.LongConsumer residentReservation;
    private transient io.rowweave.plan.ObjectTrustContext objectTrustContext =
            io.rowweave.plan.ObjectTrustContext.localTrusted();
    // JOIN_CONSUMER: the right edge's schema and connection count, the join's output schema, the key
    // fields on both sides and the join type, which together rebuild the join.
    private final DataSchema rightMeta;
    private final int rightInboundConnections;
    private final DataSchema joinOutMeta;
    private final String[] rightKeyFields;
    private final String joinType;
    // The physical strategy carried explicitly at schema 2. A legacy Java or schema-1 command with
    // no field here maps only the original join to sort-merge.
    private JoinPhysicalStrategy joinStrategy;
    // A JOIN_CONSUMER also fans out using the grouping keys of the stage below, sharing the
    // downstream list. downKeys are that stage's grouping keys and downNormalized is true when it is
    // itself a join. They are kept apart from keyFields and numericNormalized, which are this join's
    // own left keys, so the two are never confused.
    private final String[] downKeys;
    private final boolean downNormalized;
    // Physical forward edges (for example mapPartition) route producer i to consumer i.
    // The boolean preserves legacy Java-deserialization defaults: absent fields mean normal keyed routing.
    private boolean downFixedRouting;
    private int downFixedPartition;
    // A user coGroup function, which is Serializable. When it is set, JOIN_CONSUMER rebuilds a
    // CoGroup instead of a join. It is assigned before the command is sent.
    private io.rowweave.CoGroupFunction coGroupFn;

    // ---- Same-worker local channels. These fields are additive: null or zero leaves the stage on
    // the plain socket path with exactly the behaviour it had before. ----
    // Producer side: the fan-out targets per partition, socket or local. When present they are used
    // instead of the endpoint list, in partition order, agreeing with how downKeys partitions.
    private java.util.List<ShuffleTarget> downTargets;
    // The broadcast groups of a fan-out stage: one row goes to every group, and each group carries
    // its own targets and partition keys. Null means one downstream, so the stage takes the
    // downTargets or downstream path unchanged; more than one group means broadcast. A group is the
    // instance set of one downstream stage.
    private java.util.List<BroadcastGroup> broadcastGroups;
    // Listener side: how many of this stage's inbound producers are on the same worker. The number
    // arriving over sockets is inboundConnections minus this.
    private int localInbound;  // local producers on edge 0
    private int localInboundRight;  // local producers on edge 1, the right inbound edge of a JOIN_CONSUMER
    private String localInboundKey;  // rendezvous key of the local inbound on edge 0: downstream registers, producer waits
    private String localInboundKeyRight;  // the rendezvous key of the local inbound on edge 1
    private boolean objectReuse = true;  // a local edge passes the row reference by default; false copies at the boundary
    // Mux routing: the key under which this stage's inbound is registered, a globally unique
    // identifier assigned by the master. Null means muxing is off and the stage binds and reports a
    // port of its own. Otherwise it does not bind, and registers with the worker's single-port
    // DataInboundRegistry under this key. Edge 0 is the single or left inbound of KEYED_OP, SINK and
    // JOIN_CONSUMER; edge 1 is the right inbound of JOIN_CONSUMER.
    private java.util.UUID inboundTargetId;
    private java.util.UUID inboundTargetIdRight;
    // The coordinated commit group of a job with several sinks; null keeps the single-sink close path.
    private String commitGroupId;
    // The stable commit identity the master binds for the whole group; used only by a manifest sink.
    private String manifestAttemptId;
    private long manifestGeneration;
    // The identity of this execution attempt. It is a different scope from the manifest attempt and
    // generation, and the two must never be substituted for each other.
    private RunAttemptFence runAttemptFence;
    private transient io.rowweave.run.SinkAttemptIdentity sinkAttemptIdentity;
    private String publicationId;
    private long publicationGeneration;
    private io.rowweave.run.SourceSplitAssignment.Context sourceSplitAssignment;
    // consumer-only digest projections; producer bearer remains in endpoints.
    private java.util.List<ShuffleInboundAuthorization> inboundAuthorizations;

    private StageCommand(Role role, DataSchema dataSchema, int window, Source input, Sink output,
                         String host, int port, List<ShuffleEndpoint> downstream, String[] keyFields,
                         boolean numericNormalized, int inboundConnections, byte[] fragmentBytes,
                         ShuffleEndpoint sinkEndpoint, byte[] sinkFragmentBytes,
                         DataSchema rightMeta, int rightInboundConnections, DataSchema joinOutMeta,
                         String[] rightKeyFields, String joinType,
                         String[] downKeys, boolean downNormalized) {
        this.role = role;
        this.dataSchema = dataSchema;
        this.window = window;
        this.input = input;
        this.output = output;
        this.host = host;
        this.port = port;
        this.downstream = downstream;
        this.keyFields = keyFields;
        this.numericNormalized = numericNormalized;
        this.inboundConnections = inboundConnections;
        this.fragmentBytes = fragmentBytes;
        this.sinkEndpoint = sinkEndpoint;
        this.sinkFragmentBytes = sinkFragmentBytes;
        this.rightMeta = rightMeta;
        this.rightInboundConnections = rightInboundConnections;
        this.joinOutMeta = joinOutMeta;
        this.rightKeyFields = rightKeyFields;
        this.joinType = joinType;
        this.downKeys = downKeys;
        this.downNormalized = downNormalized;
    }

    // ---- point to point producer and consumer ----

    public static StageCommand consumer(DataSchema dataSchema, int window, Sink output) {
        return new StageCommand(Role.CONSUMER, dataSchema, window, null, output, null, 0, null, null,
                false, 0, null, null, null, null, 0, null, null, null, null, false);
    }

    public static StageCommand producer(DataSchema dataSchema, int window, Source input, String host, int port) {
        return new StageCommand(Role.PRODUCER, dataSchema, window, input, null, host, port, null, null,
                false, 0, null, null, null, null, 0, null, null, null, null, false);
    }

    // ---- shuffle roles ----

    /**
     * ROUTER: reads input, partitions it by hashing keyFields, and connects to downstream in
     * partition order.
     */
    public static StageCommand router(DataSchema dataSchema, int window, Source input,
                                      String[] keyFields, List<ShuffleEndpoint> downstream) {
        return new StageCommand(Role.ROUTER, dataSchema, window, input, null, null, 0, downstream, keyFields,
                false, 0, null, null, null, null, 0, null, null, null, null, false);
    }

    /**
     * ROUTER for a join: partitions on a numerically normalised key, so equal values on the left and
     * right land in the same partition and the match does not depend on scale.
     */
    public static StageCommand routerNormalized(DataSchema dataSchema, int window, Source input,
                                                String[] keyFields, List<ShuffleEndpoint> downstream) {
        return new StageCommand(Role.ROUTER, dataSchema, window, input, null, null, 0, downstream, keyFields,
                true, 0, null, null, null, null, 0, null, null, null, null, false);
    }

    /**
     * KEYED_OP: accepts inboundConnections router connections, runs the keyed fragment and connects
     * to sinkEndpoint.
     */
    public static StageCommand keyedOp(DataSchema dataSchema, int window, int inboundConnections,
                                       PlanGraph fragment, ShuffleEndpoint sinkEndpoint) {
        return withDecodedFragment(new StageCommand(Role.KEYED_OP, dataSchema, window, null, null, null, 0, null, null,
                false, inboundConnections, null, sinkEndpoint, null, null, 0, null, null, null, null, false),
                fragment);
    }

    /**
     * KEYED_OP that fans out. It accepts inboundConnections, the instance count of the stage above,
     * runs the fragment and partitions its output by downKeys across downstream, the full instance
     * set of the stage below. When that list has one element everything is sent to it with no
     * partitioning; with more, M outputs are spread over N by the downstream's grouping keys. outMeta
     * is the schema of the rows this stage emits, which is what the inbound edge carries.
     */
    public static StageCommand keyedOpFanout(DataSchema inMeta, DataSchema outMeta, int window, int inboundConnections,
                                             PlanGraph fragment, List<ShuffleEndpoint> downstream,
                                             String[] downKeys, boolean normalized) {
        return withDecodedFragment(new StageCommand(Role.KEYED_OP, inMeta, window, null, null, null, 0, downstream, null,
                false, inboundConnections, null, null, null, null, 0, outMeta, null, null,
                downKeys, normalized), fragment);
    }

    /**
     * SINK: accepts inboundConnections keyed-op connections, runs the tail fragment, which may end in
     * a total-order sort, and writes output.
     */
    public static StageCommand sink(DataSchema dataSchema, int window, int inboundConnections,
                                    PlanGraph sinkFragment, Sink output) {
        return withDecodedSinkFragment(new StageCommand(Role.SINK, dataSchema, window, null, output, null, 0, null, null,
                false, inboundConnections, null, null, null, null, 0, null, null, null, null, false),
                sinkFragment);
    }

    /**
     * SOURCE_STAGE: reads input lazily, runs a fragment that is a placeholder source at index 0
     * followed by a pass-through chain, and connects to its single downstream, with no partitioning
     * at parallelism one. This is the source-headed stage of a general DAG.
     */
    public static StageCommand sourceStage(DataSchema inMeta, DataSchema outMeta, int window,
                                           Source input, PlanGraph fragment, ShuffleEndpoint downstream) {
        // Argument positions: role, dataSchema as the inbound schema, window, input, output, host,
        // port, downstream list, keyFields, numericNormalized, inboundConnections, fragmentBytes,
        // sinkEndpoint taking downstream, sinkFragmentBytes, rightMeta, rightInboundConnections,
        // joinOutMeta reusing outMeta, rightKeyFields and joinType.
        return withDecodedFragment(new StageCommand(Role.SOURCE_STAGE, inMeta, window, input, null, null, 0, null, null,
                false, 0, null, downstream, null, null, 0, outMeta, null, null, null, false), fragment);
    }

    /**
     * SOURCE_STAGE that fans out: reads input lazily, runs the fragment and partitions by downKeys
     * across downstream, every instance of the stage below. One element means everything goes there;
     * more means M outputs spread over N by the downstream's grouping keys.
     */
    public static StageCommand sourceStageFanout(DataSchema inMeta, DataSchema outMeta, int window, Source input,
                                                 PlanGraph fragment, List<ShuffleEndpoint> downstream,
                                                 String[] downKeys, boolean normalized) {
        return withDecodedFragment(new StageCommand(Role.SOURCE_STAGE, inMeta, window, input, null, null, 0, downstream, null,
                false, 0, null, null, null, null, 0, outMeta, null, null, downKeys, normalized), fragment);
    }

    /**
     * JOIN_CONSUMER: two inbound edges, the left accepting leftInbound connections and the right
     * accepting rightInbound, each bound separately and reporting its own port. leftKeys, rightKeys
     * and joinType rebuild the join, and the output connects to sinkEndpoint.
     */
    public static StageCommand joinConsumer(DataSchema leftMeta, DataSchema rightMeta, DataSchema joinOutMeta, int window,
                                            int leftInbound, int rightInbound,
                                            String[] leftKeys, String[] rightKeys, String joinType,
                                            ShuffleEndpoint sinkEndpoint) {
        StageCommand command = new StageCommand(Role.JOIN_CONSUMER, leftMeta, window, null, null, null, 0, null, leftKeys,
                false, leftInbound, null, sinkEndpoint, null, rightMeta, rightInbound, joinOutMeta,
                rightKeys, joinType, null, false);
        command.joinStrategy = joinType == null ? null : JoinPhysicalStrategy.SORT_MERGE;
        return command;
    }

    /** A join command built by the new factory carries its physical strategy explicitly. */
    public static StageCommand joinConsumer(DataSchema leftMeta, DataSchema rightMeta, DataSchema joinOutMeta, int window,
                                            int leftInbound, int rightInbound,
                                            String[] leftKeys, String[] rightKeys, String joinType,
                                            JoinPhysicalStrategy strategy, ShuffleEndpoint sinkEndpoint) {
        StageCommand command = joinConsumer(leftMeta, rightMeta, joinOutMeta, window,
                leftInbound, rightInbound, leftKeys, rightKeys, joinType, sinkEndpoint);
        command.joinStrategy = java.util.Objects.requireNonNull(strategy, "join strategy");
        return command;
    }

    /**
     * JOIN_CONSUMER that fans out. The two inbound edges carry the router instance counts of each
     * upstream, the join runs as a sort-merge, and its output is partitioned by downKeys, the grouping
     * keys of the stage below, across downstream, that stage's full instance set. One element means
     * everything goes there; more means M outputs spread over N. leftKeys and rightKeys are this
     * join's own keys, not the downstream's.
     */
    public static StageCommand joinConsumerFanout(DataSchema leftMeta, DataSchema rightMeta, DataSchema joinOutMeta, int window,
                                                  int leftInbound, int rightInbound,
                                                  String[] leftKeys, String[] rightKeys, String joinType,
                                                  List<ShuffleEndpoint> downstream, String[] downKeys, boolean downNorm) {
        StageCommand command = new StageCommand(Role.JOIN_CONSUMER, leftMeta, window, null, null, null, 0, downstream, leftKeys,
                false, leftInbound, null, null, null, rightMeta, rightInbound, joinOutMeta,
                rightKeys, joinType, downKeys, downNorm);
        command.joinStrategy = joinType == null ? null : JoinPhysicalStrategy.SORT_MERGE;
        return command;
    }

    /** Explicit physical strategy for a fan-out join command. */
    public static StageCommand joinConsumerFanout(DataSchema leftMeta, DataSchema rightMeta, DataSchema joinOutMeta, int window,
                                                  int leftInbound, int rightInbound,
                                                  String[] leftKeys, String[] rightKeys, String joinType,
                                                  JoinPhysicalStrategy strategy,
                                                  List<ShuffleEndpoint> downstream, String[] downKeys, boolean downNorm) {
        StageCommand command = joinConsumerFanout(leftMeta, rightMeta, joinOutMeta, window,
                leftInbound, rightInbound, leftKeys, rightKeys, joinType,
                downstream, downKeys, downNorm);
        command.joinStrategy = java.util.Objects.requireNonNull(strategy, "join strategy");
        return command;
    }

    /**
     * WHOLE_GRAPH, the simplest assignment, where one worker runs a complete SINK-rooted PlanGraph
     * through PushDagExecutor.runSinkGraph. There is no shuffle and no endpoint on another worker, so
     * nothing to wire by port, and the Sink lives inside the graph's own SINK node and is written
     * locally.
     */
    public static StageCommand wholeGraph(PlanGraph sinkGraph) {
        return withDecodedFragment(new StageCommand(Role.WHOLE_GRAPH, null, 0, null, null, null, 0, null, null,
                false, 0, null, null, null, null, 0, null, null, null, null, false), sinkGraph);
    }

    /** Public-v1 whole graph; runtime objects are materialized only on the admitted Worker. */
    public static StageCommand wholePortableArtifact(byte[] artifactBytes) {
        if (artifactBytes == null || artifactBytes.length == 0) {
            throw new IllegalArgumentException(
                    "portable artifact bytes are required");
        }
        StageCommand command = new StageCommand(
                Role.WHOLE_GRAPH, null, 0, null, null, null, 0,
                null, null, false, 0, null, null, null,
                null, 0, null, null, null, null, false);
        command.portableArtifactBytes = artifactBytes.clone();
        return command;
    }

    StageCommand withPortableStageArtifact(
            byte[] artifactBytes, PortableStageSpec stageSpec) {
        if (artifactBytes == null || artifactBytes.length == 0
                || stageSpec == null || role == Role.WHOLE_GRAPH
                || portableArtifactBytes != null || portableStageSpec != null) {
            throw new IllegalArgumentException(
                    "invalid portable stage artifact binding");
        }
        // Package-local Master path owns immutable request bytes until every
        // assignment has been encoded. Avoid an unaccounted artifact-sized
        // clone per physical task; the V2 envelope copy is resident-accounted.
        portableArtifactBytes = artifactBytes;
        portableStageSpec = stageSpec;
        return this;
    }

    private static StageCommand withDecodedFragment(StageCommand command, PlanGraph graph) {
        command.decodedFragment = graph;
        return command;
    }

    private static StageCommand withDecodedSinkFragment(StageCommand command, PlanGraph graph) {
        command.decodedSinkFragment = graph;
        return command;
    }

    public Role role() {
        return role;
    }

    public DataSchema dataSchema() {
        return dataSchema;
    }

    public int window() {
        return window;
    }

    public Source input() {
        return runtimeInput == null ? input : runtimeInput;
    }

    void attachIncrementalRuntimeInput(Source runtime) {
        boolean direct = input instanceof io.rowweave.IncrementalSourceSplit
                && runtime instanceof io.rowweave.BatchExecutableSource;
        boolean batch = input instanceof io.rowweave.AssignedSourceBatch
                && runtime instanceof io.rowweave.AssignedSourceBatch;
        if (!direct && !batch) {
            throw new IllegalArgumentException(
                    "incremental runtime input does not match frozen proxy");
        }
        if (runtimeInput != null && runtimeInput != runtime) {
            throw new IllegalStateException(
                    "incremental runtime input is already attached");
        }
        runtimeInput = runtime;
    }

    public Sink output() {
        return runtimeOutput == null ? output : runtimeOutput;
    }

    void attachProviderRuntimeOutput(ProviderSinkWriteSession runtime) {
        io.rowweave.ProviderBatchSink frozen = providerOutputProxy();
        PlanGraph wholeGraph = null;
        if (output == null && role == Role.WHOLE_GRAPH) {
            wholeGraph = fragment();
        }
        if (runtime == null || frozen == null
                || !runtime.matchesProxy(frozen)) {
            throw new IllegalArgumentException(
                    "provider runtime does not match frozen output proxy");
        }
        if (runtimeOutput != null && runtimeOutput != runtime) {
            throw new IllegalStateException(
                    "provider runtime output is already attached");
        }
        runtimeOutput = runtime;
        if (wholeGraph != null) {
            decodedFragment = wholeGraph.replaceRootSinkOutput(runtime);
        }
    }

    io.rowweave.ProviderBatchSink providerOutputProxy() {
        if (output instanceof io.rowweave.ProviderBatchSink) {
            return (io.rowweave.ProviderBatchSink) output;
        }
        PlanGraph graph = role == Role.WHOLE_GRAPH ? fragment() : null;
        io.rowweave.Sink root = graph == null
                || graph.root().kind() != io.rowweave.plan.PlanNode.Kind.SINK
                ? null : graph.root().output();
        return root instanceof io.rowweave.ProviderBatchSink
                ? (io.rowweave.ProviderBatchSink) root : null;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public List<ShuffleEndpoint> downstream() {
        return downstream;
    }

    public String[] keyFields() {
        return keyFields;
    }

    public boolean numericNormalized() {
        return numericNormalized;
    }

    public int inboundConnections() {
        return inboundConnections;
    }

    public PlanGraph fragment() {
        PlanGraph decoded = decodedFragment;
        if (decoded != null) return decoded;
        if (fragmentBytes == null) return null;
        if (decoded == null) {
            synchronized (this) {
                decoded = decodedFragment;
                if (decoded == null) decodedFragment = decoded = PlanGraph.fromBytes(
                        fragmentBytes, objectTrustContext(), residentReservation());
            }
        }
        return decoded;
    }

    public ShuffleEndpoint sinkEndpoint() {
        return sinkEndpoint;
    }

    public PlanGraph sinkFragment() {
        PlanGraph decoded = decodedSinkFragment;
        if (decoded != null) return decoded;
        if (sinkFragmentBytes == null) return null;
        if (decoded == null) {
            synchronized (this) {
                decoded = decodedSinkFragment;
                if (decoded == null) decodedSinkFragment = decoded = PlanGraph.fromBytes(
                        sinkFragmentBytes, objectTrustContext(), residentReservation());
            }
        }
        return decoded;
    }

    public DataSchema rightMeta() {
        return rightMeta;
    }

    public int rightInboundConnections() {
        return rightInboundConnections;
    }

    public DataSchema joinOutMeta() {
        return joinOutMeta;
    }

    public String[] rightKeyFields() {
        return rightKeyFields;
    }

    public String joinType() {
        return joinType;
    }

    /**
     * Effective physical strategy. A null raw field is accepted only for an old Java-serialized
     * non-CoGroup JOIN_CONSUMER, whose historical runtime was unconditionally SORT_MERGE.
     */
    public JoinPhysicalStrategy joinStrategy() {
        if (joinStrategy != null) return joinStrategy;
        return role == Role.JOIN_CONSUMER && joinType != null && coGroupFn == null
                ? JoinPhysicalStrategy.SORT_MERGE : null;
    }

    JoinPhysicalStrategy rawJoinStrategy() {
        return joinStrategy;
    }

    /** The user coGroup function; when it is set, JOIN_CONSUMER rebuilds a CoGroup, not a join. */
    public io.rowweave.CoGroupFunction coGroupFn() {
        return coGroupFn;
    }

    /** Sets the coGroup function before the command is sent. */
    public StageCommand withCoGroupFn(io.rowweave.CoGroupFunction fn) {
        this.coGroupFn = fn;
        return this;
    }

    /** The grouping keys of the downstream stage, used to partition the fan-out; not this join's keys. */
    public String[] downKeys() {
        return downKeys;
    }

    /**
     * Whether the fan-out partitions on a numerically normalised key, true when the downstream stage
     * is itself a join.
     */
    public boolean downNormalized() {
        return downNormalized;
    }

    /** Returns the fixed downstream partition, or {@code -1} for ordinary keyed routing. */
    public int downFixedPartition() {
        return downFixedRouting ? downFixedPartition : -1;
    }

    StageCommand withDownFixedPartition(
            int partition, int partitionCount) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException(
                    "fixed downstream partition is outside the consumer range");
        }
        downFixedRouting = true;
        downFixedPartition = partition;
        return this;
    }

    // ---- same-worker local channel accessors and fluent setters, additive over the legacy builders ----

    /**
     * Producer side: the fan-out targets per partition, socket or local. When present they take
     * priority over the endpoint list.
     */
    public java.util.List<ShuffleTarget> downTargets() { return downTargets; }
    public StageCommand withDownTargets(java.util.List<ShuffleTarget> targets) { this.downTargets = targets; return this; }

    /**
     * The broadcast group of one downstream stage: the fan-out targets of every instance of that
     * stage, the grouping keys used to partition into them, and whether that partitioning normalises
     * numerically. Serializable, because a StageCommand reaches the worker over the wire.
     */
    public static final class BroadcastGroup implements Serializable {
        private static final long serialVersionUID = 2L;
        public final java.util.List<ShuffleTarget> targets;
        public final String[] downKeys;
        public final boolean normalized;
        // The edge type. Blocking means the producer materialises the fork's output and this group
        // pushes from its own replay cursor on a thread of its own, immune to downstream
        // backpressure, which breaks the ring a fan-out that re-joins would otherwise form.
        // Pipelined streams as it always has, and the three-argument form keeps that default.
        public final boolean blocking;
        private boolean fixedRouting;
        private int fixedPartition;
        public BroadcastGroup(java.util.List<ShuffleTarget> targets, String[] downKeys, boolean normalized) {
            this(targets, downKeys, normalized, false);
        }
        public BroadcastGroup(java.util.List<ShuffleTarget> targets, String[] downKeys, boolean normalized,
                              boolean blocking) {
            this.targets = targets;
            this.downKeys = downKeys;
            this.normalized = normalized;
            this.blocking = blocking;
        }

        BroadcastGroup withFixedPartition(
                int partition, int partitionCount) {
            if (partition < 0 || partition >= partitionCount) {
                throw new IllegalArgumentException(
                        "fixed broadcast partition is outside the consumer range");
            }
            fixedRouting = true;
            fixedPartition = partition;
            return this;
        }

        int fixedPartition() {
            return fixedRouting ? fixedPartition : -1;
        }
    }

    /**
     * The broadcast groups. Null or one group keeps the single-downstream behaviour; more than one
     * broadcasts every row to all of them.
     */
    public java.util.List<BroadcastGroup> broadcastGroups() { return broadcastGroups; }
    public StageCommand withBroadcastGroups(java.util.List<BroadcastGroup> groups) { this.broadcastGroups = groups; return this; }

    /** Listener side: local producers on edge 0; the rest of inboundConnections arrive over sockets. */
    public int localInbound() { return localInbound; }
    /**
     * Listener side: the local inbound rendezvous key of edge 0, under which the downstream registers
     * its inbound channel and the upstream producer waits for it.
     */
    public String localInboundKey() { return localInboundKey; }
    public StageCommand withLocalInbound(int count, String key) {
        this.localInbound = count; this.localInboundKey = key; return this;
    }

    /** Local producers on edge 1, the right inbound edge of a JOIN_CONSUMER. */
    public int localInboundRight() { return localInboundRight; }
    /** The local inbound rendezvous key of edge 1, the right inbound edge of a JOIN_CONSUMER. */
    public String localInboundKeyRight() { return localInboundKeyRight; }
    public StageCommand withLocalInboundRight(int count, String key) {
        this.localInboundRight = count; this.localInboundKeyRight = key; return this;
    }

    /** Whether a local edge hands over the row reference directly, which is the default. */
    public boolean objectReuse() { return objectReuse; }
    public StageCommand withObjectReuse(boolean reuse) { this.objectReuse = reuse; return this; }

    /**
     * The mux routing key of this stage's inbound for edge 0, the single or left edge. Null means
     * muxing is off and the stage binds a port of its own.
     */
    public java.util.UUID inboundTargetId() { return inboundTargetId; }
    /**
     * The mux routing key of edge 1, the right inbound edge of a JOIN_CONSUMER. Null means muxing is
     * off.
     */
    public java.util.UUID inboundTargetIdRight() { return inboundTargetIdRight; }
    public StageCommand withInboundTargetId(java.util.UUID targetId) { this.inboundTargetId = targetId; return this; }
    public StageCommand withInboundTargetIds(java.util.UUID left, java.util.UUID right) {
        this.inboundTargetId = left; this.inboundTargetIdRight = right; return this;
    }

    public String commitGroupId() { return commitGroupId; }
    public StageCommand withCommitGroupId(String groupId) { this.commitGroupId = groupId; return this; }

    public String manifestAttemptId() { return manifestAttemptId; }
    public long manifestGeneration() { return manifestGeneration; }
    public StageCommand withManifestCommitContext(String attemptId, long generation) {
        if (attemptId == null || attemptId.isEmpty() || generation <= 0L) {
            throw new IllegalArgumentException("invalid manifest attempt/generation");
        }
        this.manifestAttemptId = attemptId;
        this.manifestGeneration = generation;
        return this;
    }

    public RunAttemptFence runAttemptFence() { return runAttemptFence; }

    public io.rowweave.run.SinkAttemptIdentity sinkAttemptIdentity() {
        return sinkAttemptIdentity;
    }

    public String publicationId() { return publicationId; }

    public long publicationGeneration() { return publicationGeneration; }

    public StageCommand withDurableSinkContext(
            io.rowweave.run.SinkAttemptIdentity identity,
            String publicationId, long publicationGeneration) {
        if (identity == null || publicationId == null || publicationId.isEmpty()
                || publicationGeneration <= 0L
                || role != Role.SINK && role != Role.WHOLE_GRAPH) {
            throw new IllegalArgumentException(
                    "invalid durable Sink action context");
        }
        if (runAttemptFence != null
                && !runAttemptFence.equals(identity.fence())) {
            throw new IllegalArgumentException(
                    "durable Sink identity fence mismatch");
        }
        io.rowweave.Sink sinkOutput = output;
        if (sinkOutput == null && role == Role.WHOLE_GRAPH
                && fragment() != null
                && fragment().root().kind() == io.rowweave.plan.PlanNode.Kind.SINK) {
            sinkOutput = fragment().root().output();
        }
        io.rowweave.BatchSinkDescriptor descriptor =
                sinkOutput instanceof io.rowweave.BatchSink
                ? ((io.rowweave.BatchSink) sinkOutput).batchSinkDescriptor()
                : null;
        if (descriptor == null
                || !java.security.MessageDigest.isEqual(
                identity.descriptorDigest(),
                io.rowweave.BatchSinkDescriptorCodec.digest(descriptor))) {
            throw new IllegalArgumentException(
                    "durable Sink descriptor identity mismatch");
        }
        if (sinkAttemptIdentity != null
                && !sinkAttemptIdentity.equals(identity)) {
            throw new IllegalStateException(
                    "durable Sink identity is already bound");
        }
        this.sinkAttemptIdentity = identity;
        this.publicationId = publicationId;
        this.publicationGeneration = publicationGeneration;
        return this;
    }

    public io.rowweave.run.SourceSplitAssignment.Context sourceSplitAssignment() {
        return sourceSplitAssignment;
    }

    public java.util.List<ShuffleInboundAuthorization> inboundAuthorizations() {
        return inboundAuthorizations;
    }

    public StageCommand withInboundAuthorizations(
            java.util.List<ShuffleInboundAuthorization> authorizations) {
        if (authorizations == null || authorizations.isEmpty()
                || authorizations.size() > 2) {
            throw new IllegalArgumentException(
                    "inbound authorizations must contain one or two targets");
        }
        if (role != Role.CONSUMER && role != Role.KEYED_OP && role != Role.SINK
                && role != Role.JOIN_CONSUMER) {
            throw new IllegalStateException(
                    "inbound authorization is invalid for role " + role);
        }
        java.util.HashSet<java.util.UUID> targets = new java.util.HashSet<>();
        for (ShuffleInboundAuthorization authorization : authorizations) {
            if (authorization == null || !targets.add(authorization.targetId())) {
                throw new IllegalArgumentException(
                        "inbound authorization target is null or duplicated");
            }
        }
        this.inboundAuthorizations = java.util.Collections.unmodifiableList(
                new java.util.ArrayList<>(authorizations));
        return this;
    }

    public StageCommand withSourceSplitAssignment(
            io.rowweave.run.SourceSplitAssignment assignment) {
        if (assignment == null) {
            throw new IllegalArgumentException("source split assignment is required");
        }
        if (role != Role.SOURCE_STAGE && role != Role.WHOLE_GRAPH) {
            throw new IllegalStateException(
                    "source split assignment is invalid for role " + role);
        }
        io.rowweave.run.SourceSplitAssignment.Context next = assignment.context();
        if (sourceSplitAssignment != null
                && !java.security.MessageDigest.isEqual(
                sourceSplitAssignment.ownershipDigest(), next.ownershipDigest())) {
            throw new IllegalStateException("stage command source ownership is already bound");
        }
        sourceSplitAssignment = next;
        return this;
    }

    /** Once bound, the fence cannot be changed; binding the same value again propagates idempotently. */
    public StageCommand withRunAttemptFence(RunAttemptFence fence) {
        if (fence == null) {
            throw new IllegalArgumentException("runAttemptFence is required");
        }
        if (runAttemptFence == null) {
            runAttemptFence = fence;
        } else if (!runAttemptFence.equals(fence)) {
            throw new IllegalStateException("stage command already belongs to another run attempt");
        }
        return this;
    }

    /** The bytes of the complete SINK-rooted graph a WHOLE_GRAPH runs, carried in the fragment field. */
    public byte[] wholeGraphBytes() {
        ensureLegacyPlanBytes();
        return fragmentBytes;
    }

    byte[] portableArtifactBytesUnsafe() {
        return portableArtifactBytes;
    }

    PortableStageSpec portableStageSpec() { return portableStageSpec; }

    void attachPortableRuntimeGraph(PlanGraph graph) {
        if (graph == null || portableRuntimeGraph != null) {
            throw new IllegalStateException(
                    "invalid portable runtime graph attachment");
        }
        portableRuntimeGraph = graph;
    }

    void releasePortableRuntimeGraph() { portableRuntimeGraph = null; }

    PlanGraph portableRuntimeGraph() {
        return portableRuntimeGraph == null ? fragment() : portableRuntimeGraph;
    }

    /**
     * The V2 writer encodes from the decoded PlanGraph and an explicit trust context. The legacy byte
     * array is produced only when a loopback Java envelope is actually serialised, so a factory that
     * still assumes local trust cannot wrongly reject a signed deployment class.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        if (portableArtifactBytes != null) {
            throw new java.io.NotSerializableException(
                    "portable StageCommand requires the explicit V2 envelope");
        }
        ensureLegacyPlanBytes();
        out.defaultWriteObject();
    }

    private synchronized void ensureLegacyPlanBytes() {
        if (fragmentBytes == null && decodedFragment != null) {
            fragmentBytes = decodedFragment.toBytes(
                    objectTrustContext(), residentReservation());
        }
        if (sinkFragmentBytes == null && decodedSinkFragment != null) {
            sinkFragmentBytes = decodedSinkFragment.toBytes(
                    objectTrustContext(), residentReservation());
        }
    }

    void attachResidentReservation(java.util.function.LongConsumer reservation) {
        if (reservation == null || residentReservation != null) {
            throw new IllegalStateException("invalid StageCommand resident reservation attachment");
        }
        residentReservation = reservation;
    }

    void attachObjectTrustContext(io.rowweave.plan.ObjectTrustContext trustContext) {
        if (trustContext == null) {
            throw new IllegalArgumentException("object trust context is required");
        }
        objectTrustContext = trustContext;
    }

    private io.rowweave.plan.ObjectTrustContext objectTrustContext() {
        io.rowweave.plan.ObjectTrustContext current = objectTrustContext;
        if (current == null) {
            throw new IllegalStateException("StageCommand object trust context is not attached");
        }
        return current;
    }

    private java.util.function.LongConsumer residentReservation() {
        java.util.function.LongConsumer current = residentReservation;
        return current == null ? ignored -> { } : current;
    }

    /** The routing fields carried explicitly by the V2 decoder, with no plan or object payload slice. */
    static final class WireFields {
        Role role;
        DataSchema dataSchema;
        int window;
        String host;
        int port;
        List<ShuffleEndpoint> downstream;
        String[] keyFields;
        boolean numericNormalized;
        int inboundConnections;
        ShuffleEndpoint sinkEndpoint;
        DataSchema rightMeta;
        int rightInboundConnections;
        DataSchema joinOutMeta;
        String[] rightKeyFields;
        String joinType;
        JoinPhysicalStrategy joinStrategy;
        String[] downKeys;
        boolean downNormalized;
        boolean downFixedRouting;
        int downFixedPartition;
        java.util.List<ShuffleTarget> downTargets;
        java.util.List<BroadcastGroup> broadcastGroups;
        int localInbound;
        int localInboundRight;
        String localInboundKey;
        String localInboundKeyRight;
        boolean objectReuse;
        java.util.UUID inboundTargetId;
        java.util.UUID inboundTargetIdRight;
        String commitGroupId;
        String manifestAttemptId;
        long manifestGeneration;
        RunAttemptFence runAttemptFence;
        io.rowweave.run.SinkAttemptIdentity sinkAttemptIdentity;
        String publicationId;
        long publicationGeneration;
        io.rowweave.run.SourceSplitAssignment.Context sourceSplitAssignment;
        java.util.List<ShuffleInboundAuthorization> inboundAuthorizations;
        int outputEncoding;
        PortableStageSpec portableStageSpec;
    }

    static StageCommand fromWire(WireFields fields, Source input, Sink output,
                                 io.rowweave.CoGroupFunction coGroupFn,
                                 PlanGraph fragment, PlanGraph sinkFragment) {
        StageCommand command = new StageCommand(
                fields.role, fields.dataSchema, fields.window, input, output,
                fields.host, fields.port, fields.downstream, fields.keyFields,
                fields.numericNormalized, fields.inboundConnections, null,
                fields.sinkEndpoint, null, fields.rightMeta,
                fields.rightInboundConnections, fields.joinOutMeta,
                fields.rightKeyFields, fields.joinType, fields.downKeys,
                fields.downNormalized);
        command.coGroupFn = coGroupFn;
        command.joinStrategy = fields.joinStrategy;
        command.downFixedRouting = fields.downFixedRouting;
        command.downFixedPartition = fields.downFixedPartition;
        command.downTargets = fields.downTargets;
        command.broadcastGroups = fields.broadcastGroups;
        command.localInbound = fields.localInbound;
        command.localInboundRight = fields.localInboundRight;
        command.localInboundKey = fields.localInboundKey;
        command.localInboundKeyRight = fields.localInboundKeyRight;
        command.objectReuse = fields.objectReuse;
        command.inboundTargetId = fields.inboundTargetId;
        command.inboundTargetIdRight = fields.inboundTargetIdRight;
        command.commitGroupId = fields.commitGroupId;
        command.manifestAttemptId = fields.manifestAttemptId;
        command.manifestGeneration = fields.manifestGeneration;
        command.runAttemptFence = fields.runAttemptFence;
        command.sinkAttemptIdentity = fields.sinkAttemptIdentity;
        command.publicationId = fields.publicationId;
        command.publicationGeneration = fields.publicationGeneration;
        command.sourceSplitAssignment = fields.sourceSplitAssignment;
        command.inboundAuthorizations = fields.inboundAuthorizations;
        command.portableStageSpec = fields.portableStageSpec;
        command.decodedFragment = fragment;
        command.decodedSinkFragment = sinkFragment;
        return command;
    }
}
