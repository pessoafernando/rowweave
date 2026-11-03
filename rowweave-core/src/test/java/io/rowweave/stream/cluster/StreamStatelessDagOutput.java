package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamOutputTagDescriptor;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.contract.StreamWindowBinding;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelOutput;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only legacy graph harness retained for focused envelope/gate tests. */
final class StreamStatelessDagOutput implements StreamChannelOutput {
    private static final byte[] UNION_STATE_MAGIC =
            "RWXXUG01".getBytes(StandardCharsets.US_ASCII);
    private static final int UNION_STATE_VERSION = 1;
    private static final String UNION_STATE_NAME = "union-input-gate";
    private static final String UNION_STATE_CODEC =
            "rowweave.stream.union-input-gate-state.v1";
    private static final long CHECKPOINT_WAIT_MILLIS = 30_000L;

    private final StreamExecutionGraph graph;
    private final List<PortableStreamNode> sources;
    private final int maximumFrameBytes;
    private final Map<OutputPort, RowSerializer> producerSerializers;
    private final Set<OutputPort> upsertPorts;
    private final Map<OutputPort, List<StreamOperatorTask>> tasksByInput;
    private final Map<OutputPort, List<UnionInput>> unionsByInput;
    private final Map<OutputPort, List<Route>> routesByInput;
    private final Map<String, List<OutputPort>> portsByProducer;
    private final List<Route> routes;
    private final List<UnionGate> unionGates;
    private final Map<OutputPort, Integer> outputCounts;
    private final List<StreamOperatorTask> operatorTasks;
    private final FileCheckpointPayloadStore checkpointPayloadStore;
    private final Map<String, StreamCheckpointMember> checkpointMembers =
            new LinkedHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final Thread idleWatchdog;
    private CheckpointIdentity checkpointIdentity;

    StreamStatelessDagOutput(StreamExecutionGraph graph,
            Map<String, StreamChannelOutput> outputs,
            StreamFunctionResolver resolver) {
        this(graph, outputs, resolver, null, Collections.emptyList(),
                Collections.emptyList());
    }

    StreamStatelessDagOutput(StreamExecutionGraph graph,
            Map<String, ? extends StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers) {
        this(graph, outputs, resolver, checkpointPayloadStore,
                restoreMembers, Collections.emptyList());
    }

    StreamStatelessDagOutput(StreamExecutionGraph graph,
            Map<String, ? extends StreamChannelOutput> outputs,
            StreamFunctionResolver resolver,
            FileCheckpointPayloadStore checkpointPayloadStore,
            List<StreamCheckpointMember> restoreMembers,
            List<StreamDagBoundaryRoute> boundaryRoutes) {
        if (graph == null || outputs == null || resolver == null
                || restoreMembers == null || boundaryRoutes == null) {
            throw new IllegalArgumentException(
                    "Stream stateless DAG runtime is incomplete");
        }
        PortableStreamPlan plan = graph.portableJob().plan();
        List<SinkRoute> sinks = sinkRoutes(graph);
        if (outputs.size() != sinks.size()) {
            throw new IllegalArgumentException(
                    "Stream stateless DAG Sink routes are incomplete");
        }
        this.graph = graph;
        this.sources = sourceNodes(graph);
        this.maximumFrameBytes = maximumFrameBytes(plan);
        this.checkpointPayloadStore = checkpointPayloadStore;
        Map<OutputPort, RowSerializer> serializers = new LinkedHashMap<>();
        for (PortableStreamNode node : plan.nodes()) {
            if (node.opcode()
                    == io.rowweave.stream.contract.StreamOperatorCode.SINK) {
                continue;
            }
            serializers.put(new OutputPort(node.operatorUid(), null),
                    new RowSerializer(PortablePlanCompiler.toDataSchema(
                            node.outputSchema())));
            if (node.binding() instanceof StreamProcessBinding) {
                for (StreamOutputTagDescriptor tag
                        : ((StreamProcessBinding) node.binding())
                        .outputTags()) {
                    serializers.put(new OutputPort(
                            node.operatorUid(), tag.tagId()),
                            new RowSerializer(PortablePlanCompiler.toDataSchema(
                                    tag.schema())));
                }
            } else if (node.binding() instanceof StreamWindowBinding) {
                StreamOutputTagDescriptor tag = ((StreamWindowBinding)
                        node.binding()).lateOutputTag();
                if (tag != null) {
                    serializers.put(new OutputPort(
                            node.operatorUid(), tag.tagId()),
                            new RowSerializer(PortablePlanCompiler.toDataSchema(
                                    tag.schema())));
                }
            }
        }
        this.producerSerializers = Collections.unmodifiableMap(serializers);
        this.upsertPorts = Collections.unmodifiableSet(
                resolveUpsertPorts(graph));
        Map<OutputPort, Integer> counts = new HashMap<>();
        Map<String, List<OutputPort>> producerPorts = new LinkedHashMap<>();
        Map<String, PortableStreamNode> nodes = new HashMap<>();
        for (PortableStreamNode node : plan.nodes()) {
            nodes.put(node.operatorUid(), node);
        }
        Map<OutputPort, List<StreamOperatorTask>> resolved =
                new LinkedHashMap<>();
        List<StreamOperatorTask> allTasks = new ArrayList<>();
        for (PortableStreamNode node : unaryTaskNodes(graph)) {
            String inputUid = node.inputOperatorUids().get(0);
            OutputPort inputPort = inputPort(node, 0);
            increment(counts, inputPort);
            register(producerPorts, inputPort);
            PortableStreamNode input = nodes.get(inputUid);
            if (input == null) {
                throw new IllegalArgumentException(
                        "Stream operator input is unavailable");
            }
            List<StreamOperatorTask> tasks = resolved.get(inputPort);
            if (tasks == null) {
                tasks = new ArrayList<>();
                resolved.put(inputPort, tasks);
            }
            StreamOperatorTask task = StreamOperatorTaskFactory.create(
                    node, outputSchema(input, inputPort.outputTagId), resolver,
                    plan.maxParallelism(), plan.deliveryGuarantee());
            restore(task, restoreMembers);
            tasks.add(task);
            allTasks.add(task);
        }
        Map<OutputPort, List<Route>> routed = new LinkedHashMap<>();
        Map<OutputPort, List<UnionInput>> unionInputs =
                new LinkedHashMap<>();
        List<UnionGate> allUnionGates = new ArrayList<>();
        for (PortableStreamNode union : nodes(graph,
                StreamOperatorCode.UNION)) {
            UnionGate gate = (UnionGate)
                    StreamOperatorTaskFactory.createRouting(
                            union, () -> new UnionGate(union));
            restore(gate, restoreMembers);
            allUnionGates.add(gate);
            allTasks.add(gate);
            for (int index = 0;
                    index < union.inputOperatorUids().size(); index++) {
                OutputPort inputPort = inputPort(union, index);
                increment(counts, inputPort);
                register(producerPorts, inputPort);
                List<UnionInput> gates = unionInputs.get(inputPort);
                if (gates == null) {
                    gates = new ArrayList<>();
                    unionInputs.put(inputPort, gates);
                }
                gates.add(new UnionInput(gate, index));
            }
        }
        List<Route> sinkRoutes = new ArrayList<>(
                sinks.size() + boundaryRoutes.size());
        for (SinkRoute sink : sinks) {
            StreamChannelOutput output = outputs.get(sink.node.operatorUid());
            if (output == null) {
                throw new IllegalArgumentException(
                        "Stream stateless DAG Sink route is missing");
            }
            String inputUid = sink.node.inputOperatorUids().get(0);
            OutputPort inputPort = new OutputPort(inputUid,
                    sink.edge.sourceOutputTagId());
            increment(counts, inputPort);
            register(producerPorts, inputPort);
            Route route = new Route(sink.node, output,
                    upsertPorts.contains(inputPort));
            sinkRoutes.add(route);
            List<Route> inputs = routed.get(inputPort);
            if (inputs == null) {
                inputs = new ArrayList<>();
                routed.put(inputPort, inputs);
            }
            inputs.add(route);
        }
        for (StreamDagBoundaryRoute boundary : boundaryRoutes) {
            OutputPort inputPort = new OutputPort(
                    boundary.producerUid, boundary.outputTagId);
            RowSerializer serializer = serializers.get(inputPort);
            if (serializer == null) {
                throw new IllegalArgumentException(
                        "Stream DAG boundary producer is unavailable");
            }
            increment(counts, inputPort);
            register(producerPorts, inputPort);
            Route route = new Route(serializer, boundary.output);
            sinkRoutes.add(route);
            List<Route> inputs = routed.get(inputPort);
            if (inputs == null) {
                inputs = new ArrayList<>();
                routed.put(inputPort, inputs);
            }
            inputs.add(route);
        }
        this.tasksByInput = immutableLists(resolved);
        this.unionsByInput = immutableLists(unionInputs);
        this.routesByInput = immutableLists(routed);
        this.portsByProducer = immutableLists(producerPorts);
        this.routes = Collections.unmodifiableList(sinkRoutes);
        this.unionGates = Collections.unmodifiableList(allUnionGates);
        this.outputCounts = Collections.unmodifiableMap(counts);
        this.operatorTasks = Collections.unmodifiableList(allTasks);
        boolean timed = false;
        for (StreamOperatorTask task : operatorTasks) {
            if (task.hasIdleTimer()) {
                timed = true;
                break;
            }
        }
        this.idleWatchdog = timed ? new Thread(
                this::runIdleWatchdog, "rowweave-stream-event-time-watchdog")
                : null;
        if (idleWatchdog != null) idleWatchdog.setDaemon(true);
    }

    void start() {
        if (!started.compareAndSet(false, true)) return;
        long now = System.nanoTime();
        synchronized (this) {
            for (StreamOperatorTask task : operatorTasks) task.start(now);
        }
        if (idleWatchdog != null) idleWatchdog.start();
    }

    synchronized List<StreamCheckpointMember> checkpointMembers(
            CheckpointIdentity checkpoint) {
        if (checkpoint == null) {
            throw new IllegalArgumentException(
                    "Stream stateless operator checkpoint is unavailable");
        }
        int expected = unionGates.size();
        for (StreamOperatorTask task : operatorTasks) {
            if (task.hasCheckpointState()) expected++;
        }
        if (expected == 0) return Collections.emptyList();
        long deadline = System.nanoTime()
                + CHECKPOINT_WAIT_MILLIS * 1_000_000L;
        while (awaitingCheckpoint(checkpoint, expected) && !completed.get()
                && System.nanoTime() < deadline) {
            try {
                wait(10L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Stream stateless checkpoint wait was interrupted",
                        interrupted);
            }
        }
        if (!checkpoint.equals(checkpointIdentity)
                || checkpointMembers.size() != expected) {
            throw new IllegalStateException(
                    "Stream stateless operator checkpoint is incomplete");
        }
        return Collections.unmodifiableList(new ArrayList<>(
                checkpointMembers.values()));
    }

    private boolean awaitingCheckpoint(CheckpointIdentity checkpoint,
            int expectedMembers) {
        return checkpointIdentity == null
                || checkpointIdentity.checkpointSequence()
                < checkpoint.checkpointSequence()
                || checkpoint.equals(checkpointIdentity)
                && checkpointMembers.size() != expectedMembers;
    }

    @Override
    public StreamChannelIdentity channelIdentity() {
        return routes.get(0).output.channelIdentity();
    }

    StreamChannelOutput input(String sourceUid,
            StreamChannelIdentity identity) {
        return input(sourceUid, null, identity);
    }

    StreamChannelOutput input(String producerUid, String outputTagId,
            StreamChannelIdentity identity) {
        OutputPort producer = new OutputPort(producerUid, outputTagId);
        if (!producerSerializers.containsKey(producer) || identity == null) {
            throw new IllegalArgumentException(
                    "Stream graph input identity is invalid");
        }
        return new StreamChannelOutput() {
            @Override public StreamChannelIdentity channelIdentity() {
                return identity;
            }

            @Override public void send(StreamEnvelope envelope) {
                if (envelope == null || !identity.equals(
                        envelope.channelIdentity())) {
                    throw new IllegalArgumentException(
                            "Stream graph input envelope belongs to another attempt");
                }
                sendFrom(producer, envelope);
            }

            @Override public void close() { }
            @Override public void cancel() { }
            @Override public void fail(Throwable cause) {
                StreamStatelessDagOutput.this.fail(cause);
            }
        };
    }

    @Override
    public void send(StreamEnvelope envelope) {
        if (sources.size() != 1) {
            throw new IllegalStateException(
                    "multi-source Stream graph requires an identified input");
        }
        if (envelope == null || !channelIdentity().equals(
                envelope.channelIdentity())) {
            throw new IllegalArgumentException(
                    "Stream stateless DAG envelope belongs to another attempt");
        }
        sendFrom(new OutputPort(sources.get(0).operatorUid(), null), envelope);
    }

    void sendFrom(String sourceUid, StreamEnvelope envelope) {
        sendFrom(new OutputPort(sourceUid, null), envelope);
    }

    private void sendFrom(OutputPort producer, StreamEnvelope envelope) {
        if (!producerSerializers.containsKey(producer)) {
            throw new IllegalArgumentException(
                    "Stream graph source input is unavailable");
        }
        if (envelope == null) {
            throw new IllegalArgumentException(
                    "Stream stateless DAG envelope is required");
        }
        if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
            emitRecord(producer, envelope);
            return;
        }
        switch (envelope.kind()) {
            case WATERMARK:
                emitWatermark(producer, envelope.timestamp());
                break;
            case WATERMARK_STATUS:
                emitStatus(producer, envelope.watermarkStatus(),
                        envelope.watermarkStatusGeneration());
                break;
            case CHECKPOINT_BARRIER:
                emitBarrier(producer, envelope.checkpointIdentity());
                break;
            case END_OF_INPUT:
                emitEnd(producer);
                break;
            default:
                emitOther(producer, envelope);
        }
    }

    private void emitRecord(OutputPort producer, StreamEnvelope envelope) {
        List<Route> directRoutes = routesByInput.get(producer);
        if (!upsertPorts.contains(producer)
                && tasksByInput.get(producer) == null
                && unionsByInput.get(producer) == null
                && directRoutes != null
                && allDirectSinks(directRoutes)) {
            Long eventTimestamp = envelope.hasEventTimestamp()
                    ? Long.valueOf(envelope.timestamp()) : null;
            for (Route route : directRoutes) {
                route.sendRecord(envelope.recordFrame(), eventTimestamp);
            }
            return;
        }
        byte[] rowFrame = envelope.recordFrame();
        if (upsertPorts.contains(producer)) {
            StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                    rowFrame, maximumFrameBytes, maximumFrameBytes);
            if (!record.hasAfterImage()) {
                emitTombstone(producer, record.stableKeyFrame(),
                        envelope.hasEventTimestamp()
                                ? Long.valueOf(envelope.timestamp()) : null,
                        envelope);
                return;
            }
            rowFrame = record.afterImageFrame();
        }
        DataRow sourceRow = producerSerializers.get(producer).deserialize(
                rowFrame);
        emit(producer, sourceRow,
                envelope.hasEventTimestamp()
                        ? Long.valueOf(envelope.timestamp()) : null,
                envelope);
    }

    private static boolean allDirectSinks(List<Route> routes) {
        for (Route route : routes) {
            if (!route.directSink) return false;
        }
        return true;
    }

    private void emit(String producerUid, DataRow row, Long eventTimestamp,
            StreamEnvelope source) {
        emit(new OutputPort(producerUid, null), row, eventTimestamp, source);
    }

    private void emit(String producerUid, String outputTagId, DataRow row,
            Long eventTimestamp, StreamEnvelope source) {
        emit(new OutputPort(producerUid, outputTagId), row,
                eventTimestamp, source);
    }

    private void emit(OutputPort port, DataRow row, Long eventTimestamp,
            StreamEnvelope source) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) {
                route.sendRow(row, eventTimestamp);
            }
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        int consumers = outputCounts.containsKey(port)
                ? outputCounts.get(port) : 0;
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                DataRow input = consumers > 1 ? row.copy() : row;
                task.process(input, eventTimestamp, output(task, source));
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                DataRow input = consumers > 1 ? row.copy() : row;
                union.gate.record(union.inputIndex, input, eventTimestamp);
            }
        }
    }

    private void emitTombstone(OutputPort port, byte[] stableKey,
            Long eventTimestamp, StreamEnvelope source) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) {
                route.sendTombstone(stableKey, eventTimestamp);
            }
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                emitTombstone(new OutputPort(task.operatorUid(), null),
                        stableKey, eventTimestamp, source);
            }
        }
        if (unionsByInput.containsKey(port)) {
            throw new UnsupportedOperationException(
                    "UPSERT tombstones cannot cross Stream UNION");
        }
    }

    private void emitWatermarkFrom(String producerUid, long timestamp) {
        List<OutputPort> ports = portsByProducer.get(producerUid);
        if (ports == null) return;
        for (OutputPort port : ports) emitWatermark(port, timestamp);
    }

    private void emitWatermark(OutputPort port, long timestamp) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) route.sendWatermark(timestamp);
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                if (task.onWatermark(timestamp, output(task, null))) {
                    emitWatermarkFrom(task.operatorUid(), timestamp);
                }
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                union.gate.watermark(union.inputIndex, timestamp);
            }
        }
    }

    private void emitStatusFrom(String producerUid,
            StreamEnvelope.WatermarkStatus status, long generation) {
        List<OutputPort> ports = portsByProducer.get(producerUid);
        if (ports == null) return;
        for (OutputPort port : ports) emitStatus(port, status, generation);
    }

    private void emitStatus(OutputPort port,
            StreamEnvelope.WatermarkStatus status, long generation) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) {
                route.sendStatus(status, generation);
            }
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                if (task.onWatermarkStatus(
                        status, generation, output(task, null))) {
                    emitStatusFrom(task.operatorUid(), status, generation);
                }
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                union.gate.status(union.inputIndex, status, generation);
            }
        }
    }

    private void emitBarrierFrom(String producerUid,
            CheckpointIdentity checkpoint) {
        List<OutputPort> ports = portsByProducer.get(producerUid);
        if (ports == null) return;
        for (OutputPort port : ports) emitBarrier(port, checkpoint);
    }

    private void emitBarrier(OutputPort port,
            CheckpointIdentity checkpoint) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) route.sendBarrier(checkpoint);
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                snapshot(task, checkpoint);
                emitBarrierFrom(task.operatorUid(), checkpoint);
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                union.gate.barrier(union.inputIndex, checkpoint);
            }
        }
    }

    private void emitEndFrom(String producerUid) {
        List<OutputPort> ports = portsByProducer.get(producerUid);
        if (ports == null) return;
        for (OutputPort port : ports) emitEnd(port);
    }

    private void emitEnd(OutputPort port) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) route.sendEnd();
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                if (task.onEnd(output(task, null))) {
                    emitEndFrom(task.operatorUid());
                }
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                union.gate.end(union.inputIndex);
            }
        }
    }

    private void emitOtherFrom(String producerUid, StreamEnvelope source) {
        List<OutputPort> ports = portsByProducer.get(producerUid);
        if (ports == null) return;
        for (OutputPort port : ports) emitOther(port, source);
    }

    private void emitOther(OutputPort port, StreamEnvelope source) {
        List<Route> directRoutes = routesByInput.get(port);
        if (directRoutes != null) {
            for (Route route : directRoutes) route.sendSignal(source);
        }
        List<StreamOperatorTask> tasks = tasksByInput.get(port);
        if (tasks != null) {
            for (StreamOperatorTask task : tasks) {
                emitOtherFrom(task.operatorUid(), source);
            }
        }
        List<UnionInput> unions = unionsByInput.get(port);
        if (unions != null) {
            for (UnionInput union : unions) {
                union.gate.other(union.inputIndex, source);
            }
        }
    }

    @Override public void close() { complete(Action.CLOSE, null); }
    @Override public void cancel() { complete(Action.CANCEL, null); }
    @Override public void fail(Throwable cause) { complete(Action.FAIL, cause); }

    private void complete(Action action, Throwable cause) {
        if (!completed.compareAndSet(false, true)) return;
        if (idleWatchdog != null) idleWatchdog.interrupt();
        synchronized (this) { notifyAll(); }
        Throwable first = null;
        for (StreamOperatorTask operatorTask : operatorTasks) {
            try {
                operatorTask.close();
            } catch (Throwable failure) {
                if (first == null) first = failure;
                else first.addSuppressed(failure);
            }
        }
        for (Route route : routes) {
            try {
                if (action == Action.CLOSE) route.output.close();
                else if (action == Action.CANCEL) route.output.cancel();
                else route.output.fail(cause);
            } catch (Throwable failure) {
                if (first == null) first = failure;
                else first.addSuppressed(failure);
            }
        }
        if (first instanceof RuntimeException) throw (RuntimeException) first;
        if (first instanceof Error) throw (Error) first;
        if (first != null) throw new IllegalStateException(first);
    }

    private StreamOperatorTask.Sink output(StreamOperatorTask task,
            StreamEnvelope source) {
        return new StreamOperatorTask.Sink() {
            @Override public void emit(DataRow emitted, Long timestamp) {
                StreamStatelessDagOutput.this.emit(
                        task.operatorUid(), emitted, timestamp, source);
            }

            @Override public void emitTagged(String outputTagId,
                    DataRow emitted, Long timestamp) {
                OutputPort port = new OutputPort(
                        task.operatorUid(), outputTagId);
                if (!outputCounts.containsKey(port)) {
                    throw new IllegalArgumentException(
                            "Stream operator emitted an undeclared or unconsumed output tag");
                }
                StreamStatelessDagOutput.this.emit(port, emitted,
                        timestamp, source);
            }

            @Override public void emitWatermark(long timestamp) {
                emitWatermarkFrom(task.operatorUid(), timestamp);
            }

            @Override public void emitWatermarkStatus(
                    StreamEnvelope.WatermarkStatus status, long generation) {
                emitStatusFrom(task.operatorUid(), status, generation);
            }

            @Override public void fail(Throwable cause) {
                StreamStatelessDagOutput.this.fail(cause);
            }

            @Override public void emitEnd() {
                emitEndFrom(task.operatorUid());
            }
        };
    }

    private synchronized void snapshot(StreamOperatorTask task,
            CheckpointIdentity checkpoint) {
        if (!task.hasCheckpointState()) return;
        if (checkpointPayloadStore == null) {
            throw new IllegalStateException(
                    "Stream stateless operator checkpoint storage is unavailable");
        }
        if (checkpointIdentity == null
                || checkpoint.checkpointSequence()
                > checkpointIdentity.checkpointSequence()) {
            checkpointIdentity = checkpoint;
            checkpointMembers.clear();
        } else if (!checkpointIdentity.equals(checkpoint)) {
            throw new IllegalArgumentException(
                    "Stream stateless operator checkpoint did not advance");
        }
        if (checkpointMembers.containsKey(task.operatorUid())) {
            throw new IllegalArgumentException(
                    "Stream stateless operator checkpoint replayed");
        }
        StreamCheckpointMember member = task.snapshot(
                checkpoint, checkpointPayloadStore);
        if (member == null) {
            throw new IllegalStateException(
                    "Stream checkpointable operator returned no state handle");
        }
        checkpointMembers.put(task.operatorUid(), member);
        notifyAll();
    }

    private synchronized void snapshot(UnionGate gate,
            CheckpointIdentity checkpoint) {
        if (checkpointPayloadStore == null) {
            throw new IllegalStateException(
                    "Stream UNION checkpoint storage is unavailable");
        }
        prepareCheckpoint(checkpoint);
        if (checkpointMembers.containsKey(gate.operatorUid())) {
            throw new IllegalArgumentException(
                    "Stream UNION checkpoint replayed");
        }
        checkpointMembers.put(gate.operatorUid(), gate.snapshot(
                checkpointPayloadStore));
        notifyAll();
    }

    private void prepareCheckpoint(CheckpointIdentity checkpoint) {
        if (checkpointIdentity == null
                || checkpoint.checkpointSequence()
                > checkpointIdentity.checkpointSequence()) {
            checkpointIdentity = checkpoint;
            checkpointMembers.clear();
        } else if (!checkpointIdentity.equals(checkpoint)) {
            throw new IllegalArgumentException(
                    "Stream stateless operator checkpoint did not advance");
        }
    }

    private void restore(StreamOperatorTask task,
            List<StreamCheckpointMember> members) {
        if (!task.hasCheckpointState()) return;
        StreamCheckpointMember found = null;
        for (StreamCheckpointMember member : members) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && task.operatorUid().equals(member.operatorUid())) {
                if (found != null) {
                    throw new IllegalArgumentException(
                            "Stream operator restore state is ambiguous");
                }
                found = member;
            }
        }
        if (found == null && !members.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream operator restore state is missing");
        }
        if (found != null) task.restore(found, checkpointPayloadStore);
    }

    private void restore(UnionGate gate,
            List<StreamCheckpointMember> members) {
        StreamCheckpointMember found = stateMember(
                gate.operatorUid(), members);
        if (found == null && !members.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream UNION restore state is missing");
        }
        if (found != null) gate.restore(found, checkpointPayloadStore);
    }

    private static StreamCheckpointMember stateMember(String operatorUid,
            List<StreamCheckpointMember> members) {
        StreamCheckpointMember found = null;
        for (StreamCheckpointMember member : members) {
            if (member.kind() == StreamCheckpointMember.Kind.STATE
                    && operatorUid.equals(member.operatorUid())) {
                if (found != null) {
                    throw new IllegalArgumentException(
                            "Stream operator restore state is ambiguous");
                }
                found = member;
            }
        }
        return found;
    }

    private OutputPort inputPort(PortableStreamNode target,
            int targetInputIndex) {
        for (StreamEdge edge : graph.portableJob().plan().edges()) {
            if (edge.targetOperatorUid().equals(target.operatorUid())
                    && edge.targetInputIndex() == targetInputIndex) {
                return new OutputPort(edge.sourceOperatorUid(),
                        edge.sourceOutputTagId());
            }
        }
        throw new IllegalArgumentException(
                "Stream operator input edge is unavailable");
    }

    private static PortableRowSchema outputSchema(
            PortableStreamNode producer, String outputTagId) {
        if (outputTagId == null) return producer.outputSchema();
        if (producer.binding() instanceof StreamProcessBinding) {
            for (StreamOutputTagDescriptor tag
                    : ((StreamProcessBinding) producer.binding())
                    .outputTags()) {
                if (tag.tagId().equals(outputTagId)) return tag.schema();
            }
        } else if (producer.binding() instanceof StreamWindowBinding) {
            StreamOutputTagDescriptor tag = ((StreamWindowBinding)
                    producer.binding()).lateOutputTag();
            if (tag != null && tag.tagId().equals(outputTagId)) {
                return tag.schema();
            }
        }
        throw new IllegalArgumentException(
                "Stream tagged output schema is unavailable");
    }

    private static Set<OutputPort> resolveUpsertPorts(
            StreamExecutionGraph graph) {
        PortableStreamPlan plan = graph.portableJob().plan();
        Set<OutputPort> result = new HashSet<>();
        for (PortableStreamNode node : plan.nodes()) {
            boolean upsert = node.opcode() == StreamOperatorCode.SOURCE
                    && "RESIDENT_CDC_SNAPSHOT_LOG".equals(
                    node.scalarParameters().get("source.mode"))
                    || node.opcode() == StreamOperatorCode.TEMPORAL_JOIN
                    || node.opcode() == StreamOperatorCode.KEYED_AGGREGATE
                    || node.opcode() == StreamOperatorCode.GENERIC_WINDOW
                    && ((StreamWindowBinding) node.binding())
                    .accumulationMode()
                    == StreamWindowBinding.AccumulationMode.ACCUMULATING
                    || (node.opcode() == StreamOperatorCode.KEYED_PROCESS
                    || node.opcode()
                    == StreamOperatorCode.KEYED_CO_PROCESS)
                    && graph.node(node.operatorUid()).changelogMode()
                    == PortableStreamPlan.ChangelogMode.UPSERT;
            if (!upsert && node.inputOperatorUids().size() == 1) {
                upsert = result.contains(inputPort(plan, node, 0));
            }
            if (upsert) result.add(new OutputPort(node.operatorUid(), null));
            if (node.binding() instanceof StreamProcessBinding) {
                for (StreamOutputTagDescriptor tag
                        : ((StreamProcessBinding) node.binding())
                        .outputTags()) {
                    if (tag.changelogMode()
                            == StreamOutputTagDescriptor.ChangelogMode
                            .UPSERT) {
                        result.add(new OutputPort(
                                node.operatorUid(), tag.tagId()));
                    }
                }
            }
        }
        return result;
    }

    private static List<PortableStreamNode> sourceNodes(
            StreamExecutionGraph graph) {
        return nodes(graph, StreamOperatorCode.SOURCE);
    }

    private static List<PortableStreamNode> unaryTaskNodes(
            StreamExecutionGraph graph) {
        List<PortableStreamNode> result = new ArrayList<>();
        for (StreamExecutionGraph.Node graphNode : graph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            if (StreamOperatorTaskFactory.supportsInline(node.opcode())
                    && node.opcode().inputCount() == 1) {
                result.add(node);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static List<PortableStreamNode> nodes(StreamExecutionGraph graph,
            StreamOperatorCode opcode) {
        List<PortableStreamNode> result = new ArrayList<>();
        for (StreamExecutionGraph.Node graphNode : graph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            if (node.opcode() == opcode) result.add(node);
        }
        return Collections.unmodifiableList(result);
    }

    private static List<SinkRoute> sinkRoutes(StreamExecutionGraph graph) {
        PortableStreamPlan plan = graph.portableJob().plan();
        List<SinkRoute> result = new ArrayList<>(
                plan.rootSinkOperatorUids().size());
        for (String sinkUid : plan.rootSinkOperatorUids()) {
            StreamExecutionGraph.Node sink = graph.node(sinkUid);
            if (sink.portableNode().opcode() != StreamOperatorCode.SINK
                    || sink.inputEdges().size() != 1) {
                throw new IllegalArgumentException(
                        "Stream graph root is not a unary Sink");
            }
            result.add(new SinkRoute(sink.portableNode(),
                    sink.inputEdges().get(0)));
        }
        return Collections.unmodifiableList(result);
    }

    private static int maximumFrameBytes(PortableStreamPlan plan) {
        int maximum = 1;
        for (StreamEdge edge : plan.edges()) {
            maximum = Math.max(maximum, edge.maximumFrameBytes());
        }
        return maximum;
    }

    private static OutputPort inputPort(
            io.rowweave.stream.contract.PortableStreamPlan plan,
            PortableStreamNode target, int targetInputIndex) {
        for (io.rowweave.stream.contract.StreamEdge edge : plan.edges()) {
            if (edge.targetOperatorUid().equals(target.operatorUid())
                    && edge.targetInputIndex() == targetInputIndex) {
                return new OutputPort(edge.sourceOperatorUid(),
                        edge.sourceOutputTagId());
            }
        }
        throw new IllegalArgumentException(
                "Stream operator input edge is unavailable");
    }

    private static int[] indices(
            PortableRowSchema schema, List<String> fieldIds) {
        int[] result = new int[fieldIds.size()];
        for (int offset = 0; offset < result.length; offset++) {
            int found = -1;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(
                        fieldIds.get(offset))) {
                    found = index;
                    break;
                }
            }
            if (found < 0) {
                throw new IllegalArgumentException(
                        "Stream UPSERT stable key binding is missing");
            }
            result[offset] = found;
        }
        return result;
    }

    private static PortableRowSchema keySchema(
            PortableRowSchema schema, int[] indices) {
        List<io.rowweave.portable.PortableField> fields =
                new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }

    private static void increment(Map<OutputPort, Integer> counts,
            OutputPort port) {
        counts.put(port, counts.containsKey(port)
                ? counts.get(port) + 1 : 1);
    }

    private static void register(
            Map<String, List<OutputPort>> portsByProducer,
            OutputPort port) {
        List<OutputPort> ports = portsByProducer.get(port.producerUid);
        if (ports == null) {
            ports = new ArrayList<>();
            portsByProducer.put(port.producerUid, ports);
        }
        if (!ports.contains(port)) ports.add(port);
    }

    private void runIdleWatchdog() {
        try {
            while (!completed.get()) {
                long waitMillis = 100L;
                synchronized (this) {
                    long now = System.nanoTime();
                    for (StreamOperatorTask task : operatorTasks) {
                        if (!task.hasIdleTimer()) continue;
                        task.onIdleTimer(now, output(task, null));
                        waitMillis = Math.min(waitMillis,
                                task.idleDelayMillis(now));
                    }
                }
                Thread.sleep(Math.max(1L, waitMillis));
            }
        } catch (InterruptedException stopped) {
            if (!completed.get()) Thread.currentThread().interrupt();
        } catch (Throwable failure) {
            if (!completed.get()) fail(failure);
        }
    }

    private enum Action { CLOSE, CANCEL, FAIL }

    private static <K, T> Map<K, List<T>> immutableLists(
            Map<K, List<T>> values) {
        Map<K, List<T>> immutable = new LinkedHashMap<>();
        for (Map.Entry<K, List<T>> entry : values.entrySet()) {
            immutable.put(entry.getKey(), Collections.unmodifiableList(
                    new ArrayList<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(immutable);
    }

    private static void delete(Path path, Throwable primary) {
        if (path == null) return;
        try {
            Files.deleteIfExists(path);
        } catch (IOException failure) {
            if (primary != null) primary.addSuppressed(failure);
            else throw new IllegalStateException(
                    "Stream checkpoint staging cleanup failed", failure);
        }
    }

    private static final class Route {
        final StreamChannelOutput output;
        final RowSerializer serializer;
        final RowSerializer keySerializer;
        final int[] keyIndices;
        final boolean upsert;
        final boolean directSink;
        long sequence;

        private Route(PortableStreamNode sink,
                StreamChannelOutput output, boolean upsert) {
            this(new RowSerializer(
                    PortablePlanCompiler.toDataSchema(
                            sink.outputSchema())), output, upsert,
                    upsert ? new RowSerializer(
                            PortablePlanCompiler.toDataSchema(keySchema(
                                    sink.outputSchema(),
                                    indices(sink.outputSchema(),
                                            sink.stableKeyFields()))))
                            : null,
                    upsert ? indices(sink.outputSchema(),
                            sink.stableKeyFields()) : new int[0], true);
        }

        private Route(RowSerializer serializer,
                StreamChannelOutput output) {
            this(serializer, output, false, null, new int[0], false);
        }

        private Route(RowSerializer serializer, StreamChannelOutput output,
                boolean upsert, RowSerializer keySerializer,
                int[] keyIndices, boolean directSink) {
            this.output = output;
            this.serializer = serializer;
            this.upsert = upsert;
            this.keySerializer = keySerializer;
            this.keyIndices = keyIndices;
            this.directSink = directSink;
        }

        void sendRecord(byte[] frame, Long eventTimestamp) {
            long next = nextSequence();
            output.send(eventTimestamp != null
                    ? StreamEnvelope.record(output.channelIdentity(), next,
                    eventTimestamp.longValue(), frame)
                    : StreamEnvelope.record(output.channelIdentity(), next, frame));
        }

        void sendTombstone(byte[] stableKey, Long eventTimestamp) {
            sendRecord(StreamUpsertRecordCodec.encode(
                    StreamUpsertRecord.tombstone(stableKey)),
                    eventTimestamp);
        }

        void sendRow(DataRow row, Long eventTimestamp) {
            byte[] frame = serializer.serialize(row);
            if (upsert) {
                frame = StreamUpsertRecordCodec.encode(
                        StreamUpsertRecord.afterImage(
                                keySerializer.serializeProjected(
                                        row, keyIndices), frame));
            }
            sendRecord(frame, eventTimestamp);
        }

        void sendWatermark(long timestamp) {
            output.send(StreamEnvelope.watermark(
                    output.channelIdentity(), nextSequence(), timestamp));
        }

        void sendStatus(StreamEnvelope.WatermarkStatus status,
                long generation) {
            output.send(StreamEnvelope.watermarkStatus(
                    output.channelIdentity(), nextSequence(), status,
                    generation));
        }

        void sendBarrier(CheckpointIdentity checkpoint) {
            output.send(StreamEnvelope.checkpointBarrier(
                    output.channelIdentity(), nextSequence(), checkpoint));
        }

        void sendEnd() {
            output.send(StreamEnvelope.endOfInput(
                    output.channelIdentity(), nextSequence()));
        }

        void sendSignal(StreamEnvelope source) {
            long next = nextSequence();
            switch (source.kind()) {
                case WATERMARK:
                    output.send(StreamEnvelope.watermark(
                            output.channelIdentity(), next, source.timestamp()));
                    break;
                case CHECKPOINT_BARRIER:
                    output.send(StreamEnvelope.checkpointBarrier(
                            output.channelIdentity(), next,
                            source.checkpointIdentity()));
                    break;
                case WATERMARK_STATUS:
                    output.send(StreamEnvelope.watermarkStatus(
                            output.channelIdentity(), next,
                            source.watermarkStatus(),
                            source.watermarkStatusGeneration()));
                    break;
                case END_OF_INPUT:
                    output.send(StreamEnvelope.endOfInput(
                            output.channelIdentity(), next));
                    break;
                case ASYNC_COMPLETION:
                    output.send(StreamEnvelope.asyncCompletion(
                            output.channelIdentity(), next,
                            source.operationIdentity()));
                    break;
                case CANCEL:
                    output.send(StreamEnvelope.cancel(
                            output.channelIdentity(), next));
                    break;
                case ERROR:
                    output.send(StreamEnvelope.error(
                            output.channelIdentity(), next,
                            source.errorCode()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "record must use the Stream DAG record path");
            }
        }

        private long nextSequence() {
            sequence = Math.addExact(sequence, 1L);
            return sequence;
        }
    }

    private static final class SinkRoute {
        final PortableStreamNode node;
        final StreamEdge edge;

        private SinkRoute(PortableStreamNode node, StreamEdge edge) {
            this.node = node;
            this.edge = edge;
        }
    }

    private static final class OutputPort {
        final String producerUid;
        final String outputTagId;

        OutputPort(String producerUid, String outputTagId) {
            if (producerUid == null || producerUid.isEmpty()
                    || outputTagId != null && outputTagId.isEmpty()) {
                throw new IllegalArgumentException(
                        "Stream output port identity is invalid");
            }
            this.producerUid = producerUid;
            this.outputTagId = outputTagId;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof OutputPort)) return false;
            OutputPort that = (OutputPort) other;
            return producerUid.equals(that.producerUid)
                    && Objects.equals(outputTagId, that.outputTagId);
        }

        @Override public int hashCode() {
            return Objects.hash(producerUid, outputTagId);
        }
    }

    private final class UnionInput {
        final UnionGate gate;
        final int inputIndex;

        UnionInput(UnionGate gate, int inputIndex) {
            this.gate = gate;
            this.inputIndex = inputIndex;
        }
    }

    private final class UnionGate implements StreamOperatorTask {
        private final PortableStreamNode node;
        private final List<InputState> inputs = new ArrayList<>(2);
        private final int capacity;
        private final Thread thread;
        private CheckpointIdentity pendingCheckpoint;
        private long lastCheckpointSequence;
        private long statusGeneration;
        private long lastWatermark = Long.MIN_VALUE;
        private int nextInputIndex;
        private boolean outputIdle;
        private boolean ended;
        private boolean started;
        private boolean stopped;

        UnionGate(PortableStreamNode node) {
            this.node = node;
            this.capacity = graph.portableJob().resourceProfile()
                    .mailboxEvents();
            for (int index = 0;
                    index < node.inputOperatorUids().size(); index++) {
                String input = node.inputOperatorUids().get(index);
                StreamEdgeContract edge = inputEdge(node.operatorUid(), index);
                inputs.add(new InputState(index, input,
                        edge.outputTagId, new RowSerializer(
                        PortablePlanCompiler.toDataSchema(
                                outputSchema(planNode(input),
                                        edge.outputTagId))),
                        edge.maximumFrameBytes, edge.creditBytes));
            }
            this.thread = new Thread(this::run,
                    "rowweave-stream-union-" + node.operatorUid());
            thread.setDaemon(true);
        }

        @Override public String operatorUid() {
            return node.operatorUid();
        }

        @Override public synchronized void start() {
            if (started) return;
            started = true;
            thread.start();
        }

        @Override public void start(long nowNanos) { start(); }

        void record(int inputIndex, DataRow row, Long timestamp) {
            InputState input = state(inputIndex);
            byte[] frame = input.serializer.serialize(row);
            if (frame.length > input.maximumFrameBytes) {
                throw new IllegalArgumentException(
                        "Stream UNION record exceeds its edge frame bound");
            }
            enqueue(UnionEvent.record(inputIndex, frame, timestamp));
        }

        void watermark(int inputIndex, long timestamp) {
            enqueue(UnionEvent.watermark(inputIndex, timestamp));
        }

        void status(int inputIndex, StreamEnvelope.WatermarkStatus status,
                long generation) {
            enqueue(UnionEvent.status(inputIndex, status, generation));
        }

        void barrier(int inputIndex, CheckpointIdentity checkpoint) {
            enqueue(UnionEvent.barrier(inputIndex, checkpoint));
        }

        void end(int inputIndex) {
            enqueue(UnionEvent.end(inputIndex));
        }

        void other(int inputIndex, StreamEnvelope source) {
            enqueue(UnionEvent.other(inputIndex, source));
        }

        private void enqueue(UnionEvent event) {
            synchronized (this) {
                InputState input = state(event.inputIndex);
                try {
                    while (!stopped && !completed.get()
                            && (input.queue.size() >= capacity
                            || input.queuedBytes + event.frameBytes()
                            > input.creditBytes)) {
                        wait();
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "Stream UNION input was interrupted", interrupted);
                }
                if (stopped || completed.get()) {
                    throw new IllegalStateException(
                            "Stream UNION input gate is closed");
                }
                input.queue.addLast(event);
                input.queuedBytes += event.frameBytes();
                notifyAll();
            }
        }

        private void run() {
            try {
                while (true) {
                    UnionEvent event = take();
                    if (event == null) return;
                    process(event);
                }
            } catch (InterruptedException stoppedByOwner) {
                if (!stopped && !completed.get()) {
                    Thread.currentThread().interrupt();
                    StreamStatelessDagOutput.this.fail(stoppedByOwner);
                }
            } catch (Throwable failure) {
                if (!stopped && !completed.get()) {
                    StreamStatelessDagOutput.this.fail(failure);
                }
            }
        }

        private UnionEvent take() throws InterruptedException {
            synchronized (this) {
                while (!stopped && !completed.get()) {
                    int size = inputs.size();
                    for (int offset = 0; offset < size; offset++) {
                        int index = (nextInputIndex + offset) % size;
                        InputState input = inputs.get(index);
                        if (input.checkpoint != null
                                || input.queue.isEmpty()) continue;
                        UnionEvent event = input.queue.removeFirst();
                        input.queuedBytes -= event.frameBytes();
                        nextInputIndex = (index + 1) % size;
                        notifyAll();
                        return event;
                    }
                    wait();
                }
                return null;
            }
        }

        private void process(UnionEvent event) {
            switch (event.kind) {
                case RECORD:
                    processRecord(event);
                    return;
                case WATERMARK:
                    processWatermark(event);
                    return;
                case STATUS:
                    processStatus(event);
                    return;
                case BARRIER:
                    processBarrier(event);
                    return;
                case END:
                    processEnd(event);
                    return;
                case OTHER:
                    processOther(event);
                    return;
                default:
                    throw new IllegalStateException(
                            "unknown Stream UNION input event");
            }
        }

        private void processRecord(UnionEvent event) {
            InputState input = activeState(event.inputIndex);
            if (input.idle) {
                throw new IllegalStateException(
                        "Stream UNION IDLE input emitted a record before ACTIVE");
            }
            emit(node.operatorUid(), input.serializer.deserialize(
                    event.recordFrame), event.timestamp, null);
        }

        private void processWatermark(UnionEvent event) {
            InputState input = activeState(event.inputIndex);
            if (input.idle) {
                throw new IllegalStateException(
                        "Stream UNION IDLE input emitted a watermark before ACTIVE");
            }
            if (input.watermarkKnown && event.timestamp < input.watermark) {
                throw new IllegalArgumentException(
                        "Stream UNION input watermark moved backwards");
            }
            input.watermark = event.timestamp;
            input.watermarkKnown = true;
            emitMergedWatermark();
        }

        private void processStatus(UnionEvent event) {
            InputState input = activeState(event.inputIndex);
            boolean nextIdle = event.status
                    == StreamEnvelope.WatermarkStatus.IDLE;
            if (event.generation != input.statusGeneration + 1L
                    || nextIdle == input.idle) {
                throw new IllegalArgumentException(
                        "Stream UNION input status transition is noncanonical");
            }
            input.statusGeneration = event.generation;
            input.idle = nextIdle;
            updateOutputIdle();
            emitMergedWatermark();
        }

        private void processBarrier(UnionEvent event) {
            InputState input = activeState(event.inputIndex);
            CheckpointIdentity checkpoint = event.checkpoint;
            if (checkpoint.checkpointSequence() <= lastCheckpointSequence
                    || input.checkpoint != null) {
                throw new IllegalArgumentException(
                        "Stream UNION checkpoint barrier replayed");
            }
            if (pendingCheckpoint == null) pendingCheckpoint = checkpoint;
            if (!pendingCheckpoint.equals(checkpoint)) {
                throw new IllegalArgumentException(
                        "Stream UNION checkpoint barriers differ");
            }
            input.checkpoint = checkpoint;
            tryCompleteCheckpoint();
        }

        private void processEnd(UnionEvent event) {
            InputState input = activeState(event.inputIndex);
            synchronized (this) {
                if (!input.queue.isEmpty()) {
                    throw new IllegalStateException(
                            "Stream UNION input emitted after END_OF_INPUT");
                }
            }
            input.ended = true;
            tryCompleteCheckpoint();
            updateOutputIdle();
            emitMergedWatermark();
            for (InputState current : inputs) {
                if (!current.ended) return;
            }
            if (!ended) {
                ended = true;
                emitEndFrom(node.operatorUid());
            }
        }

        private void processOther(UnionEvent event) {
            activeState(event.inputIndex);
            if (event.source.kind() == StreamEnvelope.Kind.ERROR) {
                throw new IllegalStateException(
                        "Stream UNION input failed: "
                                + event.source.errorCode());
            }
            if (event.source.kind() == StreamEnvelope.Kind.CANCEL) {
                StreamStatelessDagOutput.this.cancel();
                return;
            }
            emitOtherFrom(node.operatorUid(), event.source);
        }

        private void tryCompleteCheckpoint() {
            if (pendingCheckpoint == null) return;
            for (InputState current : inputs) {
                if (!current.ended
                        && !pendingCheckpoint.equals(current.checkpoint)) {
                    return;
                }
            }
            CheckpointIdentity completedCheckpoint = pendingCheckpoint;
            lastCheckpointSequence = completedCheckpoint.checkpointSequence();
            for (InputState current : inputs) {
                current.checkpoint = null;
            }
            pendingCheckpoint = null;
            StreamStatelessDagOutput.this.snapshot(
                    this, completedCheckpoint);
            emitBarrierFrom(node.operatorUid(), completedCheckpoint);
            synchronized (this) { notifyAll(); }
        }

        private void updateOutputIdle() {
            boolean anyLive = false;
            boolean allIdle = true;
            for (InputState current : inputs) {
                if (current.ended) continue;
                anyLive = true;
                if (!current.idle) allIdle = false;
            }
            if (!anyLive || allIdle == outputIdle) return;
            outputIdle = allIdle;
            statusGeneration = Math.addExact(statusGeneration, 1L);
            emitStatusFrom(node.operatorUid(), allIdle
                    ? StreamEnvelope.WatermarkStatus.IDLE
                    : StreamEnvelope.WatermarkStatus.ACTIVE,
                    statusGeneration);
        }

        private InputState activeState(int inputIndex) {
            InputState input = state(inputIndex);
            if (input.ended) {
                throw new IllegalStateException(
                        "Stream UNION input emitted after END_OF_INPUT");
            }
            return input;
        }

        private InputState state(int inputIndex) {
            if (inputIndex < 0 || inputIndex >= inputs.size()) {
                throw new IllegalArgumentException(
                        "Stream UNION input is unavailable");
            }
            return inputs.get(inputIndex);
        }

        private void emitMergedWatermark() {
            boolean present = false;
            long candidate = Long.MAX_VALUE;
            for (InputState input : inputs) {
                if (input.idle || input.ended) continue;
                if (!input.watermarkKnown) return;
                present = true;
                candidate = Math.min(candidate, input.watermark);
            }
            if (present && candidate > lastWatermark) {
                lastWatermark = candidate;
                emitWatermarkFrom(node.operatorUid(), candidate);
            }
        }

        StreamCheckpointMember snapshot(
                FileCheckpointPayloadStore payloadStore) {
            if (payloadStore == null || pendingCheckpoint != null) {
                throw new IllegalArgumentException(
                        "Stream UNION checkpoint runtime is invalid");
            }
            Path staged = null;
            Throwable primary = null;
            try {
                staged = Files.createTempFile(
                        "rowweave-stream-union-", ".snapshot");
                try (DataOutputStream output = new DataOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(staged,
                                StandardOpenOption.TRUNCATE_EXISTING)))) {
                    output.write(UNION_STATE_MAGIC);
                    output.writeInt(UNION_STATE_VERSION);
                    output.writeInt(inputs.size());
                    for (InputState input : inputs) {
                        output.writeInt(input.inputIndex);
                        output.writeUTF(input.producerUid);
                        output.writeBoolean(input.outputTagId != null);
                        if (input.outputTagId != null) {
                            output.writeUTF(input.outputTagId);
                        }
                        output.writeBoolean(input.watermarkKnown);
                        output.writeLong(input.watermark);
                        output.writeLong(input.statusGeneration);
                        output.writeBoolean(input.idle);
                        output.writeBoolean(input.ended);
                    }
                    output.writeLong(lastCheckpointSequence);
                    output.writeLong(statusGeneration);
                    output.writeLong(lastWatermark);
                    output.writeBoolean(outputIdle);
                }
                FileCheckpointPayloadStore.StoredPayload stored =
                        payloadStore.store(staged);
                return StreamCheckpointMember.stateHandle(
                        node.operatorUid(), 0, 0,
                        graph.portableJob().plan().maxParallelism() - 1,
                        UNION_STATE_NAME, UNION_STATE_CODEC,
                        UNION_STATE_VERSION, stored.digest(),
                        stored.contentBytes());
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } catch (IOException failure) {
                primary = failure;
                throw new IllegalStateException(
                        "Stream UNION checkpoint staging failed", failure);
            } finally {
                delete(staged, primary);
            }
        }

        @Override public void restore(StreamCheckpointMember member,
                FileCheckpointPayloadStore payloadStore) {
            validateRestoreMember(member, payloadStore);
            Path staged = null;
            Throwable primary = null;
            try {
                staged = Files.createTempFile(
                        "rowweave-stream-union-", ".restore");
                payloadStore.restore(
                        new FileCheckpointPayloadStore.StoredPayload(
                                member.contentDigest(), member.contentBytes()),
                        staged);
                try (DataInputStream input = new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(staged)))) {
                    byte[] magic = new byte[UNION_STATE_MAGIC.length];
                    input.readFully(magic);
                    if (!Arrays.equals(UNION_STATE_MAGIC, magic)
                            || input.readInt() != UNION_STATE_VERSION
                            || input.readInt() != inputs.size()) {
                        invalidState();
                    }
                    for (InputState state : inputs) {
                        if (input.readInt() != state.inputIndex
                                || !state.producerUid.equals(input.readUTF())) {
                            invalidState();
                        }
                        String restoredTag = input.readBoolean()
                                ? input.readUTF() : null;
                        if (!Objects.equals(
                                state.outputTagId, restoredTag)) invalidState();
                        state.watermarkKnown = input.readBoolean();
                        state.watermark = input.readLong();
                        state.statusGeneration = input.readLong();
                        state.idle = input.readBoolean();
                        state.ended = input.readBoolean();
                        if (state.statusGeneration < 0L) invalidState();
                    }
                    lastCheckpointSequence = input.readLong();
                    statusGeneration = input.readLong();
                    lastWatermark = input.readLong();
                    outputIdle = input.readBoolean();
                    if (input.read() != -1 || lastCheckpointSequence <= 0L
                            || statusGeneration < 0L
                            || outputIdle != restoredAllIdle()) {
                        invalidState();
                    }
                }
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } catch (IOException failure) {
                primary = failure;
                throw new IllegalStateException(
                        "Stream UNION checkpoint restore failed", failure);
            } finally {
                delete(staged, primary);
            }
        }

        private boolean restoredAllIdle() {
            boolean live = false;
            for (InputState state : inputs) {
                if (state.ended) continue;
                live = true;
                if (!state.idle) return false;
            }
            return live;
        }

        private void validateRestoreMember(StreamCheckpointMember member,
                FileCheckpointPayloadStore payloadStore) {
            if (member == null || payloadStore == null
                    || member.kind() != StreamCheckpointMember.Kind.STATE
                    || !node.operatorUid().equals(member.operatorUid())
                    || member.subtaskIndex() != 0
                    || member.keyGroupStart() != 0
                    || member.keyGroupEnd()
                    != graph.portableJob().plan().maxParallelism() - 1
                    || !UNION_STATE_NAME.equals(member.subjectId())
                    || !UNION_STATE_CODEC.equals(member.codecId())
                    || member.schemaVersion() != UNION_STATE_VERSION) {
                throw new IllegalArgumentException(
                        "Stream UNION restore member is incompatible");
            }
        }

        private void invalidState() {
            throw new IllegalArgumentException(
                    "Stream UNION checkpoint state is invalid");
        }

        void shutdown() throws InterruptedException {
            synchronized (this) {
                stopped = true;
                notifyAll();
            }
            thread.interrupt();
            if (Thread.currentThread() != thread) {
                thread.join(5_000L);
                if (thread.isAlive()) {
                    throw new IllegalStateException(
                            "Stream UNION input gate did not stop");
                }
            }
        }

        @Override public void cancel() { close(); }

        @Override public void close() {
            try {
                shutdown();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Stream UNION input gate stop was interrupted",
                        interrupted);
            }
        }

        @Override public void awaitCompletion(long timeoutMillis)
                throws InterruptedException {
            thread.join(timeoutMillis);
        }

        private PortableStreamNode planNode(String operatorUid) {
            for (PortableStreamNode candidate
                    : graph.portableJob().plan().nodes()) {
                if (candidate.operatorUid().equals(operatorUid)) {
                    return candidate;
                }
            }
            throw new IllegalArgumentException(
                    "Stream UNION input node is unavailable");
        }

        private StreamEdgeContract inputEdge(String targetUid,
                int targetInputIndex) {
            for (StreamEdge edge : graph.portableJob().plan().edges()) {
                if (edge.targetOperatorUid().equals(targetUid)
                        && edge.targetInputIndex() == targetInputIndex) {
                    return new StreamEdgeContract(edge.maximumFrameBytes(),
                            edge.initialCreditBytes(),
                            edge.sourceOutputTagId());
                }
            }
            throw new IllegalArgumentException(
                    "Stream UNION input edge is unavailable");
        }
    }

    private static final class InputState {
        final ArrayDeque<UnionEvent> queue = new ArrayDeque<>();
        final int inputIndex;
        final String producerUid;
        final String outputTagId;
        final RowSerializer serializer;
        final int maximumFrameBytes;
        final long creditBytes;
        long queuedBytes;
        long watermark;
        long statusGeneration;
        boolean watermarkKnown;
        boolean idle;
        boolean ended;
        CheckpointIdentity checkpoint;

        InputState(int inputIndex, String producerUid, String outputTagId,
                RowSerializer serializer, int maximumFrameBytes,
                long creditBytes) {
            this.inputIndex = inputIndex;
            this.producerUid = producerUid;
            this.outputTagId = outputTagId;
            this.serializer = serializer;
            this.maximumFrameBytes = maximumFrameBytes;
            this.creditBytes = creditBytes;
        }
    }

    private static final class StreamEdgeContract {
        final int maximumFrameBytes;
        final int creditBytes;
        final String outputTagId;

        StreamEdgeContract(int maximumFrameBytes, int creditBytes,
                String outputTagId) {
            this.maximumFrameBytes = maximumFrameBytes;
            this.creditBytes = creditBytes;
            this.outputTagId = outputTagId;
        }
    }

    private static final class UnionEvent {
        enum Kind { RECORD, WATERMARK, STATUS, BARRIER, END, OTHER }
        final Kind kind;
        final int inputIndex;
        final byte[] recordFrame;
        final Long timestamp;
        final StreamEnvelope source;
        final StreamEnvelope.WatermarkStatus status;
        final long generation;
        final CheckpointIdentity checkpoint;

        private UnionEvent(Kind kind, int inputIndex, byte[] recordFrame,
                Long timestamp, StreamEnvelope source,
                StreamEnvelope.WatermarkStatus status, long generation,
                CheckpointIdentity checkpoint) {
            this.kind = kind;
            this.inputIndex = inputIndex;
            this.recordFrame = recordFrame;
            this.timestamp = timestamp;
            this.source = source;
            this.status = status;
            this.generation = generation;
            this.checkpoint = checkpoint;
        }

        static UnionEvent record(int inputIndex, byte[] recordFrame,
                Long timestamp) {
            return new UnionEvent(Kind.RECORD, inputIndex, recordFrame,
                    timestamp, null, null, 0L, null);
        }

        static UnionEvent watermark(int inputIndex, long timestamp) {
            return new UnionEvent(Kind.WATERMARK, inputIndex, null, timestamp,
                    null, null, 0L, null);
        }

        static UnionEvent status(int inputIndex,
                StreamEnvelope.WatermarkStatus status, long generation) {
            return new UnionEvent(Kind.STATUS, inputIndex, null, null,
                    null, status, generation, null);
        }

        static UnionEvent barrier(int inputIndex,
                CheckpointIdentity checkpoint) {
            return new UnionEvent(Kind.BARRIER, inputIndex, null, null,
                    null, null, 0L, checkpoint);
        }

        static UnionEvent end(int inputIndex) {
            return new UnionEvent(Kind.END, inputIndex, null, null,
                    null, null, 0L, null);
        }

        static UnionEvent other(int inputIndex, StreamEnvelope source) {
            return new UnionEvent(Kind.OTHER, inputIndex, null, null,
                    source, null, 0L, null);
        }

        int frameBytes() {
            return recordFrame == null ? 0 : recordFrame.length;
        }
    }
}
