package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.DurableSinkConfigurationEnvelope;
import io.rowweave.DurableSinkConfigurationEnvelopeCodec;
import io.rowweave.ProviderBatchSink;
import io.rowweave.internal.BatchSinkBindings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A serializable logical plan: {@link PlanNode} values in topological order, root last, with children
 * referenced by index. What crosses the network is a description. The SOURCE payload carries serializable
 * Source descriptors, so no row is read or materialized on the driver side.
 *
 * <p>Serialization layout: [nodeCount] followed by, for each node,
 * [kind ordinal][typeId UTF][outMeta(fieldCount + each name/alias/type)]
 * [childCount + each child index][params(count + each key/value UTF)]
 * [listParams(count + each key UTF + size + each value UTF)][payload tag + SOURCE Sources / SINK Sink /
 * userFunction blob].
 */
public final class PlanGraph {

    /** Internal provenance retained when Master projects the built-in provider proxy. */
    public static final String DURABLE_PROVIDER_BINDING_PARAM =
            "__rowweave_durable_provider_binding_v1";

    private final List<PlanNode> nodes;
    // The desired parallelism P of the job. It is a job property and travels with the graph, not a
    // property of a worker or service instance, so it stays independent of slots. The cluster Master
    // computes an effective P from this and the resources available, never above the desired value, and
    // does not write it back into the graph. Defaults to 1.
    private final int parallelism;

    public PlanGraph(List<PlanNode> nodes) {
        this(nodes, 1);
    }

    /** Constructor that takes the job parallelism P, which must be at least 1. Derived copies carry P through, and a graph built by hand in a test injects it the same way. */
    public PlanGraph(List<PlanNode> nodes, int parallelism) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("plan graph must have at least one node");
        }
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got " + parallelism);
        }
        long sinkDescriptorBytes = 0L;
        for (int i = 0; i < nodes.size(); i++) {
            PlanNode node = nodes.get(i);
            if (node == null) {
                throw new IllegalArgumentException("plan node must not be null at index " + i);
            }
            if (node.kind() == PlanNode.Kind.SINK && node.output() != null) {
                sinkDescriptorBytes = Math.addExact(sinkDescriptorBytes,
                        BatchSinkDescriptorCodec.encode(
                                node.batchSinkDescriptor()).length);
                if (sinkDescriptorBytes > BatchSinkBindings.MAX_TOTAL_DESCRIPTOR_BYTES) {
                    throw new IllegalArgumentException(
                            "batch sink descriptor bytes exceed aggregate limit "
                                    + BatchSinkBindings.MAX_TOTAL_DESCRIPTOR_BYTES);
                }
            }
        }
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.parallelism = parallelism;
    }

    public List<PlanNode> nodes() {
        return nodes;
    }

    /** The desired job parallelism P. The cluster Master derives the effective P from this plus available resources. Defaults to 1. */
    public int parallelism() {
        return parallelism;
    }

    public int nodeCount() {
        return nodes.size();
    }

    public PlanNode node(int index) {
        return nodes.get(index);
    }

    /** The root is the last node, which the post-order walk that builds the graph guarantees. */
    public PlanNode root() {
        return nodes.get(nodes.size() - 1);
    }

    public int rootIndex() {
        return nodes.size() - 1;
    }

    /** How many SINK nodes the graph has, which is how many output landing points exist. More than one is a whole graph fanning out to several sinks. */
    public int sinkCount() {
        int c = 0;
        for (PlanNode n : nodes) {
            if (n.kind() == PlanNode.Kind.SINK) {
                c++;
            }
        }
        return c;
    }

    /**
     * Exports the DAG as JSON: {@code {"nodes":[{id,name,typeId,location,fields:[...]}],"edges":[{from,to}]}}.
     * An edge runs from a child id to this node id, in the direction data flows. The monitor renders the
     * job DAG from this, showing each node name and its call site file:line.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"nodes\":[");
        for (int i = 0; i < nodes.size(); i++) {
            PlanNode n = nodes.get(i);
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"id\":").append(i)
                    .append(",\"name\":\"").append(esc(n.name())).append('"')
                    .append(",\"typeId\":\"").append(esc(n.typeId())).append('"')
                    .append(",\"kind\":\"").append(n.kind()).append('"')
                    .append(",\"location\":\"").append(esc(n.location() == null ? "" : n.location())).append('"')
                    .append(",\"fields\":[");
            DataSchema m = n.outMeta();
            for (int f = 0; f < m.getFieldCount(); f++) {
                if (f > 0) {
                    sb.append(',');
                }
                sb.append('"').append(esc(m.getField(f).getName())).append('"');
            }
            sb.append("]}");
        }
        sb.append("],\"edges\":[");
        boolean firstEdge = true;
        for (int i = 0; i < nodes.size(); i++) {
            for (int child : nodes.get(i).childIndices()) {
                if (!firstEdge) {
                    sb.append(',');
                }
                sb.append("{\"from\":").append(child).append(",\"to\":").append(i).append('}');
                firstEdge = false;
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Exports the DAG as Graphviz DOT: a node label is name\ntypeId\nfile:line and an edge runs child to
     * parent, in the direction data flows. The monitor can render it directly with {@code dot -Tsvg}.
     */
    public String toDot() {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph plan {\n");
        sb.append("  rankdir=BT;\n"); // source at bottom, sink at top (data flows up)
        for (int i = 0; i < nodes.size(); i++) {
            PlanNode n = nodes.get(i);
            String loc = n.location() == null ? "" : n.location();
            String label = dotEsc(n.name()) + "\\n" + dotEsc(n.typeId());
            if (!loc.isEmpty()) {
                label += "\\n" + dotEsc(loc);
            }
            sb.append("  n").append(i).append(" [label=\"").append(label).append("\"];\n");
        }
        for (int i = 0; i < nodes.size(); i++) {
            for (int child : nodes.get(i).childIndices()) {
                sb.append("  n").append(child).append(" -> n").append(i).append(";\n");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

    /** Escapes a JSON string: quotes, backslashes and control characters. */
    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default: b.append(c);
            }
        }
        return b.toString();
    }

    /** Escapes a DOT label: quotes and backslashes; the caller supplies literal \n for a line break. */
    private static String dotEsc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Whether this is a pipeline chain with no shuffle: exactly one SOURCE, every other node a
     * pass-through UNARY (filter, select, map), with no pipeline breaker (groupBySum, distinct, orderBy,
     * top) and no BINARY. Such a chain can be handed whole to a single Worker and read in parallel by
     * source split, and because each split keeps its own order, the merged result is deterministic at byte
     * level.
     */
    public boolean isPurePipeline() {
        int sourceCount = 0;
        for (PlanNode n : nodes) {
            switch (n.kind()) {
                case SOURCE:
                    sourceCount++;
                    break;
                case UNARY:
                    if (!isPassThrough(n.typeId())) {
                        return false;
                    }
                    break;
                case BINARY:
                    return false;
                default:
                    return false;
            }
        }
        return sourceCount == 1;
    }

    private static boolean isPassThrough(String typeId) {
        return "filter".equals(typeId) || "select".equals(typeId) || "map".equals(typeId)
                || "partitionByHash".equals(typeId) || "rowClassify".equals(typeId)
                || "qualityRoute".equals(typeId) || "objectQualityRoute".equals(typeId);
    }

    /**
     * Whether this is a linear section: exactly one SOURCE, every other node UNARY, no BINARY. Wider than
     * {@link #isPurePipeline}, because a pipeline breaker (groupBy, orderBy, ...) may sit in the middle or
     * at the end. Such a section compiles whole into a list of PipelineExecutor stages by PlanCompiler.
     */
    public boolean isLinearSegment() {
        int sourceCount = 0;
        for (PlanNode n : nodes) {
            switch (n.kind()) {
                case SOURCE:
                    sourceCount++;
                    break;
                case UNARY:
                    break;
                default: // BINARY
                    return false;
            }
        }
        return sourceCount == 1;
    }

    /** The index of the only SOURCE node, used by the pure pipeline path; throws if there is none. */
    public int singleSourceIndex() {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).kind() == PlanNode.Kind.SOURCE) {
                return i;
            }
        }
        throw new IllegalStateException("graph has no SOURCE node");
    }

    /**
     * Extracts the subgraph rooted at {@code rootNodeIndex}, used to cut at a shuffle boundary: collect
     * every node that node reaches, then renumber into an independent {@link PlanGraph} whose root is still
     * last. Child references are rewritten to the new numbering.
     */
    public PlanGraph subGraphRootedAt(int rootNodeIndex) {
        // post-order collect reachable nodes, assigning new contiguous indices (root last)
        List<PlanNode> newNodes = new ArrayList<>();
        java.util.Map<Integer, Integer> oldToNew = new java.util.HashMap<>();
        collectSub(rootNodeIndex, newNodes, oldToNew);
        return new PlanGraph(newNodes, parallelism);  // P carried through
    }

    private int collectSub(int oldIndex, List<PlanNode> newNodes, java.util.Map<Integer, Integer> oldToNew) {
        Integer existing = oldToNew.get(oldIndex);
        if (existing != null) {
            return existing;
        }
        PlanNode old = nodes.get(oldIndex);
        switch (old.kind()) {
            case SOURCE: {
                // Keep the lazy Source and its already frozen descriptor binding together, rather than
                // re-deriving the connector identity.
                newNodes.add(old.withSourceSplitBindings(old.sourceSplitBindings()));
                int newIdx = newNodes.size() - 1;
                oldToNew.put(oldIndex, newIdx);
                return newIdx;
            }
            case UNARY: {
                int childNew = collectSub(old.child(0), newNodes, oldToNew);
                // Rebuilt from the serializable userFunction of a code lambda, or from typeId plus
                // params; there is no in-memory Stage fallback.
                if (old.userFunction() != null) {
                    newNodes.add(old.params().isEmpty() && old.listParams().isEmpty()
                            ? PlanNode.unaryFn(old.typeId(), old.outMeta(), childNew, old.userFunction())
                            : PlanNode.unaryFnWithParams(old.typeId(), old.outMeta(), childNew,
                                    old.params(), old.listParams(), old.userFunction()));
                } else {
                    PlanNode.Builder b = PlanNode.unary(old.typeId(), old.outMeta(), childNew);
                    copyParams(old, b);
                    newNodes.add(b.build());
                }
                int newIdx = newNodes.size() - 1;
                oldToNew.put(oldIndex, newIdx);
                return newIdx;
            }
            case BINARY: {
                int leftNew = collectSub(old.child(0), newNodes, oldToNew);
                int rightNew = collectSub(old.child(1), newNodes, oldToNew);
                if (old.userFunction() != null) {
                    newNodes.add(PlanNode.binaryFn(old.typeId(), old.outMeta(), leftNew, rightNew,
                            old.params(), old.listParams(), old.userFunction()));
                } else {
                    PlanNode.Builder b = PlanNode.binary(old.typeId(), old.outMeta(), leftNew, rightNew);
                    copyParams(old, b);
                    newNodes.add(b.build());
                }
                int newIdx = newNodes.size() - 1;
                oldToNew.put(oldIndex, newIdx);
                return newIdx;
            }
            case SINK: {
                // A SINK is a unary outlet with one child, renumbered like a UNARY. The output type param
                // survives through copyParams.
                // When a Sink instance is held, use the instance overload, since that is what ships to the
                // worker; otherwise keep the legacy type string.
                int childNew = collectSub(old.child(0), newNodes, oldToNew);
                PlanNode.Builder b = (old.output() != null)
                        ? PlanNode.sink(old.outMeta(), childNew, old.output())
                        : PlanNode.sink(old.outMeta(), childNew, old.param(PlanNode.OUTPUT_TYPE_PARAM));
                copyParams(old, b);
                newNodes.add(b.build());
                int newIdx = newNodes.size() - 1;
                oldToNew.put(oldIndex, newIdx);
                return newIdx;
            }
            default:
                throw new IllegalStateException("unknown kind: " + old.kind());
        }
    }

    private static void copyParams(PlanNode src, PlanNode.Builder b) {
        for (Map.Entry<String, String> e : src.params().entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : src.listParams().entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
    }

    /**
     * Replaces a SOURCE node with the subset of Sources the application already split, lazily and without
     * materializing, and returns a new graph with the same structure. This is how the executor assigns
     * {@code Source[]} units to workers: the engine does not guess where a database or file splits.
     */
    public PlanGraph replaceSourceInputs(int sourceIndex, List<io.rowweave.Source> splitInputs) {
        PlanNode old = nodes.get(sourceIndex);
        if (old.kind() != PlanNode.Kind.SOURCE) {
            throw new IllegalArgumentException("node " + sourceIndex + " is not a SOURCE");
        }
        List<PlanNode> copy = new ArrayList<>(nodes);
        copy.set(sourceIndex, old.withInputs(splitInputs));
        return new PlanGraph(copy, parallelism);  // P carried through
    }

    /** Replaces a SOURCE with a subset of already frozen Source or descriptor bindings, without regenerating split identity. */
    public PlanGraph replaceSourceBindings(
            int sourceIndex, List<io.rowweave.BatchSourceSplitBinding> splitBindings) {
        PlanNode old = nodes.get(sourceIndex);
        if (old.kind() != PlanNode.Kind.SOURCE) {
            throw new IllegalArgumentException("node " + sourceIndex + " is not a SOURCE");
        }
        List<PlanNode> copy = new ArrayList<>(nodes);
        copy.set(sourceIndex, old.withSourceSplitBindings(splitBindings));
        return new PlanGraph(copy, parallelism);
    }

    /** Worker-only rebuild of a materialized root SINK with its admitted runtime Sink. */
    public PlanGraph replaceRootSinkOutput(io.rowweave.Sink replacement) {
        if (replacement == null || root().kind() != PlanNode.Kind.SINK
                || root().output() == null) {
            throw new IllegalArgumentException(
                    "root Sink output replacement is invalid");
        }
        PlanNode old = root();
        PlanNode.Builder builder = PlanNode.sink(
                old.outMeta(), old.child(0), replacement);
        copyParams(old, builder);
        List<PlanNode> copy = new ArrayList<>(nodes);
        copy.set(rootIndex(), builder.build());
        return new PlanGraph(copy, parallelism);
    }

    /** Master-only exact-core projection of native Manifest provider configs. */
    public PlanGraph restoreBuiltinManifestOutputs() {
        List<PlanNode> copy = null;
        for (int i = 0; i < nodes.size(); i++) {
            PlanNode old = nodes.get(i);
            if (old.kind() != PlanNode.Kind.SINK
                    || !(old.output()
                    instanceof io.rowweave.ProviderBatchSink)) {
                continue;
            }
            io.rowweave.ProviderBatchSink provider =
                    (io.rowweave.ProviderBatchSink) old.output();
            if (!"rowweave.manifest-file".equals(
                    provider.batchSinkDescriptor().connectorType())
                    || provider.batchSinkDescriptor().connectorVersion() != 1) {
                continue;
            }
            if (copy == null) copy = new ArrayList<>(nodes);
            PlanNode.Builder builder = PlanNode.sink(
                    old.outMeta(), old.child(0),
                    io.rowweave.ManifestFileSink
                            .fromProviderSink(provider));
            copyParams(old, builder);
            builder.param(DURABLE_PROVIDER_BINDING_PARAM, "true");
            copy.set(i, builder.build());
        }
        return copy == null ? this : new PlanGraph(copy, parallelism);
    }

    /**
     * Returns a copy with the node at {@code index} replaced by {@code replacement}. The graph stays
     * immutable and indexes stay stable, because only one slot changes and nothing is added, removed or
     * renumbered. An optimizer rule uses this to attach a derived annotation, by calling
     * {@code node.withParam(...)} and putting the result back in place. {@code replacement} must keep the
     * same kind and childIndices as the node it replaces, since a rule changes a param and not the
     * structure; the caller is responsible for that.
     */
    public PlanGraph withNodeReplaced(int index, PlanNode replacement) {
        List<PlanNode> copy = new ArrayList<>(nodes);
        copy.set(index, replacement);
        return new PlanGraph(copy, parallelism);  // P carried through
    }

    /**
     * Removes the **UNARY** node at {@code removeIdx}, reconnecting its parent to its only child, shifting
     * every index above removeIdx down by one and redirecting every reference to removeIdx to its child.
     * Returns a new graph with the same structure but one fewer node.
     *
     * <p>**High risk of a wrong result** (one mistake in reconnection or renumbering silently changes what the
     * job computes), so only UNARY nodes are allowed, where the single child makes the meaning obvious —
     * short-circuiting the edge through it. SOURCE (no child) and BINARY (two children, so the short-circuit is
     * ambiguous) are rejected. The post-order topology is preserved, meaning a child index always stays below
     * its own index.
     */
    public PlanGraph withNodeRemoved(int removeIdx) {
        PlanNode removed = nodes.get(removeIdx);
        if (removed.kind() != PlanNode.Kind.UNARY) {
            throw new IllegalArgumentException("withNodeRemoved only removes a UNARY node (single child); got "
                    + removed.kind() + " at " + removeIdx);
        }
        int childOfRemoved = removed.child(0);
        // Old index to new index: skip removeIdx, shift everything after it down by one, and first
        // redirect any reference to removeIdx at the child that takes its place, before mapping.
        int[] remap = new int[nodes.size()];
        for (int old = 0, neu = 0; old < nodes.size(); old++) {
            if (old == removeIdx) {
                remap[old] = -1;  // no kept node should reference it, since it was redirected; this is a placeholder
            } else {
                remap[old] = neu++;
            }
        }
        List<PlanNode> out = new ArrayList<>(nodes.size() - 1);
        for (int old = 0; old < nodes.size(); old++) {
            if (old == removeIdx) {
                continue;
            }
            out.add(rebuildWithRemappedChildren(nodes.get(old), removeIdx, childOfRemoved, remap));
        }
        return new PlanGraph(out, parallelism);  // P carried through
    }

    /** Rebuilds a node's child references through remap: a reference to the removed node points at that node's child instead, then every reference is mapped to its new index. A SOURCE has no children and is copied as is. */
    private static PlanNode rebuildWithRemappedChildren(PlanNode n, int removeIdx, int childOfRemoved, int[] remap) {
        if (n.kind() == PlanNode.Kind.SOURCE) {
            return n;  // immutable with no children, reused as is
        }
        int[] oldChildren = n.childIndices();
        int[] newChildren = new int[oldChildren.length];
        for (int k = 0; k < oldChildren.length; k++) {
            int ref = oldChildren[k] == removeIdx ? childOfRemoved : oldChildren[k];  // shortcut the removed node
            newChildren[k] = remap[ref];
        }
        PlanNode.Builder b;
        if (n.kind() == PlanNode.Kind.UNARY) {
            b = PlanNode.unary(n.typeId(), n.outMeta(), newChildren[0]);
        } else if (n.kind() == PlanNode.Kind.SINK) {
            // Keep the Sink instance if there is one, otherwise the legacy type string.
            b = (n.output() != null)
                    ? PlanNode.sink(n.outMeta(), newChildren[0], n.output())
                    : PlanNode.sink(n.outMeta(), newChildren[0], n.param(PlanNode.OUTPUT_TYPE_PARAM));  // unary outlet
        } else {
            b = PlanNode.binary(n.typeId(), n.outMeta(), newChildren[0], newChildren[1]);
        }
        copyParams(n, b);
        return b.build();
    }

    public byte[] toBytes() {
        return toBytes(value -> { });
    }

    /** Every backing or copy allocation made by the plan writer and the embedded reader is reserved first. */
    public byte[] toBytes(java.util.function.LongConsumer reservation) {
        return toBytes(ObjectTrustContext.localTrusted(), reservation);
    }

    public byte[] toBytes(ObjectTrustContext trustContext,
                          java.util.function.LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (trustContext == null) throw new IllegalArgumentException("object trust context is required");
        io.rowweave.memory.ResidentByteArrayOutputStream bos =
                new io.rowweave.memory.ResidentByteArrayOutputStream(
                        reservation, 1024, 64 * 1024 * 1024);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(nodes.size());
            for (PlanNode n : nodes) {
                writeNode(out, n, trustContext, reservation);
            }
            out.writeInt(parallelism);  // append P at the end of the frame: an old reader never reads past the body, and a new one reads it only while available() is positive
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        byte[] encoded = bos.toByteArray();
        PlanGraphEnvelopeScanner.scan(encoded, reservation);
        return encoded;
    }

    /**
     * Durable writer entry: rejects every custom/legacy Sink before any object
     * serialization hook in any node can run. Provider proxies use native -4.
     */
    public byte[] toDurableBytes(
            ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation) {
        List<PlanNode> durableNodes = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            PlanNode node = nodes.get(i);
            if (node.kind() != PlanNode.Kind.SINK) {
                durableNodes.add(node);
                continue;
            }
            io.rowweave.Sink output = node.output();
            if (output == null || (output.getClass()
                    != io.rowweave.ProviderBatchSink.class
                    && output.getClass() != io.rowweave.ManifestFileSink.class
                    && output.getClass() != io.rowweave.DiscardSink.class)) {
                throw new IllegalArgumentException(
                        "UNSUPPORTED_DURABLE_SINK_PROVIDER: SINK node=" + i);
            }
            if (output.getClass() == io.rowweave.ManifestFileSink.class) {
                PlanNode.Builder builder = PlanNode.sink(
                        node.outMeta(), node.child(0),
                        ((io.rowweave.ManifestFileSink) output)
                                .toProviderSink());
                copyParams(node, builder);
                durableNodes.add(builder.build());
            } else {
                durableNodes.add(node);
            }
        }
        return new PlanGraph(durableNodes, parallelism)
                .toBytes(trustContext, reservation);
    }

    public static PlanGraph fromBytes(byte[] bytes) {
        return fromBytes(bytes, value -> { });
    }

    public static PlanGraph fromBytes(byte[] bytes,
                                      java.util.function.LongConsumer reservation) {
        return fromBytes(bytes, ObjectTrustContext.localTrusted(), reservation);
    }

    /** A socket or InProcess cluster must be given an explicit trust context and may never fall back to a bare ObjectInputStream. */
    public static PlanGraph fromBytes(byte[] bytes, ObjectTrustContext trustContext,
                                      java.util.function.LongConsumer reservation) {
        return fromBytes(bytes, 0, bytes == null ? 0 : bytes.length, trustContext, reservation);
    }

    public static PlanGraph fromBytes(byte[] bytes, int offset, int length,
                                      ObjectTrustContext trustContext,
                                      java.util.function.LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        if (trustContext == null) throw new IllegalArgumentException("object trust context is required");
        PlanGraphEnvelopeScanner.Scan scan = PlanGraphEnvelopeScanner.scan(
                bytes, offset, length, reservation);
        reservation.accept(scan.residentBytes());
        // The legacy codec still carries each object blob in its own byte array, so the whole reservation
        // must happen before the first blob is copied or read.
        reservation.accept(Math.multiplyExact(3L, scan.objectBlobBytes()));
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(bytes, offset, length))) {
            int n = in.readInt();
            List<PlanNode> nodes = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                nodes.add(readNode(in, trustContext, reservation));
            }
            // P sits at the end of the frame. An old frame has no such field, so available() is zero and
            // the default of 1 is used, which is a reliable reading of ByteArrayInputStream semantics.
            int p = in.available() > 0 ? in.readInt() : 1;
            return new PlanGraph(nodes, p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeNode(DataOutputStream out, PlanNode n,
                                  ObjectTrustContext trustContext,
                                  java.util.function.LongConsumer reservation) throws IOException {
        // Every node is either describable, through typeId and params, or holds a serializable
        // userFunction, so either way the graph can go straight onto the wire.
        // The plan holds no transient reference to an in-memory Stage.
        n.validateCurrentSourceBindings();
        n.validateCurrentSinkBinding();
        out.writeInt(n.kind().ordinal());
        writeUtf(out, n.typeId(), reservation);
        writeMeta(out, n.outMeta(), reservation);

        int[] children = n.childIndices();
        out.writeInt(children.length);
        for (int c : children) {
            out.writeInt(c);
        }

        Map<String, String> params = n.params();
        out.writeInt(params.size());
        for (Map.Entry<String, String> e : params.entrySet()) {
            writeUtf(out, e.getKey(), reservation);
            writeUtf(out, e.getValue(), reservation);
        }

        Map<String, List<String>> listParams = n.listParams();
        out.writeInt(listParams.size());
        for (Map.Entry<String, List<String>> e : listParams.entrySet()) {
            writeUtf(out, e.getKey(), reservation);
            out.writeInt(e.getValue().size());
            for (String v : e.getValue()) {
                writeUtf(out, v, reservation);
            }
        }

        if (n.kind() == PlanNode.Kind.SOURCE) {
            // A SOURCE holds a list of Sources and ships them by Java serialization, since Source is
            // Serializable, as descriptions.
            // The driver materializes no rows; the worker rebuilds the Sources and reads them lazily. The
            // frame is [inputCount] then ([blobLen][ObjectOutputStream bytes]) for each Source.
            List<io.rowweave.Source> inputs = n.inputs();
            out.writeInt(inputs.size());
            for (io.rowweave.Source in : inputs) {
                trustContext.verifyRootForWrite(in, io.rowweave.Source.class);
                io.rowweave.memory.ResidentByteArrayOutputStream bos =
                        new io.rowweave.memory.ResidentByteArrayOutputStream(
                                reservation, 1024, 16 * 1024 * 1024);
                try (java.io.ObjectOutputStream oos =
                             io.rowweave.memory.ResidentObjectOutputStreams.open(
                                     bos, reservation, trustContext::verifyObjectForWrite)) {
                    oos.writeObject(in);
                }
                byte[] blob = bos.toByteArray();
                out.writeInt(blob.length);
                out.write(blob);
            }
        } else if (n.kind() == PlanNode.Kind.SINK
                && n.output() != null
                && n.output().getClass() == ProviderBatchSink.class) {
            // exact core proxy uses native bounded -4 config, never OIS.
            ProviderBatchSink provider =
                    (ProviderBatchSink) n.output();
            byte[] envelope = DurableSinkConfigurationEnvelopeCodec.encode(
                    provider.configurationCodec(), provider.configurationBytes());
            reservation.accept(3L * envelope.length);
            out.writeInt(-4);
            out.writeInt(envelope.length);
            out.write(envelope);
        } else if (n.kind() == PlanNode.Kind.SINK && n.output() != null) {
            // A SINK holds the serialized Sink instance, shipped to the worker by Java serialization;
            // transient writers and connections do not travel with it.
            // The frame is [-2 marker][blobLen][ObjectOutputStream bytes]. The -2 separates this from a
            // SOURCE, whose inputCount is at least zero, and from a legacy SINK or UNARY, marked -1.
            trustContext.verifyRootForWrite(n.output(), io.rowweave.Sink.class);
            io.rowweave.memory.ResidentByteArrayOutputStream bos =
                    new io.rowweave.memory.ResidentByteArrayOutputStream(
                            reservation, 1024, 16 * 1024 * 1024);
            try (java.io.ObjectOutputStream oos =
                         io.rowweave.memory.ResidentObjectOutputStreams.open(
                                 bos, reservation, trustContext::verifyObjectForWrite)) {
                oos.writeObject(n.output());
            }
            byte[] blob = bos.toByteArray();
            out.writeInt(-2);
            out.writeInt(blob.length);
            out.write(blob);
        } else if (n.userFunction() != null) {
            // A UNARY code lambda (filterFn, map, flatMap) holds a serializable userFunction.
            // The frame is [-3 marker][blobLen][ObjectOutputStream of the userFunction]. The -3 collides
            // with neither -1, -2 nor a non-negative inputCount.
            byte[] blob;
            try {
                trustContext.verifyRootForWrite(n.userFunction(), functionRootType(n.typeId()));
                io.rowweave.memory.ResidentByteArrayOutputStream bos =
                        new io.rowweave.memory.ResidentByteArrayOutputStream(
                                reservation, 1024, 16 * 1024 * 1024);
                try (java.io.ObjectOutputStream oos =
                             io.rowweave.memory.ResidentObjectOutputStreams.open(
                                     bos, reservation, trustContext::verifyObjectForWrite)) {
                    oos.writeObject(n.userFunction());
                }
                blob = bos.toByteArray();
            } catch (IOException ioe) {
                // A user lambda that captures something unserializable, or any other serialization
                // failure, fails loudly with the typeId, as on the SOURCE and SINK paths. It surfaces at
                // commit time, before anything is partly accounted. Catching IOException rather than only
                // NotSerializableException keeps the typeId context attached.
                throw new IOException("userFunction not serializable (typeId=" + n.typeId()
                        + "); filter/map/flatMap functions must be stateless and must not capture non-serializable variables", ioe);
            }
            out.writeInt(-3);
            out.writeInt(blob.length);
            out.write(blob);
        } else {
            out.writeInt(-1);
        }
    }

    private static PlanNode readNode(DataInputStream in, ObjectTrustContext trustContext,
                                     java.util.function.LongConsumer reservation) throws IOException {
        PlanNode.Kind kind = PlanNode.Kind.values()[in.readInt()];
        String typeId = readUtf(in);
        DataSchema outMeta = readMeta(in);

        int childCount = in.readInt();
        int[] children = new int[childCount];
        for (int i = 0; i < childCount; i++) {
            children[i] = in.readInt();
        }

        int paramCount = in.readInt();
        PlanNode.Builder builder = null;
        // params/listParams read into temp maps because SOURCE has no builder
        java.util.LinkedHashMap<String, String> params = new java.util.LinkedHashMap<>();
        for (int i = 0; i < paramCount; i++) {
            String k = readUtf(in);
            String v = readUtf(in);
            params.put(k, v);
        }

        int listParamCount = in.readInt();
        java.util.LinkedHashMap<String, List<String>> listParams = new java.util.LinkedHashMap<>();
        for (int i = 0; i < listParamCount; i++) {
            String k = readUtf(in);
            int size = in.readInt();
            List<String> vals = new ArrayList<>(size);
            for (int j = 0; j < size; j++) {
                vals.add(readUtf(in));
            }
            listParams.put(k, vals);
        }

        int inputCount = in.readInt();
        if (kind == PlanNode.Kind.SOURCE) {
            // Deserialize the SOURCE list of Sources, rebuilt by Java serialization, and iterate it
            // lazily on the worker.
            List<io.rowweave.Source> inputs = new ArrayList<>(Math.max(0, inputCount));
            for (int i = 0; i < inputCount; i++) {
                int len = in.readInt();
                byte[] blob = new byte[len];
                in.readFully(blob);
                inputs.add(readTrustedObject(blob, io.rowweave.Source.class,
                        trustContext, reservation, "SOURCE Source"));
            }
            return PlanNode.sourceFromWire(outMeta, inputs, params, listParams);
        }

        // A userFunction trailer, marked -3: deserialize it, then rebuild by kind. params and listParams
        // were already read above.
        // A UNARY with no params is filterFn, map or flatMap, so it becomes unaryFn; a UNARY with params
        // is reduceGroup with its keys, so it becomes unaryFnWithParams; a BINARY is coGroup with its
        // leftKeys and rightKeys, so it becomes binaryFn.
        if (inputCount == -3) {
            int len = in.readInt();
            byte[] blob = new byte[len];
            in.readFully(blob);
            Class<? extends java.io.Serializable> expectedFunction = functionRootType(typeId);
            java.io.Serializable fn = readTrustedObject(blob, expectedFunction,
                    trustContext, reservation, "userFunction (typeId=" + typeId + ")");
            if (kind == PlanNode.Kind.BINARY) {
                return PlanNode.binaryFn(typeId, outMeta, children[0], children[1], params, listParams, fn);
            }
            // UNARY
            if (params.isEmpty() && listParams.isEmpty()) {
                return PlanNode.unaryFn(typeId, outMeta, children[0], fn);
            }
            return PlanNode.unaryFnWithParams(typeId, outMeta, children[0], params, listParams, fn);
        }

        // A Sink instance trailer for a SINK, marked -2: rebuilt by deserialization, and the last stage
        // on the worker feeds it as a stream.
        io.rowweave.Sink sinkOutput = null;
        if (inputCount == -4) {
            int len = in.readInt();
            byte[] frame = new byte[len];
            in.readFully(frame);
            DurableSinkConfigurationEnvelope envelope =
                    DurableSinkConfigurationEnvelopeCodec.decode(frame);
            io.rowweave.BatchSinkDescriptor descriptor =
                    BatchSinkBindings.decodeCanonicalDescriptor(
                            listParams.get(PlanNode.SINK_DESCRIPTOR_PARAM));
            sinkOutput = new ProviderBatchSink(outMeta, descriptor,
                    envelope.configurationCodec(),
                    envelope.configurationBytes());
        } else if (inputCount == -2) {
            int len = in.readInt();
            byte[] blob = new byte[len];
            in.readFully(blob);
            sinkOutput = readTrustedObject(blob, io.rowweave.Sink.class,
                    trustContext, reservation, "SINK Sink");
        }

        if (kind == PlanNode.Kind.UNARY) {
            builder = PlanNode.unary(typeId, outMeta, children[0]);
        } else if (kind == PlanNode.Kind.SINK) {
            // A SINK outlet node with one child. Prefer the deserialized Sink instance, otherwise the
            // legacy type string.
            if (sinkOutput != null) {
                return PlanNode.sinkFromWire(
                        outMeta, children[0], sinkOutput, params, listParams);
            }
            builder = PlanNode.sink(outMeta, children[0],
                    params.getOrDefault(PlanNode.OUTPUT_TYPE_PARAM,
                            PlanNode.OUTPUT_COLLECTING));
        } else {
            builder = PlanNode.binary(typeId, outMeta, children[0], children[1]);
        }
        for (Map.Entry<String, String> e : params.entrySet()) {
            builder.param(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, List<String>> e : listParams.entrySet()) {
            builder.listParam(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    private static <T> T readTrustedObject(
            byte[] blob, Class<T> expectedRoot, ObjectTrustContext trustContext,
            java.util.function.LongConsumer reservation,
            String description) throws IOException {
        try {
            return TrustedObjectInputStream.readReserved(
                    blob, expectedRoot, trustContext, reservation);
        } catch (ClassNotFoundException missing) {
            throw new IOException("failed to deserialize " + description, missing);
        }
    }

    private static Class<? extends java.io.Serializable> functionRootType(String typeId) {
        switch (typeId) {
            case "filterFn": return io.rowweave.FilterFunction.class;
            case "map": return io.rowweave.MapFunction.class;
            case "incrementalMap": return io.rowweave.IncrementalMapFunction.class;
            case "flatMap": return io.rowweave.FlatMapFunction.class;
            case "mapPartition": return io.rowweave.MapPartitionFunction.class;
            // Supported by the public filter(expr, params) API: DataSet freezes it into an exact HashMap
            // while building the graph, so neither an arbitrary Map implementation nor an arbitrary
            // Serializable root object is accepted.
            case "filterParams": return java.util.HashMap.class;
            case "reduceGroup": return io.rowweave.GroupReduceFunction.class;
            case "combineGroup": return io.rowweave.GroupCombineFunction.class;
            case "combineReduceGroup": return io.rowweave.GroupCombineReduceFunction.class;
            case "coGroup": return io.rowweave.CoGroupFunction.class;
            default:
                throw new IllegalArgumentException(
                        "userFunction payload is invalid for operator typeId=" + typeId);
        }
    }

    private static void writeMeta(DataOutputStream out, DataSchema meta,
                                  java.util.function.LongConsumer reservation) throws IOException {
        out.writeInt(meta.getFieldCount());
        for (int i = 0; i < meta.getFieldCount(); i++) {
            Field f = meta.getField(i);
            writeUtf(out, f.getName(), reservation);
            writeUtf(out, f.getAlias(), reservation);
            out.writeInt(f.getDataType().ordinal());
        }
    }

    private static DataSchema readMeta(DataInputStream in) throws IOException {
        int fieldCount = in.readInt();
        Field[] fields = new Field[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            String name = readUtf(in);
            String alias = readUtf(in);
            DataType type = DataType.values()[in.readInt()];
            fields[i] = new Field(name, alias, type);
        }
        return new DataSchema(fields);
    }

    private static void writeUtf(DataOutputStream out, String s,
                                 java.util.function.LongConsumer reservation) throws IOException {
        reservation.accept(3L * s.length());
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > PlanGraphEnvelopeScanner.MAX_UTF8_BYTES) {
            throw new IllegalArgumentException("plan UTF-8 value exceeds 64 KiB");
        }
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readUtf(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
