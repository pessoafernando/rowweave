package io.rowweave.cluster;

import io.rowweave.ProviderBatchSink;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkLocatorCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

/** Canonical bounded Worker command for rebuilding a provider prepared group. */
final class ProviderPreparedGroupCommandCodec {
    static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int MAGIC = 0x52575047; // RWPG
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;

    enum Action { PUBLISH, ABORT, FINALIZE, QUERY }

    private ProviderPreparedGroupCommandCodec() { }

    static byte[] encode(Command command) {
        if (command == null) throw new IllegalArgumentException("provider group command is required");
        List<PlanNode> nodes = new ArrayList<>(command.outputs.size() * 2);
        for (ProviderBatchSink output : command.outputs) {
            int source = nodes.size();
            nodes.add(PlanNode.placeholderSource(output.getSchema()));
            nodes.add(PlanNode.sink(output.getSchema(), source, output).build());
        }
        byte[] graph = new PlanGraph(nodes).toBytes(
                ObjectTrustContext.loopbackCluster(), ignored -> { });
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(graph.length + 1024);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeByte(command.action.ordinal());
            out.writeInt(graph.length);
            out.write(graph);
            out.writeInt(command.locators.size());
            for (PreparedSinkLocator locator : command.locators) {
                byte[] frame = PreparedSinkLocatorCodec.encode(locator);
                out.writeInt(frame.length);
                out.write(frame);
            }
            out.writeInt(command.authorityDecisionDigest.length);
            out.write(command.authorityDecisionDigest);
            out.flush();
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
                throw new IllegalArgumentException("provider group command exceeds bound");
            }
            byte[] frame = Arrays.copyOf(body, body.length + DIGEST_BYTES);
            byte[] checksum = sha256(body);
            System.arraycopy(checksum, 0, frame, body.length, checksum.length);
            return frame;
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    static Command decode(byte[] frame, LongConsumer reservation) {
        if (frame == null || reservation == null || frame.length < 64
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("provider group command length is invalid");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        if (!MessageDigest.isEqual(sha256(Arrays.copyOf(frame, bodyLength)),
                Arrays.copyOfRange(frame, bodyLength, frame.length))) {
            throw new IllegalArgumentException("provider group command checksum mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(frame, 0, bodyLength));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException("provider group command header is invalid");
            }
            int actionId = in.readUnsignedByte();
            if (actionId >= Action.values().length) {
                throw new IllegalArgumentException("provider group action is invalid");
            }
            int graphLength = bounded(in.readInt(), in.available(), "graph");
            reservation.accept(Math.multiplyExact(3L, graphLength));
            byte[] graphBytes = new byte[graphLength];
            in.readFully(graphBytes);
            PlanGraph graph = PlanGraph.fromBytes(
                    graphBytes, ObjectTrustContext.loopbackCluster(), reservation);
            List<ProviderBatchSink> outputs = new ArrayList<>();
            for (int i = 0; i < graph.nodeCount(); i++) {
                PlanNode node = graph.node(i);
                if (node.kind() != PlanNode.Kind.SINK) continue;
                if (node.output() == null || node.output().getClass()
                        != ProviderBatchSink.class) {
                    throw new IllegalArgumentException(
                            "provider group contains non-provider output");
                }
                outputs.add((ProviderBatchSink) node.output());
            }
            int count = in.readInt();
            if (count < 1 || count > 4095 || count != outputs.size()) {
                throw new IllegalArgumentException("provider group member count is invalid");
            }
            List<PreparedSinkLocator> locators = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int locatorLength = bounded(in.readInt(), in.available(), "locator");
                if (locatorLength > PreparedSinkLocatorCodec.MAX_FRAME_BYTES) {
                    throw new IllegalArgumentException("provider locator exceeds bound");
                }
                reservation.accept(Math.multiplyExact(3L, locatorLength));
                byte[] locator = new byte[locatorLength];
                in.readFully(locator);
                locators.add(PreparedSinkLocatorCodec.decode(locator));
            }
            int decisionLength = in.readInt();
            if (decisionLength != 0 && decisionLength != DIGEST_BYTES) {
                throw new IllegalArgumentException("provider decision digest is invalid");
            }
            if (decisionLength > in.available()) {
                throw new IllegalArgumentException("provider decision digest is truncated");
            }
            byte[] decision = new byte[decisionLength];
            in.readFully(decision);
            if (in.available() != 0) {
                throw new IllegalArgumentException("provider group command has trailing bytes");
            }
            Command decoded = new Command(Action.values()[actionId], outputs, locators, decision);
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException("provider group command is noncanonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("provider group command is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException("provider group command is malformed", malformed);
        }
    }

    private static int bounded(int length, int remaining, String field) {
        if (length < 1 || length > remaining || length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("provider group " + field + " length is invalid");
        }
        return length;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static final class Command {
        final Action action;
        final List<ProviderBatchSink> outputs;
        final List<PreparedSinkLocator> locators;
        final byte[] authorityDecisionDigest;

        Command(Action action, List<ProviderBatchSink> outputs,
                List<PreparedSinkLocator> locators, byte[] authorityDecisionDigest) {
            if (action == null || outputs == null || locators == null
                    || outputs.isEmpty() || outputs.size() != locators.size()
                    || outputs.size() > 4095 || authorityDecisionDigest == null
                    || authorityDecisionDigest.length != 0
                    && authorityDecisionDigest.length != DIGEST_BYTES) {
                throw new IllegalArgumentException("provider group command fields are invalid");
            }
            this.action = action;
            this.outputs = Collections.unmodifiableList(new ArrayList<>(outputs));
            this.locators = Collections.unmodifiableList(new ArrayList<>(locators));
            this.authorityDecisionDigest = authorityDecisionDigest.clone();
        }
    }
}
