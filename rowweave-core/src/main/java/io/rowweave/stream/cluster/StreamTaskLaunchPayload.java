package io.rowweave.stream.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Versioned task-launch metadata carried outside the signed Artifact. */
final class StreamTaskLaunchPayload {
    private static final int MAGIC = 0x52575232; // RWR2
    private static final int VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private static final int HEADER_BYTES = 24;
    private static final int VERSION_2_HEADER_BYTES = 28;
    private static final int MAX_NODE_MINIMUMS = 4_096;
    private static final int MAX_NODE_UID_BYTES = 256;

    private final long channelEpoch;
    private final int parallelismOverride;
    private final Map<String, Integer> nodeMinimumParallelism;
    private final byte[] checkpointFrame;

    private StreamTaskLaunchPayload(long channelEpoch,
            int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism,
            byte[] checkpointFrame) {
        this.channelEpoch = channelEpoch;
        this.parallelismOverride = parallelismOverride;
        this.nodeMinimumParallelism = Collections.unmodifiableMap(
                new LinkedHashMap<>(nodeMinimumParallelism));
        this.checkpointFrame = checkpointFrame == null
                ? null : checkpointFrame.clone();
    }

    static byte[] start(long channelEpoch, int parallelismOverride) {
        return start(channelEpoch, parallelismOverride,
                Collections.emptyMap());
    }

    static byte[] start(long channelEpoch, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        return encode(channelEpoch, parallelismOverride,
                nodeMinimumParallelism, null);
    }

    static byte[] restore(long channelEpoch, int parallelismOverride,
            byte[] checkpointFrame) {
        return restore(channelEpoch, parallelismOverride,
                Collections.emptyMap(), checkpointFrame);
    }

    static byte[] restore(long channelEpoch, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism,
            byte[] checkpointFrame) {
        if (checkpointFrame == null || checkpointFrame.length == 0) {
            throw new IllegalArgumentException(
                    "Stream restore checkpoint frame is required");
        }
        StreamCheckpointControlCodec.decode(checkpointFrame);
        return encode(channelEpoch, parallelismOverride,
                nodeMinimumParallelism, checkpointFrame);
    }

    private static byte[] encode(long channelEpoch, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism,
            byte[] checkpointFrame) {
        if (channelEpoch < 1L || parallelismOverride < 0
                || nodeMinimumParallelism == null
                || nodeMinimumParallelism.size() > MAX_NODE_MINIMUMS) {
            throw new IllegalArgumentException(
                    "Stream task launch generation is invalid");
        }
        TreeMap<String, Integer> minimums = new TreeMap<>();
        int minimumBytes = 0;
        for (Map.Entry<String, Integer> entry
                : nodeMinimumParallelism.entrySet()) {
            byte[] uid = entry.getKey() == null ? null
                    : entry.getKey().getBytes(StandardCharsets.UTF_8);
            if (uid == null || uid.length < 1
                    || uid.length > MAX_NODE_UID_BYTES
                    || entry.getValue() == null
                    || entry.getValue() < 1 || entry.getValue() > 4_096
                    || minimums.put(entry.getKey(), entry.getValue())
                    != null) {
                throw new IllegalArgumentException(
                        "Stream task launch node minimum is invalid");
            }
            minimumBytes = Math.addExact(minimumBytes,
                    Math.addExact(8, uid.length));
        }
        int checkpointBytes = checkpointFrame == null
                ? 0 : checkpointFrame.length;
        if (checkpointBytes > StreamControlMessage.MAX_PAYLOAD_BYTES
                - VERSION_2_HEADER_BYTES - minimumBytes) {
            throw new IllegalArgumentException(
                    "Stream task launch checkpoint exceeds its bound");
        }
        ByteBuffer output = ByteBuffer.allocate(
                VERSION_2_HEADER_BYTES + minimumBytes + checkpointBytes)
                .putLong(channelEpoch).putInt(MAGIC).putInt(VERSION)
                .putInt(parallelismOverride).putInt(minimums.size())
                .putInt(checkpointBytes);
        for (Map.Entry<String, Integer> entry : minimums.entrySet()) {
            byte[] uid = entry.getKey().getBytes(StandardCharsets.UTF_8);
            output.putInt(uid.length).put(uid).putInt(entry.getValue());
        }
        output.put(checkpointFrame == null ? new byte[0] : checkpointFrame);
        return output.array();
    }

    static StreamTaskLaunchPayload decode(byte[] payload,
            StreamControlMessage.Kind kind) {
        if (payload == null || payload.length < Long.BYTES
                || (kind != StreamControlMessage.Kind.START
                && kind != StreamControlMessage.Kind.RESTORE)) {
            throw invalid();
        }
        ByteBuffer input = ByteBuffer.wrap(payload);
        long channelEpoch = input.getLong();
        if (channelEpoch < 1L) throw invalid();
        if (payload.length == Long.BYTES) {
            if (kind != StreamControlMessage.Kind.START) throw invalid();
            return new StreamTaskLaunchPayload(channelEpoch, 0,
                    Collections.emptyMap(), null);
        }
        if (payload.length >= HEADER_BYTES
                && input.getInt(input.position()) == MAGIC) {
            input.getInt();
            int version = input.getInt();
            int parallelismOverride = input.getInt();
            int minimumCount = version == VERSION ? input.getInt() : 0;
            int checkpointBytes = input.getInt();
            if (version != VERSION && version != LEGACY_VERSION
                    || parallelismOverride < 0 || minimumCount < 0
                    || minimumCount > MAX_NODE_MINIMUMS
                    || checkpointBytes < 0) {
                throw invalid();
            }
            Map<String, Integer> minimums = new LinkedHashMap<>();
            for (int index = 0; index < minimumCount; index++) {
                if (input.remaining() < 8) throw invalid();
                int uidBytes = input.getInt();
                if (uidBytes < 1 || uidBytes > MAX_NODE_UID_BYTES
                        || uidBytes > input.remaining() - 4) throw invalid();
                byte[] uid = new byte[uidBytes];
                input.get(uid);
                int minimum = input.getInt();
                String operatorUid = new String(uid, StandardCharsets.UTF_8);
                if (minimum < 1 || minimum > 4_096
                        || minimums.put(operatorUid, minimum) != null) {
                    throw invalid();
                }
            }
            if (checkpointBytes != input.remaining()) throw invalid();
            byte[] checkpoint = checkpointBytes == 0
                    ? null : new byte[checkpointBytes];
            if (checkpoint != null) input.get(checkpoint);
            if ((kind == StreamControlMessage.Kind.START)
                    != (checkpoint == null)) {
                throw invalid();
            }
            if (checkpoint != null) {
                StreamCheckpointControlCodec.decode(checkpoint);
            }
            return new StreamTaskLaunchPayload(
                    channelEpoch, parallelismOverride, minimums, checkpoint);
        }
        if (kind != StreamControlMessage.Kind.RESTORE) throw invalid();
        byte[] legacyCheckpoint = Arrays.copyOfRange(
                payload, Long.BYTES, payload.length);
        StreamCheckpointControlCodec.decode(legacyCheckpoint);
        return new StreamTaskLaunchPayload(
                channelEpoch, 0, Collections.emptyMap(), legacyCheckpoint);
    }

    long channelEpoch() { return channelEpoch; }
    int parallelismOverride() { return parallelismOverride; }
    Map<String, Integer> nodeMinimumParallelism() {
        return nodeMinimumParallelism;
    }
    boolean restoring() { return checkpointFrame != null; }
    byte[] checkpointFrame() {
        return checkpointFrame == null ? null : checkpointFrame.clone();
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException(
                "Stream task launch payload is invalid");
    }
}
