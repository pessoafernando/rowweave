package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OfflineKeyedStateRescalerTest {
    private static final String SOURCE_ARTIFACT = digest('a');
    private static final String TARGET_ARTIFACT = digest('b');
    private static final String OPERATOR = "aggregate-by-adaptor";
    private static final int MAX_PARALLELISM = 8;

    @TempDir
    Path temporaryDirectory;

    @Test
    void rescaleRewritesAndRestoresEveryKeyGroupExactly() throws Exception {
        StreamStateDescriptor descriptor = descriptor();
        FileCheckpointPayloadStore sourcePayloads = payloads("source");
        List<byte[]> keys = keysByGroup();
        List<StreamCheckpointMember> sourceMembers = new ArrayList<>();
        for (int subtask = 0; subtask < 2; subtask++) {
            int start = subtask * 4;
            int end = start + 3;
            try (ManagedKeyedValueState state = state(
                    SOURCE_ARTIFACT, descriptor, start, end,
                    "source-state-" + subtask)) {
                for (int group = start; group <= end; group++) {
                    state.put(keys.get(group), bytes("ns"),
                            bytes("value-" + group), 100L + group);
                }
                state.advanceWatermark(50L);
                sourceMembers.add(state.snapshot(subtask, sourcePayloads));
            }
        }
        FileCheckpointPayloadStore targetPayloads = payloads("target");
        OfflineKeyedStateRescaler rescaler =
                new OfflineKeyedStateRescaler(
                        temporaryDirectory.resolve("rescale-working"),
                        4L * 1024L * 1024L);

        OfflineKeyedStateRescaler.Result result = rescaler.rescale(
                OfflineKeyedStateRescaler.RescaleMode.LIVE,
                SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR, descriptor,
                MAX_PARALLELISM, 3, sourceMembers,
                sourcePayloads, targetPayloads);

        assertEquals(3, result.targetMembers().size());
        assertRange(result.targetMembers().get(0), 0, 0, 1);
        assertRange(result.targetMembers().get(1), 1, 2, 4);
        assertRange(result.targetMembers().get(2), 2, 5, 7);
        assertEquals(8, result.keyGroups().size());
        for (OfflineKeyedStateRescaler.KeyGroupEvidence evidence
                : result.keyGroups()) {
            assertEquals(1L, evidence.recordCount());
        }

        for (StreamCheckpointMember member : result.targetMembers()) {
            try (ManagedKeyedValueState restored = state(
                    TARGET_ARTIFACT, descriptor,
                    member.keyGroupStart(), member.keyGroupEnd(),
                    "target-state-" + member.subtaskIndex())) {
                restored.restore(
                        member.subtaskIndex(), member, targetPayloads);
                for (int group = member.keyGroupStart();
                     group <= member.keyGroupEnd(); group++) {
                    assertArrayEquals(bytes("value-" + group),
                            restored.get(keys.get(group), bytes("ns")));
                }
            }
        }

        for (StreamCheckpointMember source : sourceMembers) {
            Path restored = temporaryDirectory.resolve(
                    "source-proof-" + source.subtaskIndex());
            sourcePayloads.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            source.contentDigest(), source.contentBytes()),
                    restored);
            assertEquals(source.contentBytes(), Files.size(restored));
        }

        long sourceBytes = 0L;
        for (StreamCheckpointMember source : sourceMembers) {
            sourceBytes += source.contentBytes();
        }
        Path tightTargetRoot = temporaryDirectory.resolve("tight-target");
        FileCheckpointPayloadStore tightTarget =
                new FileCheckpointPayloadStore(
                        tightTargetRoot, 1024L * 1024L);
        OfflineKeyedStateRescaler tight = new OfflineKeyedStateRescaler(
                temporaryDirectory.resolve("tight-working"), sourceBytes);
        assertCode(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                () -> tight.rescale(
                        SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR,
                        descriptor, MAX_PARALLELISM, 3, sourceMembers,
                        sourcePayloads, tightTarget));
        try (java.util.stream.Stream<Path> files =
                     Files.walk(tightTargetRoot)) {
            assertEquals(0L, files.filter(Files::isRegularFile).count());
        }
    }

    @Test
    void invalidScaleAndSourceCoverageFailBeforeTargetPublication()
            throws Exception {
        StreamStateDescriptor descriptor = descriptor();
        FileCheckpointPayloadStore sourcePayloads = payloads("bad-source");
        List<byte[]> keys = keysByGroup();
        StreamCheckpointMember partial;
        try (ManagedKeyedValueState state = state(
                SOURCE_ARTIFACT, descriptor, 0, 3, "partial-state")) {
            state.put(keys.get(0), bytes("ns"), bytes("value"), 0L);
            partial = state.snapshot(0, sourcePayloads);
        }
        Path targetRoot = temporaryDirectory.resolve("bad-target");
        FileCheckpointPayloadStore targetPayloads =
                new FileCheckpointPayloadStore(targetRoot, 1024L * 1024L);
        OfflineKeyedStateRescaler rescaler =
                new OfflineKeyedStateRescaler(
                        temporaryDirectory.resolve("bad-working"),
                        1024L * 1024L);

        assertCode(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                () -> rescaler.rescale(
                        SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR,
                        descriptor, MAX_PARALLELISM, 9,
                        Arrays.asList(partial), sourcePayloads,
                        targetPayloads));
        assertCode(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                () -> rescaler.rescale(
                        SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR,
                        descriptor, MAX_PARALLELISM, 2,
                        Arrays.asList(partial), sourcePayloads,
                        targetPayloads));
        assertCode(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                () -> rescaler.rescale(
                        OfflineKeyedStateRescaler.RescaleMode.LIVE,
                        SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR,
                        descriptor, MAX_PARALLELISM, 2,
                        Arrays.asList(partial), sourcePayloads,
                        targetPayloads));
        try (java.util.stream.Stream<Path> files = Files.walk(targetRoot)) {
            assertEquals(0L, files.filter(Files::isRegularFile).count());
        }
    }

    @Test
    void mapAndListNamespacesRescaleWithEveryFrame() {
        for (StreamStateDescriptor.Kind kind : Arrays.asList(
                StreamStateDescriptor.Kind.KEYED_MAP,
                StreamStateDescriptor.Kind.KEYED_LIST)) {
            String suffix = kind.name().toLowerCase(java.util.Locale.ROOT);
            StreamStateDescriptor descriptor = new StreamStateDescriptor(
                    "state-" + suffix, kind, "bytes-v1", 1,
                    StreamStateDescriptor.TtlMode.NONE, 0L);
            FileCheckpointPayloadStore sourcePayloads = payloads(
                    "source-" + suffix);
            List<byte[]> keys = keysByGroup();
            List<StreamCheckpointMember> sourceMembers = new ArrayList<>();
            for (int subtask = 0; subtask < 2; subtask++) {
                int start = subtask * 4;
                int end = start + 3;
                try (ManagedKeyedValueState state = state(
                        SOURCE_ARTIFACT, descriptor, start, end,
                        "source-" + suffix + '-' + subtask)) {
                    for (int group = start; group <= end; group++) {
                        state.put(keys.get(group), bytes("a"),
                                bytes("first-" + group), 0L);
                        state.put(keys.get(group), bytes("b"),
                                bytes("second-" + group), 0L);
                    }
                    sourceMembers.add(state.snapshot(subtask, sourcePayloads));
                }
            }
            FileCheckpointPayloadStore targets = payloads("target-" + suffix);
            OfflineKeyedStateRescaler.Result result =
                    new OfflineKeyedStateRescaler(
                            temporaryDirectory.resolve(
                                    "working-" + suffix),
                            4L * 1024L * 1024L).rescale(
                            SOURCE_ARTIFACT, TARGET_ARTIFACT, OPERATOR,
                            descriptor, MAX_PARALLELISM, 3, sourceMembers,
                            sourcePayloads, targets);
            assertEquals(3, result.targetMembers().size());
            for (OfflineKeyedStateRescaler.KeyGroupEvidence evidence
                    : result.keyGroups()) {
                assertEquals(2L, evidence.recordCount());
            }
        }
    }

    private ManagedKeyedValueState state(
            String artifact, StreamStateDescriptor descriptor,
            int start, int end, String directory) {
        return new ManagedKeyedValueState(
                artifact, OPERATOR, descriptor, MAX_PARALLELISM,
                start, end, new MemoryManager(4096),
                temporaryDirectory.resolve(directory),
                1024L * 1024L, 128, 1024,
                new AtomicLong(1_000L)::get);
    }

    private FileCheckpointPayloadStore payloads(String name) {
        return new FileCheckpointPayloadStore(
                temporaryDirectory.resolve(name), 1024L * 1024L);
    }

    private static StreamStateDescriptor descriptor() {
        return new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                "canonical-decimal-v1", 1,
                StreamStateDescriptor.TtlMode.EVENT_TIME, 1_000L);
    }

    private static List<byte[]> keysByGroup() {
        List<byte[]> keys = new ArrayList<>();
        for (int group = 0; group < MAX_PARALLELISM; group++) {
            keys.add(keyForGroup(group));
        }
        return keys;
    }

    private static byte[] keyForGroup(int expectedGroup) {
        for (int index = 0; index < 100_000; index++) {
            byte[] key = bytes("key-" + expectedGroup + '-' + index);
            if (ManagedKeyedValueState.keyGroup(key, MAX_PARALLELISM)
                    == expectedGroup) {
                return key;
            }
        }
        throw new AssertionError("no key for group " + expectedGroup);
    }

    private static void assertRange(
            StreamCheckpointMember member, int subtask, int start, int end) {
        assertEquals(subtask, member.subtaskIndex());
        assertEquals(start, member.keyGroupStart());
        assertEquals(end, member.keyGroupEnd());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String digest(char value) {
        char[] characters = new char[64];
        Arrays.fill(characters, value);
        return new String(characters);
    }

    private static void assertCode(
            StreamCheckpointException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code());
    }
}
