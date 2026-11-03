package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded durable managed-state checkpoint cut and restore. */
class ManagedKeyedStateCheckpointTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void hardCrashRestorePreservesStateAndEventTimeCut() {
        Path payloadRoot = temporaryDirectory.resolve("checkpoints");
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(payloadRoot, 256 * 1024L);
        StreamStateDescriptor descriptor = descriptor(
                StreamStateDescriptor.TtlMode.EVENT_TIME, 100L);
        byte[] liveKey = bytes("live-adaptor");
        byte[] removedKey = bytes("removed-adaptor");
        StreamCheckpointMember member;

        try (ManagedKeyedValueState original = state(
                descriptor, temporaryDirectory.resolve("attempt-1"),
                new AtomicLong())) {
            original.put(liveKey, bytes("window"), bytes("12.3400000000"),
                    1_000L);
            original.put(removedKey, bytes("window"), bytes("removed"),
                    1_000L);
            original.remove(removedKey, bytes("window"));
            original.advanceWatermark(1_050L);
            member = original.snapshot(3, payloads);
            assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
            assertEquals("aggregate-by-adaptor", member.operatorUid());
            assertEquals(3, member.subtaskIndex());
            assertEquals("readout", member.subjectId());
            assertTrue(member.contentBytes() > 0L);
        }

        try (ManagedKeyedValueState replacement = state(
                descriptor, temporaryDirectory.resolve("attempt-2"),
                new AtomicLong())) {
            replacement.restore(3, member, payloads);
            assertArrayEquals(bytes("12.3400000000"),
                    replacement.get(liveKey, bytes("window")));
            assertNull(replacement.get(removedKey, bytes("window")));
            replacement.advanceWatermark(1_099L);
            assertArrayEquals(bytes("12.3400000000"),
                    replacement.get(liveKey, bytes("window")));
            replacement.advanceWatermark(1_100L);
            assertNull(replacement.get(liveKey, bytes("window")));
        }
    }

    @Test
    void restorePreservesProcessingTimeFloorAcrossClockRollback() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("processing-checkpoints"),
                256 * 1024L);
        StreamStateDescriptor descriptor = descriptor(
                StreamStateDescriptor.TtlMode.PROCESSING_TIME, 50L);
        AtomicLong originalClock = new AtomicLong(5_000L);
        StreamCheckpointMember member;
        try (ManagedKeyedValueState original = state(
                descriptor, temporaryDirectory.resolve("processing-attempt-1"),
                originalClock)) {
            original.put(bytes("key"), bytes("ns"), bytes("value"), 0L);
            originalClock.set(5_049L);
            assertArrayEquals(bytes("value"),
                    original.get(bytes("key"), bytes("ns")));
            member = original.snapshot(0, payloads);
        }

        AtomicLong rolledBackClock = new AtomicLong(1L);
        try (ManagedKeyedValueState replacement = state(
                descriptor, temporaryDirectory.resolve("processing-attempt-2"),
                rolledBackClock)) {
            replacement.restore(0, member, payloads);
            assertArrayEquals(bytes("value"),
                    replacement.get(bytes("key"), bytes("ns")));
            rolledBackClock.set(5_050L);
            assertNull(replacement.get(bytes("key"), bytes("ns")));
        }
    }

    @Test
    void corruptPayloadFailsBeforeAnyStateBecomesVisible() throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("corrupt-checkpoints"),
                256 * 1024L);
        StreamStateDescriptor descriptor = descriptor(
                StreamStateDescriptor.TtlMode.NONE, 0L);
        StreamCheckpointMember member;
        try (ManagedKeyedValueState original = state(
                descriptor, temporaryDirectory.resolve("corrupt-attempt-1"),
                new AtomicLong())) {
            original.put(bytes("key"), bytes("ns"), bytes("original"), 0L);
            member = original.snapshot(0, payloads);
        }
        Files.write(payloads.payloadPath(member.contentDigest()),
                new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);

        try (ManagedKeyedValueState replacement = state(
                descriptor, temporaryDirectory.resolve("corrupt-attempt-2"),
                new AtomicLong())) {
            StreamCheckpointException corrupt = assertThrows(
                    StreamCheckpointException.class,
                    () -> replacement.restore(0, member, payloads));
            assertEquals(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                    corrupt.code());
            assertEquals(0L, replacement.spillBytes());
            assertNull(replacement.get(bytes("key"), bytes("ns")));
            replacement.put(bytes("new-key"), bytes("ns"), bytes("usable"),
                    0L);
            assertArrayEquals(bytes("usable"),
                    replacement.get(bytes("new-key"), bytes("ns")));
        }
    }

    @Test
    void wrongMemberIdentityAndPayloadLimitFailWithoutPublication() throws Exception {
        Path payloadRoot = temporaryDirectory.resolve("bounded-checkpoints");
        FileCheckpointPayloadStore tinyPayloads =
                new FileCheckpointPayloadStore(payloadRoot, 32L);
        StreamStateDescriptor descriptor = descriptor(
                StreamStateDescriptor.TtlMode.NONE, 0L);
        try (ManagedKeyedValueState original = state(
                descriptor, temporaryDirectory.resolve("bounded-attempt"),
                new AtomicLong())) {
            original.put(bytes("key"), bytes("ns"), bytes("value"), 0L);
            StreamCheckpointException limited = assertThrows(
                    StreamCheckpointException.class,
                    () -> original.snapshot(0, tinyPayloads));
            assertEquals(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                    limited.code());
            assertArrayEquals(bytes("value"),
                    original.get(bytes("key"), bytes("ns")));
        }
        try (Stream<Path> payloadFiles = Files.list(
                payloadRoot.resolve("payloads"))) {
            assertEquals(0L, payloadFiles.count());
        }

        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("identity-checkpoints"),
                256 * 1024L);
        StreamCheckpointMember member;
        try (ManagedKeyedValueState original = state(
                descriptor, temporaryDirectory.resolve("identity-attempt-1"),
                new AtomicLong())) {
            member = original.snapshot(0, payloads);
        }
        StreamCheckpointMember wrongOperator = StreamCheckpointMember.stateHandle(
                "other-operator", 0, 0, 0, member.subjectId(),
                member.codecId(), member.schemaVersion(),
                member.contentDigest(), member.contentBytes());
        try (ManagedKeyedValueState replacement = state(
                descriptor, temporaryDirectory.resolve("identity-attempt-2"),
                new AtomicLong())) {
            StreamStateException mismatch = assertThrows(
                    StreamStateException.class,
                    () -> replacement.restore(0, wrongOperator, payloads));
            assertEquals(StreamStateException.Code.CORRUPT_SNAPSHOT,
                    mismatch.code());
            assertEquals(0L, replacement.spillBytes());
        }
    }

    private ManagedKeyedValueState state(
            StreamStateDescriptor descriptor, Path spillRoot,
            AtomicLong processingClock) {
        return new ManagedKeyedValueState(
                "artifact-1", "aggregate-by-adaptor", descriptor,
                1, 0, 0, new MemoryManager(512), spillRoot,
                256 * 1024L, 128, 1024, processingClock::get);
    }

    private static StreamStateDescriptor descriptor(
            StreamStateDescriptor.TtlMode ttlMode, long ttlMillis) {
        return new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                "canonical-decimal-v1", 1, ttlMode, ttlMillis);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
