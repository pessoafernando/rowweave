package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileOfflineRescaleAuthorityTest {
    private static final String SOURCE_ARTIFACT = digest('a');
    private static final String TARGET_ARTIFACT = digest('b');
    private static final String OPERATOR = "aggregate-by-adaptor";

    @TempDir
    Path temporaryDirectory;

    @Test
    void verifiedRescaleCutsOverOnceAndRejectsStaleGeneration()
            throws Exception {
        Fixture fixture = fixture("valid");
        FileOfflineRescaleAuthority authority =
                new FileOfflineRescaleAuthority(
                        temporaryDirectory.resolve("authority"),
                        fixture.targetPayloads);

        FileOfflineRescaleAuthority.CompletedRescale completed =
                authority.cutover("rescale-1", 1L, digest('c'),
                        TARGET_ARTIFACT, 2, 2, fixture.result);

        assertEquals("rescale-1", completed.rescaleId());
        assertEquals(1L, completed.generation());
        assertEquals(digest('c'), completed.sourceSavepointDigest());
        assertEquals(fixture.result.targetMembers(),
                completed.targetMembers());
        assertEquals(fixture.result.keyGroups(), completed.keyGroups());
        assertEquals(completed,
                authority.cutover("rescale-1", 1L, digest('c'),
                        TARGET_ARTIFACT, 2, 2, fixture.result));
        assertEquals(completed, new FileOfflineRescaleAuthority(
                temporaryDirectory.resolve("authority"),
                fixture.targetPayloads).active());

        assertCode(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                () -> authority.cutover("stale", 1L, digest('d'),
                        TARGET_ARTIFACT, 2, 2, fixture.result));
        fixture.sourcePayloads.verifyStored(
                new FileCheckpointPayloadStore.StoredPayload(
                        fixture.sourceMember.contentDigest(),
                        fixture.sourceMember.contentBytes()));
    }

    @Test
    void corruptTargetCannotMoveActivePointer() throws Exception {
        Fixture fixture = fixture("corrupt");
        StreamCheckpointMember first =
                fixture.result.targetMembers().get(0);
        Files.write(fixture.targetPayloads.payloadPath(first.contentDigest()),
                new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);
        FileOfflineRescaleAuthority authority =
                new FileOfflineRescaleAuthority(
                        temporaryDirectory.resolve("corrupt-authority"),
                        fixture.targetPayloads);

        assertCode(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                () -> authority.cutover("rescale-1", 1L, digest('c'),
                        TARGET_ARTIFACT, 2, 2, fixture.result));
        assertNull(authority.active());
    }

    @Test
    void verifiedResultCannotBeReboundToAnotherTargetArtifact()
            throws Exception {
        Fixture fixture = fixture("target-rebind");
        FileOfflineRescaleAuthority authority =
                new FileOfflineRescaleAuthority(
                        temporaryDirectory.resolve("target-rebind-authority"),
                        fixture.targetPayloads);

        assertCode(StreamCheckpointException.Code.RESCALE_INCOMPATIBLE,
                () -> authority.cutover("rescale-1", 1L, digest('c'),
                        digest('d'), 2, 2, fixture.result));
        assertNull(authority.active());
    }

    private Fixture fixture(String name) throws Exception {
        StreamStateDescriptor descriptor = new StreamStateDescriptor(
                "readout", StreamStateDescriptor.Kind.KEYED_VALUE,
                "canonical-decimal-v1", 1,
                StreamStateDescriptor.TtlMode.NONE, 0L);
        FileCheckpointPayloadStore sourcePayloads =
                new FileCheckpointPayloadStore(
                        temporaryDirectory.resolve(name + "-source"),
                        1024L * 1024L);
        StreamCheckpointMember source;
        try (ManagedKeyedValueState state = new ManagedKeyedValueState(
                SOURCE_ARTIFACT, OPERATOR, descriptor, 2, 0, 1,
                new MemoryManager(4096),
                temporaryDirectory.resolve(name + "-state"),
                1024L * 1024L, 128, 1024,
                new AtomicLong()::get)) {
            state.put(key(0), bytes("ns"), bytes("zero"), 0L);
            state.put(key(1), bytes("ns"), bytes("one"), 0L);
            source = state.snapshot(0, sourcePayloads);
        }
        FileCheckpointPayloadStore targetPayloads =
                new FileCheckpointPayloadStore(
                        temporaryDirectory.resolve(name + "-target"),
                        1024L * 1024L);
        OfflineKeyedStateRescaler.Result result =
                new OfflineKeyedStateRescaler(
                        temporaryDirectory.resolve(name + "-working"),
                        4L * 1024L * 1024L)
                        .rescale(SOURCE_ARTIFACT, TARGET_ARTIFACT,
                                OPERATOR, descriptor, 2, 2,
                                Collections.singletonList(source),
                                sourcePayloads, targetPayloads);
        return new Fixture(sourcePayloads, targetPayloads, source, result);
    }

    private static byte[] key(int group) {
        for (int index = 0; index < 10_000; index++) {
            byte[] key = bytes("key-" + group + '-' + index);
            if (ManagedKeyedValueState.keyGroup(key, 2) == group) return key;
        }
        throw new AssertionError("no key for group " + group);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void assertCode(
            StreamCheckpointException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code());
    }

    private static final class Fixture {
        private final FileCheckpointPayloadStore sourcePayloads;
        private final FileCheckpointPayloadStore targetPayloads;
        private final StreamCheckpointMember sourceMember;
        private final OfflineKeyedStateRescaler.Result result;

        private Fixture(
                FileCheckpointPayloadStore sourcePayloads,
                FileCheckpointPayloadStore targetPayloads,
                StreamCheckpointMember sourceMember,
                OfflineKeyedStateRescaler.Result result) {
            this.sourcePayloads = sourcePayloads;
            this.targetPayloads = targetPayloads;
            this.sourceMember = sourceMember;
            this.result = result;
        }
    }
}
