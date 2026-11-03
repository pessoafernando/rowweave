package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamStateMigrationBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileStreamStateMigrationAuthorityTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void cutoverRequiresEveryVerifiedMappingAndSurvivesReopen()
            throws Exception {
        FileCheckpointChunkStore shadow = shadowStore("shadow");
        Target memberA = target(shadow, "ark", "ARK", 0, 63);
        Target memberB = target(shadow, "readout", "READOUT", 64, 127);
        FileStreamStateMigrationAuthority authority =
                new FileStreamStateMigrationAuthority(
                        temporaryDirectory.resolve("authority"), shadow);
        StreamCheckpointMember memberASource =
                source(memberA.binding, 0, 63);
        StreamCheckpointMember memberBSource = source(memberB.binding, 64, 127);

        authority.begin("migration-1", 1L, digest('a'), digest('b'),
                Arrays.asList(
                        FileStreamStateMigrationAuthority.ExpectedState.of(
                                memberA.binding, memberASource),
                        FileStreamStateMigrationAuthority.ExpectedState.of(
                                memberB.binding, memberBSource)));
        authority.acknowledge("migration-1", memberA.binding,
                memberASource, memberA.member);

        assertCode(StreamCheckpointException.Code.STATE_MIGRATION_INCOMPLETE,
                () -> authority.cutover("migration-1"));
        assertNull(authority.active());

        authority.acknowledge("migration-1", memberB.binding,
                memberBSource, memberB.member);
        authority.acknowledge("migration-1", memberA.binding,
                memberASource, memberA.member);
        FileStreamStateMigrationAuthority.CompletedMigration completed =
                authority.cutover("migration-1");

        assertEquals("migration-1", completed.migrationId());
        assertEquals(1L, completed.generation());
        assertEquals(digest('a'), completed.sourceSavepointDigest());
        assertEquals(digest('b'), completed.targetArtifactId());
        assertEquals(Arrays.asList(memberA.member, memberB.member),
                completed.targetMembers());
        assertEquals(completed, authority.cutover("migration-1"));
        assertEquals(completed, new FileStreamStateMigrationAuthority(
                temporaryDirectory.resolve("authority"), shadow).active());

        authority.begin("stale-migration", 1L, digest('f'), digest('e'),
                Collections.singletonList(
                        FileStreamStateMigrationAuthority.ExpectedState.of(
                                memberA.binding, memberASource)));
        assertCode(StreamCheckpointException.Code
                        .STATE_MIGRATION_INCOMPATIBLE,
                () -> authority.cutover("stale-migration"));
    }

    @Test
    void corruptShadowChunkCannotMoveActivePointer() throws Exception {
        FileCheckpointChunkStore shadow = shadowStore("corrupt-shadow");
        Target memberA = target(shadow, "readout", "READOUT", 0, 127);
        FileStreamStateMigrationAuthority authority =
                new FileStreamStateMigrationAuthority(
                        temporaryDirectory.resolve("corrupt-authority"), shadow);
        StreamCheckpointMember memberASource =
                source(memberA.binding, 0, 127);
        authority.begin("migration-2", 1L, digest('a'), digest('b'),
                Collections.singletonList(
                        FileStreamStateMigrationAuthority.ExpectedState.of(
                                memberA.binding, memberASource)));
        authority.acknowledge("migration-2", memberA.binding,
                memberASource, memberA.member);
        Path chunk = shadow.chunkPath(
                memberA.payload.chunks().get(0));
        byte[] corrupt = Files.readAllBytes(chunk);
        corrupt[0] ^= 1;
        Files.write(chunk, corrupt);

        assertCode(StreamCheckpointException.Code.CORRUPT_CHUNK,
                () -> authority.cutover("migration-2"));
        assertNull(authority.active());
    }

    @Test
    void overlappingSourceKeyGroupsAreNotAnExpectedClosure() {
        FileStreamStateMigrationAuthority authority =
                new FileStreamStateMigrationAuthority(
                        temporaryDirectory.resolve("overlap-authority"),
                        shadowStore("overlap-shadow"));
        StreamStateMigrationBinding binding =
                binding("readout", digest('f'));

        assertCode(StreamCheckpointException.Code
                        .STATE_MIGRATION_INCOMPATIBLE,
                () -> authority.begin("migration-overlap", 1L,
                        digest('a'), digest('b'), Arrays.asList(
                                FileStreamStateMigrationAuthority.ExpectedState
                                        .of(binding, source(binding, 0, 63)),
                                FileStreamStateMigrationAuthority.ExpectedState
                                        .of(binding, source(binding, 32, 95)))));
    }

    private Target target(
            FileCheckpointChunkStore shadow, String stateName,
            String content, int keyGroupStart, int keyGroupEnd)
            throws Exception {
        Path file = temporaryDirectory.resolve(stateName + ".bin");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        FileCheckpointChunkStore.StoredChunkedPayload payload =
                shadow.store(file);
        StreamStateMigrationBinding binding = binding(
                stateName, payload.digest());
        StreamCheckpointMember member = StreamCheckpointMember.stateHandle(
                "ledger", 0, keyGroupStart, keyGroupEnd, stateName,
                "decimal-v2", 2, payload.digest(), payload.contentBytes());
        return new Target(binding, member, payload);
    }

    private static StreamCheckpointMember source(
            StreamStateMigrationBinding binding,
            int keyGroupStart, int keyGroupEnd) {
        return StreamCheckpointMember.stateHandle(
                binding.operatorUid(), 0, keyGroupStart, keyGroupEnd,
                binding.sourceState().stateName(),
                binding.sourceState().serializerContractId(),
                binding.sourceState().stateSchemaVersion(), digest('c'), 7L);
    }

    private static StreamStateMigrationBinding binding(
            String stateName, String targetDigest) {
        return new StreamStateMigrationBinding(
                "ledger", state(stateName, "decimal-v1", 1),
                state(stateName, "decimal-v2", 2), digest('d'),
                "state-migration", digest('e'),
                "app.DecimalMigration", targetDigest, 4096L);
    }

    private static StreamStateDescriptor state(
            String name, String serializer, int schemaVersion) {
        return new StreamStateDescriptor(
                name, StreamStateDescriptor.Kind.KEYED_VALUE,
                serializer, schemaVersion,
                StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    private FileCheckpointChunkStore shadowStore(String name) {
        return new FileCheckpointChunkStore(
                temporaryDirectory.resolve(name), 4, 4096L, 1024);
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

    private static final class Target {
        private final StreamStateMigrationBinding binding;
        private final StreamCheckpointMember member;
        private final FileCheckpointChunkStore.StoredChunkedPayload payload;

        private Target(
                StreamStateMigrationBinding binding,
                StreamCheckpointMember member,
                FileCheckpointChunkStore.StoredChunkedPayload payload) {
            this.binding = binding;
            this.member = member;
            this.payload = payload;
        }
    }
}
