package io.rowweave.deployment;

import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateMigrationBinding;
import io.rowweave.stream.runtime.FileCheckpointChunkStore;
import io.rowweave.stream.runtime.StreamCheckpointException;
import io.rowweave.stream.runtime.StreamStateShadowMigrator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamStateShadowMigratorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void signedMigrationPublishesVerifiedStateOnlyToShadow() throws Exception {
        try (SignedStreamStateMigrationResolverTest.Fixture fixture =
                     SignedStreamStateMigrationResolverTest.openFixture(
                             temporaryDirectory.resolve("deployment"))) {
            FileCheckpointChunkStore sourceStore = store("source");
            FileCheckpointChunkStore shadowStore = store("shadow");
            Path sourceFile = temporaryDirectory.resolve("source.bin");
            Files.write(sourceFile, bytes("readout"));
            FileCheckpointChunkStore.StoredChunkedPayload sourcePayload =
                    sourceStore.store(sourceFile);
            StreamCheckpointMember sourceMember = sourceMember(sourcePayload);
            StreamStateShadowMigrator migrator = new StreamStateShadowMigrator(
                    temporaryDirectory.resolve("working"), sourceStore,
                    shadowStore, fixture.resolver(),
                    ProviderActionSupervisor.processWide(4), 5_000L);

            StreamCheckpointMember target = migrator.migrate(
                    fixture.binding(), sourceMember, sourcePayload);

            assertEquals("decimal-v2", target.codecId());
            assertEquals(2, target.schemaVersion());
            assertEquals(fixture.binding().targetContentDigest(),
                    target.contentDigest());
            FileCheckpointChunkStore reopened = store("shadow");
            Path restoredTarget = temporaryDirectory.resolve("target.bin");
            reopened.restore(reopened.get(target.contentDigest()), restoredTarget);
            assertArrayEquals(bytes("READOUT"),
                    Files.readAllBytes(restoredTarget));

            Path restoredSource = temporaryDirectory.resolve("old-source.bin");
            sourceStore.restore(sourceStore.get(sourcePayload.digest()),
                    restoredSource);
            assertArrayEquals(bytes("readout"),
                    Files.readAllBytes(restoredSource));
        }
    }

    @Test
    void targetDigestMismatchLeavesShadowUnpublishedAndSourceRecoverable()
            throws Exception {
        try (SignedStreamStateMigrationResolverTest.Fixture fixture =
                     SignedStreamStateMigrationResolverTest.openFixture(
                             temporaryDirectory.resolve("bad-deployment"))) {
            FileCheckpointChunkStore sourceStore = store("bad-source");
            FileCheckpointChunkStore shadowStore = store("bad-shadow");
            Path sourceFile = temporaryDirectory.resolve("bad-source.bin");
            Files.write(sourceFile, bytes("readout"));
            FileCheckpointChunkStore.StoredChunkedPayload sourcePayload =
                    sourceStore.store(sourceFile);
            StreamStateMigrationBinding exact = fixture.binding();
            StreamStateMigrationBinding wrongDigest =
                    new StreamStateMigrationBinding(
                            exact.operatorUid(), exact.sourceState(),
                            exact.targetState(), exact.deploymentSetId(),
                            exact.deploymentId(), exact.deploymentJarDigest(),
                            exact.adapterClassName(), digest('f'),
                            exact.maximumOutputBytes());
            StreamStateShadowMigrator migrator = new StreamStateShadowMigrator(
                    temporaryDirectory.resolve("bad-working"), sourceStore,
                    shadowStore, fixture.resolver(),
                    ProviderActionSupervisor.processWide(4), 5_000L);

            assertCode(StreamCheckpointException.Code
                            .STATE_MIGRATION_OUTPUT_MISMATCH,
                    () -> migrator.migrate(
                            wrongDigest, sourceMember(sourcePayload),
                            sourcePayload));
            assertNull(shadowStore.get(digest('f')));

            Path restored = temporaryDirectory.resolve("source-still-valid.bin");
            sourceStore.restore(sourcePayload, restored);
            assertArrayEquals(bytes("readout"), Files.readAllBytes(restored));

            FileCheckpointChunkStore nonDeterministicShadow =
                    store("non-deterministic-shadow");
            StreamStateShadowMigrator nonDeterministic =
                    new StreamStateShadowMigrator(
                            temporaryDirectory.resolve(
                                    "non-deterministic-working"),
                            sourceStore, nonDeterministicShadow,
                            fixture.resolver(),
                            ProviderActionSupervisor.processWide(4), 5_000L);
            assertCode(StreamCheckpointException.Code
                            .STATE_MIGRATION_NON_DETERMINISTIC,
                    () -> nonDeterministic.migrate(
                            fixture.nonDeterministicBinding(),
                            sourceMember(sourcePayload), sourcePayload));
            assertNull(nonDeterministicShadow.get(
                    fixture.binding().targetContentDigest()));
        }
    }

    private FileCheckpointChunkStore store(String name) {
        return new FileCheckpointChunkStore(
                temporaryDirectory.resolve(name), 4, 4096L, 1024);
    }

    private static StreamCheckpointMember sourceMember(
            FileCheckpointChunkStore.StoredChunkedPayload payload) {
        return StreamCheckpointMember.stateHandle(
                "ledger", 0, 0, 127, "readout", "decimal-v1", 1,
                payload.digest(), payload.contentBytes());
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
}
