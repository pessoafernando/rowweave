package io.rowweave.upgrade;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUpgradeAuthorityTest {
    @TempDir
    Path temp;

    @Test
    void shadowCopyVerifyAndAckLossCutoverExposeCompleteAuthority()
            throws Exception {
        Path source = temp.resolve("namespaces/legacy");
        Files.createDirectories(source.resolve("shard-0"));
        Files.write(source.resolve("shard-0/run.frame"),
                "run-v1".getBytes(StandardCharsets.UTF_8));
        Files.write(source.resolve("frontier.frame"),
                "frontier-v1".getBytes(StandardCharsets.UTF_8));

        FileUpgradeAuthority authority = new FileUpgradeAuthority(temp);
        UpgradeOwner owner = new UpgradeOwner(3L, "master-a", "inc-a", 5L);
        UpgradeAuthorityRecord idle = authority.bootstrap(
                new StoreFormatHeader(1, "store-a", 1, 0, digest('a')),
                new StoreFormatHeader(1, "store-a", 1, 2, digest('b')),
                8L, "legacy", "shadow-8", owner, 5_000L);
        assertEquals(UpgradePhase.IDLE, idle.phase());
        assertTrue(idle.writesAllowed());
        assertTrue(Files.isRegularFile(temp.resolve(
                ".rowweave-upgrade-v1/store-header.frame")));

        UpgradeAuthorityRecord frozen = accepted(authority.apply(command(
                idle, UpgradeCommand.Kind.FREEZE_ADMISSION, "freeze",
                idle.activePointerVersion(), 0L, 0, 5_000L),
                1L, 1L, 1_000L));
        UpgradeCommand quiesce = command(frozen, UpgradeCommand.Kind.QUIESCE,
                "q", frozen.activePointerVersion(), 0L, 0, 5_000L);
        assertEquals(UpgradeTransition.Code.ACTIVE_WORK_REMAINS,
                authority.apply(quiesce, 1L, 0L, 1_000L).code());
        UpgradeAuthorityRecord quiesced = accepted(
                authority.apply(quiesce, 0L, 0L, 1_000L));
        UpgradeCommand copy = command(quiesced, UpgradeCommand.Kind.BEGIN_COPY,
                "copy", quiesced.activePointerVersion(), 0L, 0, 5_000L);
        UpgradeAuthorityRecord copying = accepted(
                authority.apply(copy, 0L, 0L, 1_000L));

        NamespaceVerification evidence = authority.copyAndVerifyShadow(copy);
        assertEquals(2L, evidence.objectCount());
        assertEquals(copying.sourceDigest(), evidence.sourceDigest());
        assertEquals(copying.shadowDigest(), evidence.targetDigest());
        assertTrue(Files.isRegularFile(temp.resolve(
                "namespaces/shadow-8/shard-0/run.frame")));
        UpgradeObjectEvidence runEvidence = authority.copyEvidence(
                copy, "shard-0/run.frame");
        assertEquals("shard-0/run.frame", runEvidence.relativePath());
        assertEquals(copy.commandId(), runEvidence.commandId());
        assertEquals(copy.commandDigest(), runEvidence.commandDigest());
        assertEquals(sha256("run-v1"), runEvidence.sourceDigest());
        assertEquals(sha256("run-v1"), runEvidence.targetDigest());
        assertEquals(evidence.objectCount(), evidence.evidenceCount());

        UpgradeCommand verify = command(copying, UpgradeCommand.Kind.VERIFY,
                "verify", copying.activePointerVersion(),
                evidence.objectCount(), evidence.shardCount(), 5_000L);
        UpgradeAuthorityRecord verified = accepted(
                authority.apply(verify, 0L, 0L, 1_000L));

        AtomicBoolean failAck = new AtomicBoolean(true);
        FileUpgradeAuthority ackLossAuthority = new FileUpgradeAuthority(
                temp, point -> {
                    if (point == FileUpgradeAuthority.PersistencePoint
                            .AFTER_AUTHORITY_REPLACE_BEFORE_ACK
                            && failAck.getAndSet(false)) {
                        throw new SimulatedAckLoss();
                    }
                });
        UpgradeCommand cutover = command(verified, UpgradeCommand.Kind.CUTOVER,
                "cutover", verified.activePointerVersion(),
                evidence.objectCount(), evidence.shardCount(), 5_000L);
        assertThrows(SimulatedAckLoss.class,
                () -> ackLossAuthority.apply(cutover, 0L, 0L, 1_000L));

        FileUpgradeAuthority recovered = new FileUpgradeAuthority(temp);
        UpgradeAuthorityRecord after = recovered.read();
        assertEquals(UpgradePhase.CUTOVER, after.phase());
        assertEquals("shadow-8", after.activeNamespace());
        assertEquals(1L, after.activePointerVersion());
        assertEquals(2L, after.writerEpoch());
        assertFalse(after.writesAllowed());
        assertEquals(UpgradeTransition.Code.REPLAYED,
                recovered.apply(cutover, 0L, 0L, 1_000L).code());
        assertTrue(Files.exists(source));
    }

    @Test
    void corruptAuthorityFailsClosedBeforeMutation() throws Exception {
        Path source = temp.resolve("namespaces/legacy");
        Files.createDirectories(source);
        Files.write(source.resolve("value"), new byte[]{1});
        FileUpgradeAuthority authority = new FileUpgradeAuthority(temp);
        UpgradeOwner owner = new UpgradeOwner(3L, "master-a", "inc-a", 5L);
        authority.bootstrap(
                new StoreFormatHeader(1, "store-a", 1, 0, digest('a')),
                new StoreFormatHeader(1, "store-a", 1, 2, digest('b')),
                8L, "legacy", "shadow-8", owner, 5_000L);
        Path record = temp.resolve(
                ".rowweave-upgrade-v1/upgrade-authority.frame");
        byte[] bytes = Files.readAllBytes(record);
        bytes[20] ^= 1;
        Files.write(record, bytes);
        assertThrows(IllegalStateException.class, authority::read);
        assertEquals(1L, Files.size(source.resolve("value")));
    }

    private static UpgradeAuthorityRecord accepted(UpgradeTransition transition) {
        assertEquals(UpgradeTransition.Code.ACCEPTED, transition.code());
        return transition.record();
    }

    private static UpgradeCommand command(
            UpgradeAuthorityRecord record, UpgradeCommand.Kind kind, String id,
            long pointerVersion, long objectCount, int shardCount, long deadline) {
        return new UpgradeCommand(kind, id, "release-2",
                record.activeHeader().storeId(), record.generation(),
                record.sourceNamespace(), record.sourceDigest(),
                record.shadowNamespace(), record.shadowDigest(), record.owner(),
                pointerVersion, objectCount, shardCount,
                record.sourceDigest(), record.shadowDigest(), deadline);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder encoded = new StringBuilder(64);
        for (byte item : digest) encoded.append(String.format("%02x", item & 0xff));
        return encoded.toString();
    }

    private static final class SimulatedAckLoss extends RuntimeException { }
}
