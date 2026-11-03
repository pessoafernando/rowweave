package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** seal transfers immutable member ownership away from Worker rollback. */
class PreparedOutputSealTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-seal", "attempt-1", 1L);

    @Test
    void sealedMemberSurvivesRollbackCloseAndExactReplay(@TempDir Path tmp) {
        Path domain = tmp.resolve("domain");
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        ManifestFileSink output = preparedOutput(domain, "publication-1", 7L);
        assertTrue(registry.register("group", 101L, output, FENCE));
        byte[] encoded = registry.installManifestMember(
                "group", 101L, "publication-1", 7L, FENCE);
        CommitManifest.Member member = ManifestCommitWire.decodeMember(encoded);
        Path installed = domain.resolve(member.relativeDataPath());
        assertTrue(Files.exists(installed));
        assertEquals(1, registry.activeCount());

        registry.sealManifestMember(
                "group", 101L, "publication-1", 7L, FENCE);
        registry.sealManifestMember(
                "group", 101L, "publication-1", 7L, FENCE);
        assertEquals(0, registry.activeCount(),
                "seal must release the Sink strong reference");
        assertArrayEquals(encoded, registry.installManifestMember(
                "group", 101L, "publication-1", 7L, FENCE),
                "new leader must recover the bounded member descriptor after seal ACK loss");

        registry.rollback("group", 101L, FENCE);
        registry.rollbackAll();
        registry.close();
        assertTrue(Files.exists(installed),
                "sealed immutable data belongs to the journal/GC, never Worker rollback");
    }

    @Test
    void wrongFenceCannotSealOrReleaseThePreparedOwner(@TempDir Path tmp) {
        PreparedOutputRegistry registry = new PreparedOutputRegistry(8, 60_000L);
        ManifestFileSink output = preparedOutput(tmp.resolve("domain"), "publication-1", 7L);
        registry.register("group", 101L, output, FENCE);
        registry.installManifestMember("group", 101L, "publication-1", 7L, FENCE);

        assertThrows(IllegalArgumentException.class, () -> registry.sealManifestMember(
                "group", 101L, "publication-1", 7L,
                new RunAttemptFence("run-seal", "attempt-stale", 2L)));
        assertEquals(1, registry.activeCount());
        registry.close();
    }

    private static ManifestFileSink preparedOutput(
            Path domain, String publicationId, long generation) {
        ManifestFileSink output = new ManifestFileSink(META, domain.toString(), "ledger");
        output.bindManifestAttempt(publicationId, generation);
        DataRow row = new DataRow(1);
        row.set(0, 9L);
        output.collect(row);
        output.prepareCommit();
        return output;
    }
}
