package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerManifestControlTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("manifest-run", "manifest-attempt", 1L);

    @Test
    void installAndPublishUseDedicatedResponsesAndFenceReplacementWorker(@TempDir Path tmp)
            throws Exception {
        Path domain = tmp.resolve("domain");
        String attempt = "attempt-worker";
        long generation = 23L;
        MasterServer master = new MasterServer(5_000L);
        WorkerServer worker = WorkerServer.newUnguarded(
                "manifest-worker", 1, "127.0.0.1", master.port(), tmp.resolve("spill").toFile());
        ManifestFileSink output = preparedOutput(domain, attempt, generation);
        preparedOutputs(worker).register("manifest-group", 501L, output, FENCE);
        try {
            RpcMessage stale = worker.handler().apply(RpcMessage.installManifestMember(
                    "manifest-worker", 501L, "manifest-group", attempt, generation,
                    "stale-inc", FENCE));
            assertEquals(RpcMessage.Type.ACK, stale.type());
            assertFalse(java.nio.file.Files.exists(domain.resolve("data").resolve("23")));

            RunAttemptFence staleFence =
                    new RunAttemptFence("manifest-run", "stale-attempt", 2L);
            RpcMessage fenced = worker.handler().apply(RpcMessage.installManifestMember(
                    "manifest-worker", 501L, "manifest-group", attempt, generation,
                    worker.incarnationId(), staleFence));
            assertEquals(RpcMessage.Type.JOB_RESULT, fenced.type());
            assertFalse(fenced.success());
            assertFalse(java.nio.file.Files.exists(domain.resolve("data").resolve("23")));

            RpcMessage installed = worker.handler().apply(RpcMessage.installManifestMember(
                    "manifest-worker", 501L, "manifest-group", attempt, generation,
                    worker.incarnationId(), FENCE));
            assertEquals(RpcMessage.Type.MANIFEST_MEMBER_INSTALLED, installed.type());
            CommitManifest.Member member = ManifestCommitWire.decodeMember(installed.commitPayload());
            CommitManifest manifest = new CommitManifest(
                    output.domainId(), generation, attempt, Collections.singletonList(member));

            RpcMessage stalePublish = worker.handler().apply(RpcMessage.publishManifestGroup(
                    "manifest-worker", 501L, "manifest-group", attempt, generation,
                    manifest.encode(), worker.incarnationId(), staleFence));
            assertEquals(RpcMessage.Type.JOB_RESULT, stalePublish.type());
            assertFalse(stalePublish.success());
            assertNull(new ManifestFileStore(domain).latest());

            RpcMessage published = worker.handler().apply(RpcMessage.publishManifestGroup(
                    "manifest-worker", 501L, "manifest-group", attempt, generation,
                    manifest.encode(), worker.incarnationId(), FENCE));
            assertEquals(RpcMessage.Type.ACK, published.type());
            assertEquals(generation, new ManifestFileStore(domain).latest().generation());
        } finally {
            worker.close();
            master.close();
        }
    }

    @Test
    void sealControlReleasesWorkerOwnershipAndLateRollbackCannotDeleteMember(@TempDir Path tmp)
            throws Exception {
        Path domain = tmp.resolve("sealed-domain");
        String publication = "publication-sealed";
        long generation = 24L;
        MasterServer master = new MasterServer(5_000L);
        WorkerServer worker = WorkerServer.newUnguarded(
                "manifest-worker", 1, "127.0.0.1", master.port(),
                tmp.resolve("sealed-spill").toFile());
        ManifestFileSink output = preparedOutput(domain, publication, generation);
        preparedOutputs(worker).register("manifest-group", 502L, output, FENCE);
        try {
            RpcMessage installed = worker.handler().apply(RpcMessage.installManifestMember(
                    "manifest-worker", 502L, "manifest-group", publication, generation,
                    worker.incarnationId(), FENCE));
            CommitManifest.Member member = ManifestCommitWire.decodeMember(
                    installed.commitPayload());
            Path data = domain.resolve(member.relativeDataPath());
            assertTrue(java.nio.file.Files.exists(data));

            RpcMessage sealed = worker.handler().apply(RpcMessage.sealManifestMember(
                    "manifest-worker", 502L, "manifest-group", publication, generation,
                    worker.incarnationId(), FENCE));
            assertEquals(RpcMessage.Type.ACK, sealed.type());
            assertEquals(0, worker.preparedOutputActiveCount());

            worker.handler().apply(RpcMessage.rollbackPreparedOutput(
                    "manifest-worker", 502L, "manifest-group",
                    worker.incarnationId(), FENCE));
            assertTrue(java.nio.file.Files.exists(data));
        } finally {
            worker.close();
            master.close();
        }
    }

    private static ManifestFileSink preparedOutput(Path domain, String attempt, long generation) {
        ManifestFileSink output = new ManifestFileSink(META, domain.toString(), "ledger");
        output.bindManifestAttempt(attempt, generation);
        DataRow row = new DataRow(1);
        row.set(0, 9L);
        output.collect(row);
        output.prepareCommit();
        return output;
    }

    private static PreparedOutputRegistry preparedOutputs(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("preparedOutputs");
        field.setAccessible(true);
        return (PreparedOutputRegistry) field.get(worker);
    }
}

