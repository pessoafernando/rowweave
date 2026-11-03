package io.rowweave.cluster;

import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.SignedIncrementalSourceProviderFixture;
import io.rowweave.incremental.IncrementalCaptureRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** provider capture/materialize execute only inside Worker. */
class WorkerIncrementalSourceActionTest {

    @Test
    void admittedWorkerExecutesCaptureAndMaterializeOnSharedSupervisor(
            @TempDir Path temp) throws Exception {
        try (SignedIncrementalSourceProviderFixture.Session fixture =
                     SignedIncrementalSourceProviderFixture.open(temp);
             DeploymentRuntime runtime = fixture.workerRuntime();
             MasterServer master = new MasterServer(60_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "source-worker", 16,
                     new FixedMasterLocator(new Endpoint(
                             "127.0.0.1", master.port())),
                     temp.resolve("worker").toFile(), runtime)) {
            IncrementalSourceProviderContract contract =
                    SignedIncrementalSourceProviderFixture.contract();
            IncrementalSourceConfig config = new IncrementalSourceConfig(
                    contract.providerKey(),
                    IncrementalSourceProviderContractCodec.digest(contract),
                    "config".getBytes(StandardCharsets.UTF_8));
            IncrementalCaptureRequest capture = IncrementalCaptureRequest.normal(
                    1L, digest("definition"), 0L, IncrementalFrontier.begin(),
                    "trigger", IncrementalRunMode.SNAPSHOT_V1);
            RpcMessage capturedResponse = worker.handler().apply(
                    action(worker, master,
                            IncrementalSourceActionCommand.capture(
                                    contract, config, capture)));
            assertEquals(RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT,
                    capturedResponse.type());
            IncrementalSourceActionResult captured =
                    IncrementalSourceActionResultCodec.decode(
                            capturedResponse.incrementalSourceActionResultBytes(),
                            ignored -> { });
            assertTrue(new String(captured.capturedTarget().snapshotIdentity(),
                    StandardCharsets.UTF_8).startsWith("rowweave-provider-action-"));

            RpcMessage materializedResponse = worker.handler().apply(
                    action(worker, master,
                            IncrementalSourceActionCommand.materialize(
                                    contract, config, captured.capturedTarget())));
            IncrementalSourceActionResult materialized =
                    IncrementalSourceActionResultCodec.decode(
                            materializedResponse.incrementalSourceActionResultBytes(),
                            ignored -> { });
            assertEquals(1, materialized.materialization().splits().size());
            assertTrue(new String(materialized.materialization().splits().get(0)
                            .openRequest(), StandardCharsets.UTF_8)
                    .startsWith("rowweave-provider-action-"));

            IncrementalSourceActionResult routed =
                    master.executeIncrementalSourceActionControl(
                            IncrementalSourceActionCommand.capture(
                                    contract, config, capture),
                            System.nanoTime() + 5_000_000_000L);
            assertEquals(IncrementalSourceActionResult.Kind.CAPTURED,
                    routed.kind());
            assertEquals(IncrementalFrontier.int64Sequence(10L),
                    routed.capturedTarget().target());

            RpcMessage stale = RpcMessage.executeIncrementalSourceAction(
                    "source-worker",
                    IncrementalSourceActionCommandCodec.encode(
                            IncrementalSourceActionCommand.capture(
                                    contract, config, capture)),
                    new Endpoint("127.0.0.1", master.port()), 1L,
                    "stale-incarnation");
            RpcMessage rejected = worker.handler().apply(stale);
            assertEquals(RpcMessage.Type.JOB_RESULT, rejected.type());
            assertFalse(rejected.success());
            assertTrue(rejected.errorMessage().contains("incarnation"));
        }
    }

    private static RpcMessage action(
            WorkerServer worker, MasterServer master,
            IncrementalSourceActionCommand command) {
        return RpcMessage.executeIncrementalSourceAction(
                "source-worker",
                IncrementalSourceActionCommandCodec.encode(command),
                new Endpoint("127.0.0.1", master.port()), 1L,
                worker.incarnationId());
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }
}
