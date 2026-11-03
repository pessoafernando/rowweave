package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MasterLeadershipFencingTest {

    @Test
    void standbyRejectsStateChangingCallsWithoutMutatingWorkerState() {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer first = new MasterServer(5_000, 0, registry);
        MasterServer second = new MasterServer(5_000, 0, registry);
        MasterElection firstElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", first.port()));
        MasterElection secondElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", second.port()));
        first.bindLeadership(firstElection);
        second.bindLeadership(secondElection);
        firstElection.start();
        secondElection.start();
        MasterServer leader = firstElection.isLeader() ? first : second;
        MasterServer standby = firstElection.isLeader() ? second : first;
        try {
            RpcMessage register = standby.handler().apply(
                    RpcMessage.registerWorker("standby-only", "127.0.0.1", 23456, 4));
            assertRejectedByStandby(register);
            assertFalse(standby.workers().containsKey("standby-only"));

            RpcMessage submit = standby.handler().apply(RpcMessage.submitJob(new byte[]{1, 2, 3}));
            assertRejectedByStandby(submit);

            assertNull(standby.handler().apply(RpcMessage.heartbeat("standby-only", 4)),
                    "a one-way heartbeat must be dropped rather than writing an orphan failure frame");
            assertNull(standby.handler().apply(
                    RpcMessage.progressReport("standby-only", 1L, 1L, 0L, true)),
                    "a one-way progress report must be dropped rather than writing an orphan failure frame");

            RpcMessage leaderRegister = leader.handler().apply(
                    RpcMessage.registerWorker("leader-worker", "127.0.0.1", 23457, 4));
            assertEquals(RpcMessage.Type.ACK, leaderRegister.type());
        } finally {
            firstElection.close();
            secondElection.close();
            first.close();
            second.close();
        }
    }

    @Test
    void oldMasterSocketRejectsAfterLeadershipClose() {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer first = new MasterServer(5_000, 0, registry);
        MasterServer second = new MasterServer(5_000, 0, registry);
        MasterElection firstElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", first.port()));
        MasterElection secondElection = new MasterElection(
                registry, new Endpoint("127.0.0.1", second.port()));
        first.bindLeadership(firstElection);
        second.bindLeadership(secondElection);
        firstElection.start();
        secondElection.start();
        MasterServer oldMaster = firstElection.isLeader() ? first : second;
        MasterElection oldElection = firstElection.isLeader() ? firstElection : secondElection;
        try {
            oldElection.close();
            RpcMessage response = oldMaster.handler().apply(RpcMessage.submitJob(new byte[]{9}));
            assertRejectedByStandby(response);
        } finally {
            firstElection.close();
            secondElection.close();
            first.close();
            second.close();
        }
    }

    @Test
    void registryWorkerRejectsMissingAndStaleLeaderClaims(@TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(5_000, 0, registry);
        Endpoint current = new Endpoint("127.0.0.1", master.port());
        MasterElection election = new MasterElection(registry, current);
        master.bindLeadership(election);
        election.start();
        WorkerServer worker = WorkerServer.newUnguarded(
                "w-fenced", 2, new RegistryMasterLocator(registry), tmp.toFile());
        try {
            RpcMessage missing = worker.handler().apply(
                    RpcMessage.assignStage("w-fenced", 101L, new byte[]{1}, worker.incarnationId()));
            assertAssignmentRejected(missing, "missing");
            assertEquals(0, worker.runningStageCount());

            RpcMessage stale = worker.handler().apply(
                    RpcMessage.assignStage("w-fenced", 102L, new byte[]{2},
                            new Endpoint("127.0.0.1", current.port() + 1), election.termToken(),
                            worker.incarnationId()));
            assertAssignmentRejected(stale, "stale");
            assertEquals(0, worker.runningStageCount());

            RpcMessage oldTerm = worker.handler().apply(
                    RpcMessage.assignStage("w-fenced", 103L, new byte[]{3},
                            current, election.termToken() ^ 1L, worker.incarnationId()));
            assertAssignmentRejected(oldTerm, "term");
            assertEquals(0, worker.runningStageCount());

            io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                    new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
            byte[] validCommand = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                    meta, 64, new io.rowweave.CollectingSink(meta))));
            RpcMessage accepted = worker.handler().apply(
                    RunAttemptFenceTestSupport.socket("w-fenced", 104L, validCommand,
                            current, election.termToken(), worker.incarnationId()));
            assertEquals(RpcMessage.Type.ACK, accepted.type(),
                    "a claim for the current endpoint plus term should pass the fence");

            long endedTerm = election.termToken();
            election.close();
            long cancellationDeadline = System.currentTimeMillis() + 3_000L;
            while (worker.runningStageCount() != 0
                    && System.currentTimeMillis() < cancellationDeadline) {
                Thread.sleep(10L);
            }
            assertEquals(0, worker.runningStageCount(), "a leader disappearance event should asynchronously clear accepted stages of the old term");
            RpcMessage noLeader = worker.handler().apply(
                    RpcMessage.assignStage(
                            "w-fenced", 105L, new byte[]{5}, current, endedTerm, worker.incarnationId()));
            assertAssignmentRejected(noLeader, "no active");
        } finally {
            worker.close();
            election.close();
            master.close();
        }
    }

    @Test
    void registryWorkerRejectsStaleAndUnavailableReconciliationClaims(@TempDir Path tmp)
            throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(5_000, 0, registry);
        Endpoint current = new Endpoint("127.0.0.1", master.port());
        MasterElection election = new MasterElection(registry, current);
        master.bindLeadership(election);
        election.start();
        WorkerServer worker = WorkerServer.newUnguarded(
                "w-reconcile-fenced", 1, new RegistryMasterLocator(registry), tmp.toFile());
        try {
            io.rowweave.run.RunAttemptFence fence = new io.rowweave.run.RunAttemptFence(
                    "run-reconcile-fenced", "attempt-1", 3L);
            io.rowweave.run.StageAssignmentRecord assignment =
                    io.rowweave.run.StageAssignmentRecord.create(
                            fence, 1L, 0, 501L, 0, 0,
                            io.rowweave.run.StageAssignmentRole.SINK,
                            "w-reconcile-fenced", "127.0.0.1", worker.port(), -1,
                            worker.incarnationId());
            io.rowweave.run.StageAssignmentToken token =
                    io.rowweave.run.StageAssignmentSnapshot.initial(
                            new io.rowweave.run.StageAssignmentManifest(
                                    fence, "artifact-reconcile-fenced", 1L,
                                    java.util.Collections.singletonList(assignment))).token(0);
            byte[] encoded = io.rowweave.run.StageAssignmentManifestCodec.encodeToken(token);
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(new byte[] {1});

            RpcMessage stale = worker.handler().apply(RpcMessage.reconcileDurableStage(
                    "w-reconcile-fenced", encoded, digest, current,
                    election.termToken() ^ 1L, worker.incarnationId(), fence));
            assertAssignmentRejected(stale, "term");
            assertEquals(0, worker.runningStageCount());

            long endedTerm = election.termToken();
            election.close();
            RpcMessage unavailable = worker.handler().apply(RpcMessage.reconcileDurableStage(
                    "w-reconcile-fenced", encoded, digest, current,
                    endedTerm, worker.incarnationId(), fence));
            assertAssignmentRejected(unavailable, "no active");
            assertEquals(0, worker.runningStageCount());
        } finally {
            worker.close();
            election.close();
            master.close();
        }
    }

    @Test
    void registryWorkerFencesPreparedOutputControlsByLeaderTerm(@TempDir Path tmp) {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(5_000, 0, registry);
        Endpoint current = new Endpoint("127.0.0.1", master.port());
        MasterElection election = new MasterElection(registry, current);
        master.bindLeadership(election);
        election.start();
        WorkerServer worker = WorkerServer.newUnguarded(
                "w-output-fenced", 1, new RegistryMasterLocator(registry), tmp.toFile());
        try {
            RpcMessage missing = worker.handler().apply(RpcMessage.rollbackPreparedOutput(
                    "w-output-fenced", 301L, "group", worker.incarnationId(),
                    RunAttemptFenceTestSupport.FENCE));
            assertAssignmentRejected(missing, "missing");
            assertEquals(0, worker.preparedOutputEntryCount(),
                    "a missing leader claim must not create a rollback tombstone");

            RpcMessage stale = worker.handler().apply(RpcMessage.rollbackPreparedOutput(
                    "w-output-fenced", 301L, "group", current,
                    election.termToken() ^ 1L, worker.incarnationId(),
                    RunAttemptFenceTestSupport.FENCE));
            assertAssignmentRejected(stale, "term");
            assertEquals(0, worker.preparedOutputEntryCount(),
                    "Sink control from an old term must not touch the prepared registry");

            RpcMessage accepted = worker.handler().apply(RpcMessage.rollbackPreparedOutput(
                    "w-output-fenced", 301L, "group", current,
                    election.termToken(), worker.incarnationId(),
                    RunAttemptFenceTestSupport.FENCE));
            assertEquals(RpcMessage.Type.ACK, accepted.type());
            assertEquals(1, worker.preparedOutputEntryCount(),
                    "a rollback by the current leader should create a tombstone that blocks a late register");
        } finally {
            worker.close();
            election.close();
            master.close();
        }
    }

    @Test
    void leaderTermLossRollsBackPreparedOutputs(@TempDir Path tmp) throws Exception {
        InProcessRegistry registry = new InProcessRegistry();
        MasterServer master = new MasterServer(5_000, 0, registry);
        Endpoint current = new Endpoint("127.0.0.1", master.port());
        MasterElection election = new MasterElection(registry, current);
        master.bindLeadership(election);
        election.start();
        WorkerServer worker = WorkerServer.newUnguarded(
                "w-term-output", 1, new RegistryMasterLocator(registry), tmp.toFile());
        TrackingOutput output = new TrackingOutput();
        preparedOutputs(worker).register(
                "term-group", 401L, output, RunAttemptFenceTestSupport.FENCE);
        try {
            election.close();
            long deadline = System.currentTimeMillis() + 3_000L;
            while (output.rollbackCalls == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(1, output.rollbackCalls,
                    "losing the leader term must roll back a prepared Sink that no longer has a valid coordinator");
            assertEquals(0, worker.preparedOutputActiveCount());
        } finally {
            worker.close();
            election.close();
            master.close();
        }
    }

    @Test
    void fixedMasterWorkerAcceptsAssignmentWithoutLeaderClaim(@TempDir Path tmp) {
        MasterServer master = new MasterServer(5_000);
        WorkerServer worker = WorkerServer.newUnguarded(
                "w-fixed", 1, "127.0.0.1", master.port(), tmp.toFile());
        try {
            io.rowweave.DataSchema meta = new io.rowweave.DataSchema(
                    new io.rowweave.Field("k", io.rowweave.DataType.IntegerType));
            byte[] command = master.serialize(RunAttemptFenceTestSupport.bind(StageCommand.consumer(
                    meta, 64, new io.rowweave.CollectingSink(meta))));
            RpcMessage response = worker.handler().apply(
                    RunAttemptFenceTestSupport.socket(
                            "w-fixed", 201L, command, worker.incarnationId()));
            assertEquals(RpcMessage.Type.ACK, response.type(),
                    "the fixed single-master path keeps accepting an ASSIGN with no leader claim but a correct Worker incarnation");
        } finally {
            worker.close();
            master.close();
        }
    }

    private static void assertRejectedByStandby(RpcMessage response) {
        assertNotNull(response);
        assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
        assertFalse(response.success());
        assertTrue(response.errorMessage().contains("not active master leader"), response.errorMessage());
    }

    private static void assertAssignmentRejected(RpcMessage response, String expected) {
        assertNotNull(response);
        assertEquals(RpcMessage.Type.JOB_RESULT, response.type());
        assertFalse(response.success());
        assertTrue(response.errorMessage().toLowerCase().contains(expected), response.errorMessage());
    }

    private static PreparedOutputRegistry preparedOutputs(WorkerServer worker) throws Exception {
        java.lang.reflect.Field field = WorkerServer.class.getDeclaredField("preparedOutputs");
        field.setAccessible(true);
        return (PreparedOutputRegistry) field.get(worker);
    }

    private static final class TrackingOutput implements io.rowweave.Sink {
        private static final long serialVersionUID = 1L;
        volatile int rollbackCalls;

        @Override public io.rowweave.DataSchema getSchema() {
            return new io.rowweave.DataSchema(
                    new io.rowweave.Field("v", io.rowweave.DataType.IntegerType));
        }
        @Override public void collect(io.rowweave.DataRow row) { }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void commitPrepared() { }
        @Override public void rollbackCommit() { rollbackCalls++; }
        @Override public void finalizeCommit() { }
    }
}

