package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.commit.CommitManifest;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** late assignment/manifest calls must not cross final purge. */
class ZkRunIncarnationAssignmentFencingTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void publishResultConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean connectionLoss : new boolean[] { true, false }) {
                Fixture fixture = fixture(writer,
                        "publish-result-transport-" + connectionLoss,
                        LateMutation.PUBLISH, false);
                AtomicInteger faults = new AtomicInteger();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .RESULT_BEFORE_AUTHORITY_CONFIRMATION) {
                        faults.incrementAndGet();
                        if (connectionLoss) {
                            throw new KeeperException.ConnectionLossException();
                        }
                        throw new KeeperException.OperationTimeoutException();
                    }
                });

                assertThrows(RunStateStoreUnavailableException.class,
                        () -> reader.publish(fixture.manifest));
                assertEquals(1, faults.get(),
                        "publish result confirmation must fail once");

                reader.evidenceReadObserverForTest(phase -> { });
                assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                        reader.publish(fixture.manifest).code(),
                        "fresh publish must reconcile the committed manifest");
            }
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void publishResultConfirmationFencesOrdinaryAndRecoveredReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean recovered : new boolean[] { false, true }) {
                String runId = "publish-result-replacement-"
                        + (recovered ? "recovered" : "ordinary");
                Fixture original = fixture(
                        writer, runId, LateMutation.PUBLISH, recovered);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<Fixture> replacement = new AtomicReference<>();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.evidenceReadObserverForTest(phase -> {
                    if (phase == ZkRunStateStore.EvidenceReadPhase
                            .RESULT_BEFORE_AUTHORITY_CONFIRMATION
                            && replaced.compareAndSet(false, true)) {
                        retireAndPurge(writer, original);
                        replacement.set(fixture(
                                writer, runId, LateMutation.PUBLISH, recovered));
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, runId));
                    }
                });

                Object result;
                try {
                    result = invoke(
                            reader, original, LateMutation.PUBLISH, recovered);
                } finally {
                    reader.evidenceReadObserverForTest(phase -> { });
                }

                assertTrue(replaced.get(),
                        "publish result confirmation phase must be reached");
                assertStale(result, LateMutation.PUBLISH);
                assertReplacementUntouched(
                        writer, replacement.get(), LateMutation.PUBLISH);
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, runId),
                        "old publish result must not mutate replacement B");
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void publishHelperAckLossContinuesOnlyForOriginalIncarnation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (ZkRunStateStore.GuardedMutationKind helper
                        : publishHelperKinds()) {
                    String runId = "publish-helper-original-"
                            + helper.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, LateMutation.PUBLISH, recovered);
                    AtomicBoolean removedIntent = new AtomicBoolean();
                    AtomicBoolean ackLost = new AtomicBoolean();
                    reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                        removeIntentForRepair(
                                second, runId, helper, kind, moment,
                                removedIntent);
                        if (kind == helper
                                && moment == ZkRunStateStore.GuardedMutationMoment
                                .AFTER_COMMIT_BEFORE_ACK
                                && ackLost.compareAndSet(false, true)) {
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
                    try {
                        StageManifestPublication result =
                                (StageManifestPublication) invoke(
                                        reader, fixture,
                                        LateMutation.PUBLISH, recovered);
                        if ((result.code()
                                != StageManifestPublication.Code.PUBLISHED
                                && result.code()
                                != StageManifestPublication.Code.ALREADY_PUBLISHED)
                                || result.snapshot() == null) {
                            violations.add(helper + "/" + recovered
                                    + " returned " + result.code());
                        }
                    } catch (RuntimeException failure) {
                        violations.add(helper + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(ackLost.get(), helper
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                }
            }
            assertTrue(violations.isEmpty(),
                    "original helper ACK loss must resume publication: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void publishHelperAckLossCannotContinueOnEquivalentReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (ZkRunStateStore.GuardedMutationKind helper
                        : publishHelperKinds()) {
                    String runId = "publish-helper-replacement-"
                            + helper.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, LateMutation.PUBLISH, recovered);
                    AtomicBoolean removedIntent = new AtomicBoolean();
                    AtomicBoolean replaced = new AtomicBoolean();
                    reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                        removeIntentForRepair(
                                second, runId, helper, kind, moment,
                                removedIntent);
                        if (kind == helper
                                && moment == ZkRunStateStore.GuardedMutationMoment
                                .AFTER_COMMIT_BEFORE_ACK
                                && replaced.compareAndSet(false, true)) {
                            retireAndPurge(writer, original);
                            fixture(writer, runId,
                                    LateMutation.PUBLISH, recovered);
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
                    try {
                        StageManifestPublication result =
                                (StageManifestPublication) invoke(
                                        reader, original,
                                        LateMutation.PUBLISH, recovered);
                        if (result.code()
                                != StageManifestPublication.Code.STALE_RUN
                                || result.snapshot() != null) {
                            violations.add(helper + "/" + recovered
                                    + " returned " + result.code());
                        }
                    } catch (RuntimeException failure) {
                        violations.add(helper + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(replaced.get(), helper
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                }
            }
            assertTrue(violations.isEmpty(),
                    "old helper ACK loss must not continue on replacement B: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void publishHelperAckLossFencesEveryReplacementState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (ZkRunStateStore.GuardedMutationKind helper
                        : publishHelperKinds()) {
                    for (AckReplacementState state
                            : AckReplacementState.values()) {
                        String runId = "publish-helper-state-"
                                + helper.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + "-" + state.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + (recovered
                                        ? "-recovered" : "-ordinary");
                        Fixture original = fixture(
                                writer, runId,
                                LateMutation.PUBLISH, recovered);
                        AtomicBoolean removedIntent = new AtomicBoolean();
                        AtomicBoolean ackLost = new AtomicBoolean();
                        CountDownLatch layoutReady = new CountDownLatch(1);
                        CountDownLatch allowCreate = new CountDownLatch(1);
                        AtomicReference<Fixture> replacement =
                                new AtomicReference<>();
                        AtomicReference<List<String>> replacementFingerprint =
                                new AtomicReference<>();
                        AtomicReference<Thread> creator =
                                new AtomicReference<>();
                        AtomicReference<Throwable> createFailure =
                                new AtomicReference<>();
                        reader.guardedMutationObserverForTest(
                                (kind, ordinal, moment) -> {
                            removeIntentForRepair(
                                    second, runId, helper, kind, moment,
                                    removedIntent);
                            if (kind == helper
                                    && moment == ZkRunStateStore
                                    .GuardedMutationMoment
                                    .AFTER_COMMIT_BEFORE_ACK
                                    && ackLost.compareAndSet(false, true)) {
                                retireAndPurge(writer, original);
                                if (state == AckReplacementState
                                        .PREPUBLICATION) {
                                    AtomicBoolean paused = new AtomicBoolean();
                                    writer.evidenceReadObserverForTest(phase -> {
                                        if (phase == ZkRunStateStore
                                                .EvidenceReadPhase
                                                .APPLY_AFTER_INITIAL_SNAPSHOT
                                                && paused.compareAndSet(
                                                        false, true)) {
                                            layoutReady.countDown();
                                            if (!allowCreate.await(
                                                    10, TimeUnit.SECONDS)) {
                                                throw new AssertionError(
                                                        "replacement CREATE "
                                                                + "release "
                                                                + "timed out");
                                            }
                                        }
                                    });
                                    Thread thread = new Thread(() -> {
                                        try {
                                            replacement.set(fixture(
                                                    writer, runId,
                                                    LateMutation.PUBLISH,
                                                    recovered));
                                        } catch (Throwable failure) {
                                            createFailure.set(failure);
                                        }
                                    }, "zk-publish-helper-prepublication-"
                                            + helper + "-" + recovered);
                                    creator.set(thread);
                                    thread.start();
                                    if (!layoutReady.await(
                                            10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "replacement layout not "
                                                        + "published");
                                    }
                                    replacementFingerprint.set(
                                            runSubtreeFingerprint(
                                                    second, runId));
                                } else if (state
                                        != AckReplacementState.PURGED) {
                                    Fixture next = fixture(
                                            writer, runId,
                                            LateMutation.PUBLISH, recovered);
                                    replacement.set(next);
                                    if (state
                                            == AckReplacementState.TERMINAL) {
                                        retire(writer, next);
                                    }
                                    replacementFingerprint.set(
                                            runSubtreeFingerprint(
                                                    second, runId));
                                }
                                throw new KeeperException
                                        .ConnectionLossException();
                            }
                        });

                        StageManifestPublication result = null;
                        RuntimeException failure = null;
                        try {
                            result = (StageManifestPublication) invoke(
                                    reader, original,
                                    LateMutation.PUBLISH, recovered);
                        } catch (RuntimeException thrown) {
                            failure = thrown;
                        } finally {
                            if (state == AckReplacementState.PREPUBLICATION
                                    && replacementFingerprint.get() != null) {
                                try {
                                    if (!replacementFingerprint.get().equals(
                                            runSubtreeFingerprint(
                                                    second, runId))) {
                                        violations.add(helper + "/" + state
                                                + "/" + recovered
                                                + " mutated paused "
                                                + "prepublication subtree");
                                    }
                                } catch (Exception inspectionFailure) {
                                    violations.add(helper + "/" + state
                                            + "/" + recovered
                                            + " prepublication inspection "
                                            + "threw "
                                            + inspectionFailure.getClass()
                                                    .getSimpleName());
                                }
                            }
                            allowCreate.countDown();
                            Thread thread = creator.get();
                            if (thread != null) {
                                thread.join(TimeUnit.SECONDS.toMillis(10));
                                assertTrue(!thread.isAlive(),
                                        "replacement CREATE must terminate");
                            }
                        }

                        String label = helper + "/" + state + "/" + recovered;
                        assertTrue(ackLost.get(), label
                                + " must reach AFTER_COMMIT_BEFORE_ACK");
                        if (failure != null) {
                            violations.add(label + " threw "
                                    + failure.getClass().getSimpleName());
                        } else if (result.code()
                                != StageManifestPublication.Code.STALE_RUN
                                || result.snapshot() != null) {
                            violations.add(label + " returned "
                                    + result.code());
                        }
                        if (createFailure.get() != null) {
                            violations.add(label + " replacement CREATE threw "
                                    + createFailure.get().getClass()
                                            .getSimpleName());
                        }
                        if (helper == ZkRunStateStore.GuardedMutationKind
                                .PUBLISH_INTENT_REPAIR
                                && !removedIntent.get()) {
                            violations.add(label
                                    + " did not expose intent repair");
                        }
                        Fixture next = replacement.get();
                        if (state == AckReplacementState.PURGED) {
                            if (writer.get(runId) != null) {
                                violations.add(label
                                        + " unexpectedly recreated Run");
                            }
                        } else if (next == null) {
                            violations.add(label + " replacement missing");
                        } else {
                            RunSnapshot snapshot = writer.get(runId);
                            long expectedRevision = state
                                    == AckReplacementState.TERMINAL ? 3L : 2L;
                            if (snapshot == null
                                    || snapshot.revision() != expectedRevision) {
                                violations.add(label + " replacement revision="
                                        + (snapshot == null
                                                ? "missing"
                                                : snapshot.revision()));
                            }
                            if (writer.assignmentSnapshot(next.fence) != null) {
                                violations.add(label
                                        + " published replacement assignment");
                            }
                            if (state != AckReplacementState.PREPUBLICATION) {
                                try {
                                    if (!replacementFingerprint.get().equals(
                                            runSubtreeFingerprint(
                                                    second, runId))) {
                                        violations.add(label
                                                + " mutated replacement "
                                                + "subtree");
                                    }
                                } catch (Exception inspectionFailure) {
                                    violations.add(label
                                            + " subtree inspection threw "
                                            + inspectionFailure.getClass()
                                                    .getSimpleName());
                                }
                            }
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "publish helper ACK reconciliation must fence every "
                            + "replacement state: " + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void abortGuardAckLossContinuesOnlyForOriginalIncarnation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.MEMBER, LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    String runId = "helper-ack-original-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, mutation, recovered);
                    second.delete().forPath(abortGuardPath(runId));
                    AtomicBoolean beforeCommit = new AtomicBoolean();
                    AtomicBoolean ackLost = new AtomicBoolean();
                    ZkRunStateStore.GuardedMutationKind helper =
                            abortGuardKind(mutation);
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == helper) {
                            if (moment == ZkRunStateStore.GuardedMutationMoment
                                    .BEFORE_COMMIT) {
                                beforeCommit.set(true);
                            } else if (ackLost.compareAndSet(false, true)) {
                                throw new KeeperException
                                        .ConnectionLossException();
                            }
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, fixture, mutation, recovered);
                        if (!isCommitted(result, mutation)) {
                            violations.add(mutation + "/" + recovered
                                    + " returned "
                                    + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(beforeCommit.get(), helper + "/" + recovered
                            + " must reach BEFORE_COMMIT");
                    assertTrue(ackLost.get(), helper + "/" + recovered
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                }
            }
            assertTrue(violations.isEmpty(),
                    "an original helper commit must be confirmed before the "
                            + "outer mutation continues: " + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void abortGuardAckLossCannotConfirmAgainstReplacementIncarnation()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.MEMBER, LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    String runId = "helper-ack-replacement-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, mutation, recovered);
                    second.delete().forPath(abortGuardPath(runId));
                    AtomicBoolean beforeCommit = new AtomicBoolean();
                    AtomicBoolean replaced = new AtomicBoolean();
                    ZkRunStateStore.GuardedMutationKind helper =
                            abortGuardKind(mutation);
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == helper
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.BEFORE_COMMIT) {
                            beforeCommit.set(true);
                        } else if (kind == helper
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && replaced.compareAndSet(false, true)) {
                            retireAndPurge(writer, original);
                            fixture(writer, runId, mutation, recovered);
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, original, mutation, recovered);
                        if (!isStale(result, mutation)) {
                            violations.add(mutation + "/" + recovered
                                    + " returned "
                                    + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(beforeCommit.get(), helper + "/" + recovered
                            + " must reach BEFORE_COMMIT");
                    assertTrue(replaced.get(), helper + "/" + recovered
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                }
            }
            assertTrue(violations.isEmpty(),
                    "a replacement guard must not acknowledge an old helper: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void recoveredIntentHelperAndFinalFenceReplacementForSixteenRounds()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (int round = 0; round < 16; round++) {
                String runId = "intent-helper-aba-" + round;
                Fixture original = fixture(
                        writer, runId, LateMutation.INTENT, true);
                second.delete().forPath(abortGuardPath(runId));
                AtomicBoolean helperCommitted = new AtomicBoolean();
                AtomicReference<Fixture> replacement = new AtomicReference<>();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == ZkRunStateStore.GuardedMutationKind
                            .MANIFEST_INTENT_ABORT_GUARD
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK
                            && helperCommitted.compareAndSet(false, true)) {
                        retireAndPurge(writer, original);
                        Fixture next = fixture(
                                writer, runId, LateMutation.INTENT, true);
                        replacement.set(next);
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, runId));
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                Object result = null;
                RuntimeException failure = null;
                try {
                    result = invoke(
                            reader, original, LateMutation.INTENT, true);
                } catch (RuntimeException caught) {
                    failure = caught;
                }

                assertTrue(helperCommitted.get(),
                        "round " + round + " must commit the helper before ACK loss");
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, runId),
                        "round " + round
                                + " old helper/final path must not mutate B");
                assertNull(failure,
                        "replacement must classify the old helper/final call as stale");
                assertStale(result, LateMutation.INTENT);
                assertReplacementUntouched(
                        writer, replacement.get(), LateMutation.INTENT);
            }
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void abortGuardAckLossFencesEveryReplacementState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.MEMBER,
                        LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    for (AckReplacementState state
                            : AckReplacementState.values()) {
                        String runId = "abort-helper-state-"
                                + mutation.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + "-" + state.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + (recovered
                                        ? "-recovered" : "-ordinary");
                        Fixture original = fixture(
                                writer, runId, mutation, recovered);
                        second.delete().forPath(abortGuardPath(runId));
                        ZkRunStateStore.GuardedMutationKind helper =
                                abortGuardKind(mutation);
                        AtomicBoolean ackLost = new AtomicBoolean();
                        CountDownLatch layoutReady = new CountDownLatch(1);
                        CountDownLatch allowCreate = new CountDownLatch(1);
                        AtomicReference<Fixture> replacement =
                                new AtomicReference<>();
                        AtomicReference<Long> expectedRevision =
                                new AtomicReference<>();
                        AtomicReference<List<String>> replacementFingerprint =
                                new AtomicReference<>();
                        AtomicReference<Thread> creator =
                                new AtomicReference<>();
                        AtomicReference<Throwable> createFailure =
                                new AtomicReference<>();
                        reader.guardedMutationObserverForTest(
                                (kind, ordinal, moment) -> {
                            if (kind == helper
                                    && moment == ZkRunStateStore
                                    .GuardedMutationMoment
                                    .AFTER_COMMIT_BEFORE_ACK
                                    && ackLost.compareAndSet(false, true)) {
                                retireAndPurge(writer, original);
                                if (state == AckReplacementState
                                        .PREPUBLICATION) {
                                    AtomicBoolean paused = new AtomicBoolean();
                                    writer.evidenceReadObserverForTest(phase -> {
                                        if (phase == ZkRunStateStore
                                                .EvidenceReadPhase
                                                .APPLY_AFTER_INITIAL_SNAPSHOT
                                                && paused.compareAndSet(
                                                        false, true)) {
                                            layoutReady.countDown();
                                            if (!allowCreate.await(
                                                    10, TimeUnit.SECONDS)) {
                                                throw new AssertionError(
                                                        "replacement CREATE "
                                                                + "release "
                                                                + "timed out");
                                            }
                                        }
                                    });
                                    Thread thread = new Thread(() -> {
                                        try {
                                            Fixture next = fixture(
                                                    writer, runId,
                                                    mutation, recovered);
                                            replacement.set(next);
                                            expectedRevision.set(
                                                    writer.get(runId)
                                                            .revision());
                                        } catch (Throwable failure) {
                                            createFailure.set(failure);
                                        }
                                    }, "zk-abort-helper-prepublication-"
                                            + mutation + "-" + recovered);
                                    creator.set(thread);
                                    thread.start();
                                    if (!layoutReady.await(
                                            10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "replacement layout not "
                                                        + "published");
                                    }
                                    replacementFingerprint.set(
                                            runSubtreeFingerprint(
                                                    second, runId));
                                } else if (state
                                        != AckReplacementState.PURGED) {
                                    Fixture next = fixture(
                                            writer, runId,
                                            mutation, recovered);
                                    replacement.set(next);
                                    if (state
                                            == AckReplacementState.TERMINAL) {
                                        retire(writer, next);
                                    }
                                    expectedRevision.set(
                                            writer.get(runId).revision());
                                    replacementFingerprint.set(
                                            runSubtreeFingerprint(
                                                    second, runId));
                                }
                                throw new KeeperException
                                        .ConnectionLossException();
                            }
                        });

                        Object result = null;
                        RuntimeException failure = null;
                        try {
                            result = invoke(
                                    reader, original, mutation, recovered);
                        } catch (RuntimeException thrown) {
                            failure = thrown;
                        } finally {
                            if (state == AckReplacementState.PREPUBLICATION
                                    && replacementFingerprint.get() != null) {
                                try {
                                    if (!replacementFingerprint.get().equals(
                                            runSubtreeFingerprint(
                                                    second, runId))) {
                                        violations.add(helper + "/" + state
                                                + "/" + recovered
                                                + " mutated paused "
                                                + "prepublication subtree");
                                    }
                                } catch (Exception inspectionFailure) {
                                    violations.add(helper + "/" + state
                                            + "/" + recovered
                                            + " prepublication inspection "
                                            + "threw "
                                            + inspectionFailure.getClass()
                                                    .getSimpleName());
                                }
                            }
                            allowCreate.countDown();
                            Thread thread = creator.get();
                            if (thread != null) {
                                thread.join(TimeUnit.SECONDS.toMillis(10));
                                assertTrue(!thread.isAlive(),
                                        "replacement CREATE must terminate");
                            }
                        }

                        String label = helper + "/" + state + "/" + recovered;
                        assertTrue(ackLost.get(), label
                                + " must reach AFTER_COMMIT_BEFORE_ACK");
                        if (failure != null) {
                            violations.add(label + " threw "
                                    + failure.getClass().getSimpleName());
                        } else if (!isStale(result, mutation)) {
                            violations.add(label + " returned "
                                    + resultCode(result, mutation));
                        }
                        if (createFailure.get() != null) {
                            violations.add(label + " replacement CREATE threw "
                                    + createFailure.get().getClass()
                                            .getSimpleName());
                        }
                        Fixture next = replacement.get();
                        if (state == AckReplacementState.PURGED) {
                            if (writer.get(runId) != null) {
                                violations.add(label
                                        + " unexpectedly recreated Run");
                            }
                        } else if (next == null) {
                            violations.add(label + " replacement missing");
                        } else {
                            RunSnapshot snapshot = writer.get(runId);
                            if (snapshot == null
                                    || !Long.valueOf(snapshot.revision()).equals(
                                            expectedRevision.get())) {
                                violations.add(label + " replacement revision="
                                        + (snapshot == null
                                                ? "missing"
                                                : snapshot.revision()));
                            }
                            if (replacementTargetPresent(
                                    writer, next, mutation)) {
                                violations.add(label
                                        + " mutated replacement target");
                            }
                            if (state != AckReplacementState.PREPUBLICATION) {
                                try {
                                    if (!replacementFingerprint.get().equals(
                                            runSubtreeFingerprint(
                                                    second, runId))) {
                                        violations.add(label
                                                + " mutated replacement "
                                                + "subtree");
                                    }
                                } catch (Exception inspectionFailure) {
                                    violations.add(label
                                            + " subtree inspection threw "
                                            + inspectionFailure.getClass()
                                                    .getSimpleName());
                                }
                            }
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "abort helper ACK reconciliation must fence every "
                            + "replacement state: " + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void helperAckConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();

            for (boolean recovered : new boolean[] { false, true }) {
                for (ZkRunStateStore.GuardedMutationKind helper
                        : publishHelperKinds()) {
                    String label = helper + "/" + recovered;
                    String runId = "helper-confirm-"
                            + helper.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, LateMutation.PUBLISH, recovered);
                    AtomicBoolean removedIntent = new AtomicBoolean();
                    assertHelperConfirmationUnavailable(
                            reader, helper, label,
                            (kind, ordinal, moment) -> removeIntentForRepair(
                                    second, runId, helper, kind, moment,
                                    removedIntent),
                            () -> invoke(
                                    reader, fixture,
                                    LateMutation.PUBLISH, recovered),
                            violations);
                    if (helper == ZkRunStateStore.GuardedMutationKind
                            .PUBLISH_INTENT_REPAIR
                            && !removedIntent.get()) {
                        violations.add(label
                                + " did not expose intent repair");
                    }
                }

                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.MEMBER,
                        LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    ZkRunStateStore.GuardedMutationKind helper =
                            abortGuardKind(mutation);
                    String label = helper + "/" + recovered;
                    String runId = "abort-helper-confirm-"
                            + mutation.name().toLowerCase(
                                    java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, mutation, recovered);
                    second.delete().forPath(abortGuardPath(runId));
                    assertHelperConfirmationUnavailable(
                            reader, helper, label,
                            (kind, ordinal, moment) -> { },
                            () -> invoke(
                                    reader, fixture, mutation, recovered),
                            violations);
                }
            }

            assertTrue(violations.isEmpty(),
                    "helper ACK confirmation transport must not continue "
                            + "the outer mutation: " + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void commitAckLossReplaysOnlyWhileOriginalAuthorityRemains()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore fixtureStore = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.PUBLISH,
                        LateMutation.ACCEPT,
                        LateMutation.TERMINAL,
                        LateMutation.MEMBER,
                        LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    String runId = "ack-original-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            fixtureStore, runId, mutation, recovered);
                    AtomicBoolean beforeCommit = new AtomicBoolean();
                    AtomicBoolean ackLost = new AtomicBoolean();
                    reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                        if (kind == mutation.kind) {
                            if (moment == ZkRunStateStore.GuardedMutationMoment
                                    .BEFORE_COMMIT) {
                                beforeCommit.set(true);
                            } else if (ackLost.compareAndSet(false, true)) {
                                throw new KeeperException.ConnectionLossException();
                            }
                        }
                    });
                    try {
                        Object result = invoke(reader, fixture, mutation, recovered);
                        if (!isReplay(result, mutation)) {
                            violations.add(mutation + "/"
                                    + (recovered ? "recovered" : "ordinary")
                                    + " returned " + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/"
                                + (recovered ? "recovered" : "ordinary")
                                + " threw " + failure.getClass().getSimpleName());
                    }
                    assertTrue(beforeCommit.get(), mutation
                            + " must reach BEFORE_COMMIT");
                    assertTrue(ackLost.get(), mutation
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                }
            }
            assertTrue(violations.isEmpty(),
                    "the unchanged incarnation must preserve replay semantics: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void commitAckLossCannotReplayEquivalentReplacementAuthority()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : new LateMutation[] {
                        LateMutation.PUBLISH,
                        LateMutation.ACCEPT,
                        LateMutation.TERMINAL,
                        LateMutation.MEMBER,
                        LateMutation.INTENT,
                        LateMutation.ABORT }) {
                    String runId = "ack-replacement-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, mutation, recovered);
                    AtomicBoolean replaced = new AtomicBoolean();
                    AtomicReference<List<String>> replacementFingerprint =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore.GuardedMutationMoment
                                .AFTER_COMMIT_BEFORE_ACK
                                && replaced.compareAndSet(false, true)) {
                            retireAndPurge(writer, original);
                            Fixture replacement = fixture(
                                    writer, runId, mutation, recovered);
                            invoke(writer, replacement, mutation, recovered);
                            replacementFingerprint.set(
                                    runSubtreeFingerprint(second, runId));
                            throw new KeeperException.ConnectionLossException();
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, original, mutation, recovered);
                        if (!isStale(result, mutation)) {
                            violations.add(mutation + "/"
                                    + (recovered ? "recovered" : "ordinary")
                                    + " returned " + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/"
                                + (recovered ? "recovered" : "ordinary")
                                + " threw " + failure.getClass().getSimpleName());
                    }
                    assertTrue(replaced.get(), mutation
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    assertEquals(replacementFingerprint.get(),
                            runSubtreeFingerprint(second, runId),
                            mutation + "/" + recovered
                                    + " must leave ACTIVE replacement B "
                                    + "subtree byte-for-byte unchanged");
                }
            }
            assertTrue(violations.isEmpty(),
                    "old calls must not reconcile against replacement B: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void commitAckLossAfterFinalPurgeReturnsStaleWithoutReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : ackMutationKinds()) {
                    String runId = "ack-purged-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, mutation, recovered);
                    AtomicBoolean purged = new AtomicBoolean();
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && purged.compareAndSet(false, true)) {
                            retireAndPurge(writer, original);
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, original, mutation, recovered);
                        if (!isStale(result, mutation)) {
                            violations.add(mutation + "/" + recovered
                                    + " returned "
                                    + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(purged.get(), mutation + "/" + recovered
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    assertNull(writer.get(runId));
                }
            }
            assertTrue(violations.isEmpty(),
                    "a committed old call must not need replacement data to "
                            + "project stale: " + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void commitAckLossCannotReplayEquivalentTerminalReplacementAuthority()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : ackMutationKinds()) {
                    String runId = "ack-terminal-replacement-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, mutation, recovered);
                    AtomicBoolean replaced = new AtomicBoolean();
                    AtomicReference<List<String>> replacementFingerprint =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && replaced.compareAndSet(false, true)) {
                            retireAndPurge(writer, original);
                            Fixture replacement = fixture(
                                    writer, runId, mutation, recovered);
                            invoke(writer, replacement, mutation, recovered);
                            retire(writer, replacement);
                            replacementFingerprint.set(
                                    runSubtreeFingerprint(second, runId));
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, original, mutation, recovered);
                        if (!isStale(result, mutation)) {
                            violations.add(mutation + "/" + recovered
                                    + " returned "
                                    + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    }
                    assertTrue(replaced.get(), mutation + "/" + recovered
                            + " must reach AFTER_COMMIT_BEFORE_ACK");
                    assertTrue(writer.get(runId).status().isTerminal(),
                            "replacement B must remain committed terminal");
                    assertEquals(replacementFingerprint.get(),
                            runSubtreeFingerprint(second, runId),
                            mutation + "/" + recovered
                                    + " must leave TERMINAL replacement B "
                                    + "subtree byte-for-byte unchanged");
                }
            }
            assertTrue(violations.isEmpty(),
                    "terminal replacement B must not prove old ACK success: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void commitAckLossCannotMutateReplacementPrepublicationLayout()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : ackMutationKinds()) {
                    String runId = "ack-prepublication-replacement-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture original = fixture(
                            writer, runId, mutation, recovered);
                    CountDownLatch layoutReady = new CountDownLatch(1);
                    CountDownLatch allowCreate = new CountDownLatch(1);
                    AtomicBoolean replacementPaused = new AtomicBoolean();
                    AtomicReference<Fixture> replacement =
                            new AtomicReference<>();
                    AtomicReference<List<String>> replacementFingerprint =
                            new AtomicReference<>();
                    AtomicReference<Throwable> createFailure =
                            new AtomicReference<>();
                    AtomicReference<Thread> createThread =
                            new AtomicReference<>();
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && createThread.get() == null) {
                            retireAndPurge(writer, original);
                            writer.evidenceReadObserverForTest(phase -> {
                                if (phase == ZkRunStateStore.EvidenceReadPhase
                                        .APPLY_AFTER_INITIAL_SNAPSHOT
                                        && replacementPaused.compareAndSet(
                                                false, true)) {
                                    layoutReady.countDown();
                                    if (!allowCreate.await(
                                            10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "replacement CREATE release "
                                                        + "timed out");
                                    }
                                }
                            });
                            Thread creator = new Thread(() -> {
                                try {
                                    replacement.set(fixture(
                                            writer, runId,
                                            mutation, recovered));
                                } catch (Throwable failure) {
                                    createFailure.set(failure);
                                }
                            }, "zk-prepublication-" + mutation + "-"
                                    + recovered);
                            if (!createThread.compareAndSet(null, creator)) {
                                throw new AssertionError(
                                        "replacement creator started twice");
                            }
                            creator.start();
                            if (!layoutReady.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError(
                                        "replacement layout was not published");
                            }
                            replacementFingerprint.set(
                                    runSubtreeFingerprint(second, runId));
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });
                    try {
                        Object result = invoke(
                                reader, original, mutation, recovered);
                        if (!isStale(result, mutation)) {
                            violations.add(mutation + "/" + recovered
                                    + " returned "
                                    + resultCode(result, mutation));
                        }
                    } catch (RuntimeException failure) {
                        violations.add(mutation + "/" + recovered + " threw "
                                + failure.getClass().getSimpleName());
                    } finally {
                        if (replacementFingerprint.get() != null) {
                            try {
                                if (!replacementFingerprint.get().equals(
                                        runSubtreeFingerprint(
                                                second, runId))) {
                                    violations.add(mutation + "/" + recovered
                                            + " mutated paused "
                                            + "prepublication subtree");
                                }
                            } catch (Exception inspectionFailure) {
                                violations.add(mutation + "/" + recovered
                                        + " prepublication inspection threw "
                                        + inspectionFailure.getClass()
                                                .getSimpleName());
                            }
                        }
                        allowCreate.countDown();
                        Thread creator = createThread.get();
                        if (creator != null) {
                            creator.join(TimeUnit.SECONDS.toMillis(10));
                            assertTrue(!creator.isAlive(),
                                    "replacement creator must terminate");
                        }
                    }
                    assertTrue(replacementPaused.get(), mutation + "/"
                            + recovered + " must expose real prepublication");
                    assertNull(createFailure.get(),
                            "replacement CREATE must complete after release");
                    assertTrue(replacement.get() != null,
                            "replacement fixture must complete");
                    assertReplacementUntouched(
                            writer, replacement.get(), mutation);
                }
            }
            assertTrue(violations.isEmpty(),
                    "prepublication replacement B must not be read or mutated "
                            + "as old ACK evidence: " + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void commitAckConfirmationTransportFailsImmediatelyForEveryEntryPoint()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
            CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : ackMutationKinds()) {
                    String runId = "ack-confirmation-transport-"
                            + mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, mutation, recovered);
                    AtomicBoolean ackLost = new AtomicBoolean();
                    AtomicInteger confirmationFaults = new AtomicInteger();
                    reader.evidenceReadObserverForTest(phase -> {
                        if (ackLost.get()
                                && phase == ZkRunStateStore.EvidenceReadPhase
                                .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                            confirmationFaults.incrementAndGet();
                            throw new KeeperException
                                    .OperationTimeoutException();
                        }
                    });
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                                && ackLost.compareAndSet(false, true)) {
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });

                    RuntimeException failure = null;
                    try {
                        invoke(reader, fixture, mutation, recovered);
                    } catch (RuntimeException thrown) {
                        failure = thrown;
                    }
                    String label = mutation + "/" + recovered;
                    if (!(failure
                            instanceof RunStateStoreUnavailableException)) {
                        violations.add(label + " returned "
                                + (failure == null ? "normally"
                                        : failure.getClass().getSimpleName()));
                    }
                    if (!ackLost.get()) {
                        violations.add(label
                                + " did not lose the committed ACK");
                    }
                    if (confirmationFaults.get() != 1) {
                        violations.add(label + " confirmation faults="
                                + confirmationFaults.get());
                    }

                    reader.evidenceReadObserverForTest(phase -> { });
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> { });
                    Object committed = invoke(
                            writer, fixture, mutation, recovered);
                    if (!isReplay(committed, mutation)) {
                        violations.add(label + " durable result="
                                + resultCode(committed, mutation));
                    }
                }
            }
            assertTrue(violations.isEmpty(),
                    "every final mutation ACK confirmation transport must "
                            + "fail closed after the transaction commits: "
                            + violations);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void finalMutationUnknownOutcomeUsesSixteenConfirmedMisses()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            List<String> violations = new ArrayList<>();
            for (boolean recovered : new boolean[] { false, true }) {
                for (LateMutation mutation : ackMutationKinds()) {
                    String label = mutation + "/" + recovered;
                    String runId = "confirmed-miss-"
                            + mutation.name().toLowerCase(
                                    java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary");
                    Fixture fixture = fixture(
                            writer, runId, mutation, recovered);
                    AtomicInteger attempts = new AtomicInteger();
                    AtomicInteger confirmations = new AtomicInteger();
                    reader.evidenceReadObserverForTest(phase -> {
                        if (phase == ZkRunStateStore.EvidenceReadPhase
                                .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                            confirmations.incrementAndGet();
                        }
                    });
                    reader.guardedMutationObserverForTest(
                            (kind, ordinal, moment) -> {
                        if (kind == mutation.kind
                                && moment == ZkRunStateStore
                                .GuardedMutationMoment.BEFORE_COMMIT) {
                            attempts.incrementAndGet();
                            throw new KeeperException
                                    .ConnectionLossException();
                        }
                    });

                    RuntimeException failure = null;
                    try {
                        invoke(reader, fixture, mutation, recovered);
                    } catch (RuntimeException thrown) {
                        failure = thrown;
                    } finally {
                        reader.evidenceReadObserverForTest(phase -> { });
                        reader.guardedMutationObserverForTest(
                                (kind, ordinal, moment) -> { });
                    }
                    if (!(failure
                            instanceof RunStateStoreUnavailableException)) {
                        violations.add(label + " returned "
                                + (failure == null ? "normally"
                                        : failure.getClass().getSimpleName()));
                    }
                    if (attempts.get() != 16) {
                        violations.add(label + " attempts=" + attempts.get());
                    }
                    if (confirmations.get() != 16) {
                        violations.add(label + " confirmations="
                                + confirmations.get());
                    }
                    assertReplacementUntouched(writer, fixture, mutation);
                }
            }
            assertTrue(violations.isEmpty(),
                    "final mutation unknown outcomes require exactly sixteen "
                            + "successful incarnation confirmations that miss "
                            + "the intended write: " + violations);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryPublishBeforeHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.PUBLISH, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredPublishBeforeHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.PUBLISH, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryPublishAfterStagingIsStale() throws Exception {
        assertLateMutationStale(LateMutation.PUBLISH_AFTER_STAGING, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredPublishAfterStagingIsStale() throws Exception {
        assertLateMutationStale(LateMutation.PUBLISH_AFTER_STAGING, true);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void publishLayoutPostCommitReplacementWithoutAckLossIsStale()
            throws Exception {
        assertPublishPostCommitReplacementIsStale(
                ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void publishRootPostCommitReplacementWithoutAckLossIsStale()
            throws Exception {
        assertPublishPostCommitReplacementIsStale(
                ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void publishCorruptionConfirmationTransportFailsImmediately()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean timeout : new boolean[] { false, true }) {
                String runId = "publish-corruption-confirmation-"
                        + (timeout ? "timeout" : "connection-loss");
                Fixture fixture = fixture(
                        writer, runId, LateMutation.PUBLISH, false);
                AtomicBoolean layoutCorrupted = new AtomicBoolean();
                AtomicInteger validatedReadsAfterCorruption =
                        new AtomicInteger();
                AtomicInteger confirmationFaults = new AtomicInteger();
                TransactionWrites writes = new TransactionWrites();
                reader.transactionWriteObserverForTest(writes);
                reader.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> {
                            if (kind == ZkRunStateStore.GuardedMutationKind
                                    .PUBLISH_LAYOUT
                                    && moment == ZkRunStateStore
                                    .GuardedMutationMoment
                                    .AFTER_COMMIT_BEFORE_ACK
                                    && layoutCorrupted.compareAndSet(
                                            false, true)) {
                                second.delete().forPath(
                                        runRootPath(runId)
                                                + "/stage-assignment/"
                                                + "assignment-shards");
                            }
                        });
                reader.evidenceReadObserverForTest(phase -> {
                    if (layoutCorrupted.get()
                            && phase == ZkRunStateStore.EvidenceReadPhase
                                    .CORE_AFTER_VALIDATED_PAIR
                            && validatedReadsAfterCorruption
                                    .incrementAndGet() == 2) {
                        confirmationFaults.incrementAndGet();
                        if (timeout) {
                            throw new KeeperException
                                    .OperationTimeoutException();
                        }
                        throw new KeeperException.ConnectionLossException();
                    }
                });

                RuntimeException failure = assertThrows(
                        RuntimeException.class,
                        () -> invoke(reader, fixture,
                                LateMutation.PUBLISH, false));
                reader.evidenceReadObserverForTest(phase -> { });
                reader.guardedMutationObserverForTest(
                        (kind, ordinal, moment) -> { });

                assertTrue(failure
                                instanceof RunStateStoreUnavailableException,
                        "candidate confirmation transport must fail closed: "
                                + failure);
                assertTrue(layoutCorrupted.get(),
                        "test must create stable layout corruption");
                assertEquals(1, confirmationFaults.get(),
                        "confirmation transport must fail exactly once");
                assertEquals(1, writes.records.size(),
                        "confirmation failure must not replay or write again");
                assertEquals(ZkRunStateStore.GuardedMutationKind
                                .PUBLISH_LAYOUT,
                        writes.records.get(0).kind);

                List<String> afterUnavailable =
                        authorityFingerprint(second);
                reader.transactionWriteObserverForTest(
                        new TransactionWrites());
                IllegalStateException stableCorruption = assertThrows(
                        IllegalStateException.class,
                        () -> invoke(reader, fixture,
                                LateMutation.PUBLISH, false));
                assertTrue(stableCorruption.getMessage().contains(runId));
                assertEquals(afterUnavailable,
                        authorityFingerprint(second),
                        "fresh call must report stable corruption without "
                                + "repairing authority");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryAcceptBeforeRequiredSnapshotIsStale() throws Exception {
        assertLateMutationStale(LateMutation.ACCEPT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredAcceptBeforeRequiredSnapshotIsStale() throws Exception {
        assertLateMutationStale(LateMutation.ACCEPT, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryTerminalBeforeRequiredSnapshotIsStale() throws Exception {
        assertLateMutationStale(LateMutation.TERMINAL, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredTerminalBeforeRequiredSnapshotIsStale() throws Exception {
        assertLateMutationStale(LateMutation.TERMINAL, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryMemberBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.MEMBER, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredMemberBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.MEMBER, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryIntentBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.INTENT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredIntentBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.INTENT, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryAbortBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.ABORT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredAbortBeforeAbortGuardHelperIsStale() throws Exception {
        assertLateMutationStale(LateMutation.ABORT, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryPublishCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.PUBLISH, false);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void recoveredPublishCommitCannotCrossReplacement() throws Exception {
        for (int round = 0; round < 16; round++) {
            assertCommitFenceStale(
                    LateMutation.PUBLISH, true, "-round-" + round);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryAcceptCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.ACCEPT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredAcceptCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.ACCEPT, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryTerminalCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.TERMINAL, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredTerminalCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.TERMINAL, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryMemberCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.MEMBER, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredMemberCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.MEMBER, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryIntentCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.INTENT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredIntentCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.INTENT, true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void ordinaryAbortCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.ABORT, false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recoveredAbortCommitCannotCrossReplacement() throws Exception {
        assertCommitFenceStale(LateMutation.ABORT, true);
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void everyAssignmentMutationFailsClosedForEveryStableCoreOrphan()
            throws Exception {
        List<String> violations = new ArrayList<>();
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (LateMutation mutation : ackMutationKinds()) {
                for (boolean recovered : new boolean[] { false, true }) {
                    for (boolean missingAbortGuard
                            : stableAbortGuardVariants(mutation)) {
                        for (StableOrphanShape shape
                                : StableOrphanShape.values()) {
                        String runId = "stable-assignment-orphan-"
                                + mutation.name().toLowerCase(
                                        java.util.Locale.ROOT)
                                + (recovered ? "-recovered-" : "-ordinary-")
                                + (missingAbortGuard
                                        ? "legacy-abort-guard-" : "guarded-")
                                + shape.name().toLowerCase(
                                        java.util.Locale.ROOT);
                        Fixture fixture = fixture(
                                writer, runId, mutation, recovered);
                        if (missingAbortGuard) {
                            second.delete().forPath(abortGuardPath(runId));
                        }
                        Stat rootBefore = second.checkExists().forPath(
                                runRootPath(runId));
                        assertNotNull(rootBefore);
                        assertNotNull(second.checkExists().forPath(
                                runRootPath(runId) + "/snapshot"));
                        assertNotNull(second.checkExists().forPath(
                                lifecycleEntryPath(runId)));

                        AtomicBoolean transitioned = new AtomicBoolean();
                        AtomicReference<List<String>> before =
                                new AtomicReference<>();
                        if (shape == StableOrphanShape
                                .LAYOUT_ONLY_AFTER_BOTH) {
                            if (missingAbortGuard) {
                                reader.evidenceReadObserverForTest(phase -> {
                                    if (phase == ZkRunStateStore
                                                    .EvidenceReadPhase
                                                    .CORE_AFTER_VALIDATED_PAIR
                                            && transitioned.compareAndSet(
                                                    false, true)) {
                                        assertNotNull(second.checkExists()
                                                .forPath(runRootPath(runId)
                                                        + "/snapshot"));
                                        assertNotNull(second.checkExists()
                                                .forPath(lifecycleEntryPath(
                                                        runId)));
                                        shape.corrupt(second, runId);
                                        assertStableOrphanRoot(
                                                second, runId, rootBefore);
                                        before.set(authorityFingerprint(second));
                                    }
                                });
                            } else {
                                reader.guardedMutationObserverForTest(
                                        (kind, ordinal, moment) -> {
                                    if (kind == mutation.kind
                                            && moment == ZkRunStateStore
                                                    .GuardedMutationMoment
                                                    .BEFORE_COMMIT
                                            && transitioned.compareAndSet(
                                                    false, true)) {
                                        assertNotNull(second.checkExists()
                                                .forPath(runRootPath(runId)
                                                        + "/snapshot"));
                                        assertNotNull(second.checkExists()
                                                .forPath(lifecycleEntryPath(
                                                        runId)));
                                        shape.corrupt(second, runId);
                                        assertStableOrphanRoot(
                                                second, runId, rootBefore);
                                        before.set(
                                                authorityFingerprint(second));
                                    }
                                });
                            }
                        } else {
                            shape.corrupt(second, runId);
                            assertStableOrphanRoot(
                                    second, runId, rootBefore);
                            before.set(authorityFingerprint(second));
                        }

                        Throwable failure = null;
                        Object returned = null;
                        try {
                            returned = invoke(
                                    reader, fixture, mutation, recovered);
                        } catch (Throwable caught) {
                            failure = caught;
                        } finally {
                            reader.evidenceReadObserverForTest(phase -> { });
                            reader.guardedMutationObserverForTest(
                                    (kind, ordinal, moment) -> { });
                        }
                        String label = mutation + "/"
                                + (recovered ? "recovered" : "ordinary")
                                + "/" + (missingAbortGuard
                                        ? "legacy-abort-guard/" : "")
                                + shape;
                        if (shape == StableOrphanShape
                                .LAYOUT_ONLY_AFTER_BOTH
                                && !transitioned.get()) {
                            violations.add(label
                                    + " did not transition inside the "
                                    + "selected mutation call");
                        }
                        if (!(failure instanceof IllegalStateException)
                                || failure
                                        instanceof RunStateStoreUnavailableException
                                || failure.getMessage() == null
                                || !failure.getMessage().contains(runId)) {
                            violations.add(label
                                    + " did not throw a run-specific "
                                    + "stable-corruption IllegalStateException "
                                    + "(returned=" + resultDescription(
                                            returned, mutation)
                                    + ", failure=" + failure + ")");
                        }
                        List<String> after = authorityFingerprint(second);
                        if (before.get() == null
                                || !before.get().equals(after)) {
                            violations.add(label
                                    + " mutated the authoritative ZooKeeper "
                                    + "subtree");
                        }
                    }
                }
            }
        }
        }
        assertEquals(Collections.emptyList(), violations,
                "assignment mutations must reject every stable core orphan "
                        + "without stale projection, repair, or progress");
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void everyAssignmentTransactionUsesExactD8OperationsAndRealRequestBytes()
            throws Exception {
        List<String> violations = new ArrayList<>();
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            for (LateMutation mutation : ackMutationKinds()) {
                for (boolean recovered : new boolean[] { false, true }) {
                    String label = mutation + "/"
                            + (recovered ? "recovered" : "ordinary");
                    Fixture fixture = fixture(store,
                            "d8-" + label.toLowerCase(Locale.ROOT)
                                    .replace('/', '-'),
                            mutation, recovered);
                    TransactionWrites writes = new TransactionWrites();
                    store.transactionWriteObserverForTest(writes);
                    Throwable failure = null;
                    try {
                        invoke(store, fixture, mutation, recovered);
                    } catch (Throwable caught) {
                        failure = caught;
                    }
                    if (failure != null) {
                        violations.add(label + " failed before accounting: "
                                + failure);
                        continue;
                    }
                    assertD8Writes(
                            violations, label, mutation, recovered,
                            writes.records);
                }
            }

            for (LateMutation mutation : Arrays.asList(
                    LateMutation.MEMBER,
                    LateMutation.INTENT,
                    LateMutation.ABORT)) {
                for (boolean recovered : new boolean[] { false, true }) {
                    String label = mutation + "/"
                            + (recovered ? "recovered" : "ordinary")
                            + "/legacy-abort-guard";
                    Fixture fixture = fixture(store,
                            "d8-helper-" + label.toLowerCase(Locale.ROOT)
                                    .replace('/', '-'),
                            mutation, recovered);
                    client.delete().forPath(abortGuardPath(fixture.runId));
                    TransactionWrites writes = new TransactionWrites();
                    store.transactionWriteObserverForTest(writes);
                    try {
                        invoke(store, fixture, mutation, recovered);
                    } catch (Throwable caught) {
                        violations.add(label + " failed before accounting: "
                                + caught);
                        continue;
                    }
                    if (writes.records.size() != 2) {
                        violations.add(label + " expected helper+final writes, got "
                                + writes.records);
                        continue;
                    }
                    assertD8Record(violations, label + "/helper",
                            writes.records.get(0), abortGuardKind(mutation),
                            -1, 4);
                    assertD8Record(violations, label + "/final",
                            writes.records.get(1), mutation.kind, -1,
                            expectedFinalOperations(mutation, recovered));
                }
            }

            Fixture repair = fixture(store, "d8-publish-intent-repair",
                    LateMutation.PUBLISH, false);
            invoke(store, repair, LateMutation.PUBLISH, false);
            client.delete().forPath(manifestIntentPath(repair.runId));
            TransactionWrites repairWrites = new TransactionWrites();
            store.transactionWriteObserverForTest(repairWrites);
            invoke(store, repair, LateMutation.PUBLISH, false);
            if (repairWrites.records.size() != 1) {
                violations.add("PUBLISH_INTENT_REPAIR expected one write, got "
                        + repairWrites.records);
            } else {
                assertD8Record(violations, "PUBLISH_INTENT_REPAIR",
                        repairWrites.records.get(0),
                        ZkRunStateStore.GuardedMutationKind
                                .PUBLISH_INTENT_REPAIR,
                        -1, 5);
            }
        }
        assertEquals(Collections.emptyList(), violations,
                "every assignment/helper transaction must match the frozen publication rule "
                        + "using a real MultiOperationRecord byte count");
    }

    private static void assertD8Writes(
            List<String> violations, String label,
            LateMutation mutation, boolean recovered,
            List<TransactionWrite> records) {
        if (mutation == LateMutation.PUBLISH) {
            int[] expected = recovered
                    ? new int[] { 9, 6, 8, 7 }
                    : new int[] { 9, 5, 8, 6 };
            ZkRunStateStore.GuardedMutationKind[] kinds = {
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_MARKER,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_SHARD,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT };
            if (records.size() != expected.length) {
                violations.add(label + " expected four publication writes, got "
                        + records);
                return;
            }
            for (int i = 0; i < expected.length; i++) {
                assertD8Record(violations, label + "/" + kinds[i],
                        records.get(i), kinds[i],
                        i == 2 ? 0 : -1, expected[i]);
            }
            return;
        }
        if (records.size() != 1) {
            violations.add(label + " expected one final write, got " + records);
            return;
        }
        assertD8Record(violations, label, records.get(0), mutation.kind,
                -1, expectedFinalOperations(mutation, recovered));
    }

    private static int expectedFinalOperations(
            LateMutation mutation, boolean recovered) {
        switch (mutation) {
            case ACCEPT:
                return recovered ? 7 : 6;
            case TERMINAL:
                return recovered ? 8 : 7;
            case MEMBER:
                return recovered ? 11 : 10;
            case INTENT:
                return recovered ? 9 : 8;
            case ABORT:
                return recovered ? 8 : 7;
            default:
                throw new AssertionError(
                        "no final operation formula for " + mutation);
        }
    }

    private static void assertD8Record(
            List<String> violations, String label, TransactionWrite actual,
            ZkRunStateStore.GuardedMutationKind kind,
            int ordinal, int operationCount) {
        if (actual.kind != kind || actual.ordinal != ordinal
                || actual.operationCount != operationCount
                || actual.requestBytes <= 0
                || actual.requestBytes >= 900_000) {
            violations.add(label + " expected " + kind + "[" + ordinal
                    + "] ops=" + operationCount
                    + " and 0<realBytes<900000, got " + actual);
        }
    }

    private static void assertLateMutationStale(
            LateMutation mutation, boolean recovered) throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            Fixture fixture = fixture(writer,
                    mutation.name().toLowerCase(java.util.Locale.ROOT)
                            + (recovered ? "-recovered" : "-ordinary"),
                    mutation, recovered);
            AtomicBoolean purged = new AtomicBoolean();
            reader.evidenceReadObserverForTest(phase -> {
                if (phase == mutation.phase
                        && purged.compareAndSet(false, true)) {
                    retireAndPurge(writer, fixture);
                }
            });

            Object result = invoke(reader, fixture, mutation, recovered);

            assertStale(result, mutation);
            assertNull(writer.get(fixture.runId));
        }
    }

    private static void assertCommitFenceStale(
            LateMutation mutation, boolean recovered) throws Exception {
        assertCommitFenceStale(mutation, recovered, "");
    }

    private static void assertCommitFenceStale(
            LateMutation mutation, boolean recovered, String runIdSuffix)
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            String runId = "commit-"
                    + mutation.name().toLowerCase(java.util.Locale.ROOT)
                    + (recovered ? "-recovered" : "-ordinary")
                    + runIdSuffix;
            Fixture original = fixture(writer, runId, mutation, recovered);
            AtomicBoolean replaced = new AtomicBoolean();
            final Fixture[] replacement = new Fixture[1];
            AtomicReference<List<String>> replacementFingerprint =
                    new AtomicReference<>();
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == mutation.kind
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaced.compareAndSet(false, true)) {
                    retireAndPurge(writer, original);
                    replacement[0] = fixture(
                            writer, runId, mutation, recovered);
                    replacementFingerprint.set(
                            runSubtreeFingerprint(second, runId));
                }
            });

            Object result = invoke(reader, original, mutation, recovered);

            assertStale(result, mutation);
            assertReplacementUntouched(writer, replacement[0], mutation);
            assertEquals(replacementFingerprint.get(),
                    runSubtreeFingerprint(second, runId),
                    "old assignment call must not mutate replacement B");
        }
    }

    private static void assertPublishPostCommitReplacementIsStale(
            ZkRunStateStore.GuardedMutationKind mutationKind) throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework first = connectedClient(server);
             CuratorFramework second = connectedClient(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(first);
            ZkRunStateStore writer = new ZkRunStateStore(second);
            for (boolean recovered : new boolean[] { false, true }) {
                String runId = "post-commit-replacement-"
                        + mutationKind.name().toLowerCase(Locale.ROOT)
                        + (recovered ? "-recovered" : "-ordinary");
                Fixture original = fixture(
                        writer, runId, LateMutation.PUBLISH, recovered);
                AtomicBoolean replaced = new AtomicBoolean();
                AtomicReference<Fixture> replacement = new AtomicReference<>();
                AtomicReference<List<String>> replacementFingerprint =
                        new AtomicReference<>();
                reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                    if (kind == mutationKind
                            && moment == ZkRunStateStore.GuardedMutationMoment
                                    .AFTER_COMMIT_BEFORE_ACK
                            && replaced.compareAndSet(false, true)) {
                        retireAndPurge(writer, original);
                        Fixture next = fixture(
                                writer, runId, LateMutation.PUBLISH, recovered);
                        replacement.set(next);
                        replacementFingerprint.set(
                                runSubtreeFingerprint(second, runId));
                    }
                });

                Object result = invoke(
                        reader, original, LateMutation.PUBLISH, recovered);

                assertTrue(replaced.get(), mutationKind + "/" + recovered
                        + " must replace A after the helper transaction commits");
                assertStale(result, LateMutation.PUBLISH);
                assertReplacementUntouched(
                        writer, replacement.get(), LateMutation.PUBLISH);
                assertEquals(replacementFingerprint.get(),
                        runSubtreeFingerprint(second, runId),
                        "old publication must not inspect B as corruption or mutate it");
            }
        }
    }

    private static void assertReplacementUntouched(
            ZkRunStateStore store, Fixture replacement,
            LateMutation mutation) {
        StageAssignmentSnapshot assignments =
                store.assignmentSnapshot(replacement.fence);
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                assertNull(assignments);
                return;
            case ACCEPT:
                assertNull(assignments.acceptance(0));
                return;
            case TERMINAL:
                assertNull(assignments.terminal(0));
                return;
            case MEMBER:
                assertNull(store.manifestCommitJournal(
                        replacement.fence).member(0));
                return;
            case INTENT:
                assertNull(store.manifestCommitJournal(
                        replacement.fence).intent());
                return;
            case ABORT:
                assertNull(store.batchSinkAbort(replacement.fence));
                return;
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static boolean replacementTargetPresent(
            ZkRunStateStore store, Fixture replacement,
            LateMutation mutation) {
        switch (mutation) {
            case MEMBER:
                ManifestCommitJournalSnapshot memberJournal =
                        store.manifestCommitJournal(replacement.fence);
                return memberJournal != null
                        && memberJournal.member(0) != null;
            case INTENT:
                ManifestCommitJournalSnapshot intentJournal =
                        store.manifestCommitJournal(replacement.fence);
                return intentJournal != null
                        && intentJournal.intent() != null;
            case ABORT:
                return store.batchSinkAbort(replacement.fence) != null;
            default:
                throw new AssertionError(
                        "no abort helper target for " + mutation);
        }
    }

    private static Object invoke(
            ZkRunStateStore store, Fixture fixture,
            LateMutation mutation, boolean recovered) {
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                return recovered
                        ? store.publishRecovered(
                                fixture.recoveryToken, fixture.manifest)
                        : store.publish(fixture.manifest);
            case ACCEPT:
                return recovered
                        ? store.acceptRecovered(
                                fixture.recoveryToken, fixture.acceptance)
                        : store.accept(fixture.acceptance);
            case TERMINAL:
                return recovered
                        ? store.recordTerminalRecovered(
                                fixture.recoveryToken, fixture.terminal)
                        : store.recordTerminal(fixture.terminal);
            case MEMBER:
                return recovered
                        ? store.recordManifestMemberRecovered(
                                fixture.recoveryToken, fixture.member)
                        : store.recordManifestMember(fixture.member);
            case INTENT:
                return recovered
                        ? store.commitManifestIntentRecovered(
                                fixture.recoveryToken, fixture.intent)
                        : store.commitManifestIntent(fixture.intent);
            case ABORT:
                return recovered
                        ? store.recordBatchSinkAbortRecovered(
                                fixture.recoveryToken, fixture.abort)
                        : store.recordBatchSinkAbort(fixture.abort);
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static void assertStale(Object result, LateMutation mutation) {
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                StageManifestPublication publication =
                        (StageManifestPublication) result;
                assertEquals(StageManifestPublication.Code.STALE_RUN,
                        publication.code());
                assertNull(publication.snapshot());
                return;
            case ACCEPT:
                StageAcceptanceDecision acceptance =
                        (StageAcceptanceDecision) result;
                assertEquals(StageAcceptanceDecision.Code.STALE_RUN,
                        acceptance.code());
                assertNull(acceptance.acceptance());
                return;
            case TERMINAL:
                StageTerminalDecision terminal = (StageTerminalDecision) result;
                assertEquals(StageTerminalDecision.Code.STALE_RUN, terminal.code());
                assertNull(terminal.receipt());
                return;
            case MEMBER:
                ManifestMemberDecision member = (ManifestMemberDecision) result;
                assertEquals(ManifestMemberDecision.Code.STALE_RUN, member.code());
                assertNull(member.receipt());
                return;
            case INTENT:
                ManifestIntentDecision intent = (ManifestIntentDecision) result;
                assertEquals(ManifestIntentDecision.Code.STALE_RUN, intent.code());
                assertNull(intent.intent());
                return;
            case ABORT:
                BatchSinkAbortDecision abort = (BatchSinkAbortDecision) result;
                assertEquals(BatchSinkAbortDecision.Code.STALE_RUN, abort.code());
                assertNull(abort.record());
                return;
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static boolean isStale(Object result, LateMutation mutation) {
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                StageManifestPublication publication =
                        (StageManifestPublication) result;
                return publication.code()
                                == StageManifestPublication.Code.STALE_RUN
                        && publication.snapshot() == null;
            case ACCEPT:
                StageAcceptanceDecision acceptance =
                        (StageAcceptanceDecision) result;
                return acceptance.code()
                                == StageAcceptanceDecision.Code.STALE_RUN
                        && acceptance.acceptance() == null;
            case TERMINAL:
                StageTerminalDecision terminal =
                        (StageTerminalDecision) result;
                return terminal.code()
                                == StageTerminalDecision.Code.STALE_RUN
                        && terminal.receipt() == null;
            case MEMBER:
                ManifestMemberDecision member =
                        (ManifestMemberDecision) result;
                return member.code()
                                == ManifestMemberDecision.Code.STALE_RUN
                        && member.receipt() == null;
            case INTENT:
                ManifestIntentDecision intent =
                        (ManifestIntentDecision) result;
                return intent.code()
                                == ManifestIntentDecision.Code.STALE_RUN
                        && intent.intent() == null;
            case ABORT:
                BatchSinkAbortDecision abort =
                        (BatchSinkAbortDecision) result;
                return abort.code()
                                == BatchSinkAbortDecision.Code.STALE_RUN
                        && abort.record() == null;
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static boolean isReplay(Object result, LateMutation mutation) {
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                return ((StageManifestPublication) result).code()
                        == StageManifestPublication.Code.ALREADY_PUBLISHED;
            case ACCEPT:
                return ((StageAcceptanceDecision) result).code()
                        == StageAcceptanceDecision.Code.ALREADY_ACCEPTED;
            case TERMINAL:
                return ((StageTerminalDecision) result).code()
                        == StageTerminalDecision.Code.ALREADY_RECORDED;
            case MEMBER:
                return ((ManifestMemberDecision) result).code()
                        == ManifestMemberDecision.Code.ALREADY_RECORDED;
            case INTENT:
                return ((ManifestIntentDecision) result).code()
                        == ManifestIntentDecision.Code.ALREADY_COMMITTED;
            case ABORT:
                return ((BatchSinkAbortDecision) result).code()
                        == BatchSinkAbortDecision.Code.ALREADY_RECORDED;
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static boolean isCommitted(Object result, LateMutation mutation) {
        switch (mutation) {
            case MEMBER:
                return ((ManifestMemberDecision) result).code()
                        == ManifestMemberDecision.Code.RECORDED;
            case INTENT:
                return ((ManifestIntentDecision) result).code()
                        == ManifestIntentDecision.Code.COMMITTED;
            case ABORT:
                return ((BatchSinkAbortDecision) result).code()
                        == BatchSinkAbortDecision.Code.RECORDED;
            default:
                throw new AssertionError("no abort guard for " + mutation);
        }
    }

    private static ZkRunStateStore.GuardedMutationKind abortGuardKind(
            LateMutation mutation) {
        switch (mutation) {
            case MEMBER:
                return ZkRunStateStore.GuardedMutationKind
                        .MANIFEST_MEMBER_ABORT_GUARD;
            case INTENT:
                return ZkRunStateStore.GuardedMutationKind
                        .MANIFEST_INTENT_ABORT_GUARD;
            case ABORT:
                return ZkRunStateStore.GuardedMutationKind.SINK_ABORT_GUARD;
            default:
                throw new AssertionError("no abort guard for " + mutation);
        }
    }

    private static ZkRunStateStore.GuardedMutationKind[] publishHelperKinds() {
        return new ZkRunStateStore.GuardedMutationKind[] {
                ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT,
                ZkRunStateStore.GuardedMutationKind.PUBLISH_INTENT_REPAIR,
                ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_MARKER,
                ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_SHARD };
    }

    private static void assertHelperConfirmationUnavailable(
            ZkRunStateStore store,
            ZkRunStateStore.GuardedMutationKind target,
            String label,
            ZkRunStateStore.GuardedMutationObserver beforeObserver,
            Runnable invocation, List<String> violations) {
        AtomicBoolean ackLost = new AtomicBoolean();
        AtomicInteger confirmationFaults = new AtomicInteger();
        store.evidenceReadObserverForTest(phase -> {
            if (ackLost.get()
                    && phase == ZkRunStateStore.EvidenceReadPhase
                    .CORE_MISMATCH_BEFORE_CONFIRMATION) {
                confirmationFaults.incrementAndGet();
                throw new KeeperException.OperationTimeoutException();
            }
        });
        store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
            beforeObserver.at(kind, ordinal, moment);
            if (kind == target
                    && moment == ZkRunStateStore.GuardedMutationMoment
                    .AFTER_COMMIT_BEFORE_ACK
                    && ackLost.compareAndSet(false, true)) {
                throw new KeeperException.ConnectionLossException();
            }
        });
        RuntimeException failure = null;
        try {
            invocation.run();
        } catch (RuntimeException thrown) {
            failure = thrown;
        } finally {
            store.evidenceReadObserverForTest(phase -> { });
            store.guardedMutationObserverForTest(
                    (kind, ordinal, moment) -> { });
        }
        if (!ackLost.get()) {
            violations.add(label + " did not lose its committed ACK");
        }
        if (!(failure instanceof RunStateStoreUnavailableException)) {
            violations.add(label + " returned "
                    + (failure == null ? "normally"
                            : failure.getClass().getSimpleName()));
        }
        if (confirmationFaults.get() != 1) {
            violations.add(label + " confirmation faults="
                    + confirmationFaults.get());
        }
    }

    private static LateMutation[] ackMutationKinds() {
        return new LateMutation[] {
                LateMutation.PUBLISH,
                LateMutation.ACCEPT,
                LateMutation.TERMINAL,
                LateMutation.MEMBER,
                LateMutation.INTENT,
                LateMutation.ABORT };
    }

    private static boolean[] stableAbortGuardVariants(
            LateMutation mutation) {
        return mutation == LateMutation.MEMBER
                || mutation == LateMutation.INTENT
                || mutation == LateMutation.ABORT
                ? new boolean[] { false, true }
                : new boolean[] { false };
    }

    private static void removeIntentForRepair(
            CuratorFramework client, String runId,
            ZkRunStateStore.GuardedMutationKind target,
            ZkRunStateStore.GuardedMutationKind observed,
            ZkRunStateStore.GuardedMutationMoment moment,
            AtomicBoolean removed) throws Exception {
        if (target == ZkRunStateStore.GuardedMutationKind.PUBLISH_INTENT_REPAIR
                && observed == ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT
                && moment == ZkRunStateStore.GuardedMutationMoment
                .AFTER_COMMIT_BEFORE_ACK
                && removed.compareAndSet(false, true)) {
            client.delete().forPath(manifestIntentPath(runId));
        }
    }

    private static String abortGuardPath(String runId) {
        return "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId)
                + "/sink-abort";
    }

    private static String manifestIntentPath(String runId) {
        return "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId)
                + "/stage-assignment/manifest-intent";
    }

    private static List<String> runSubtreeFingerprint(
            CuratorFramework client, String runId) throws Exception {
        String root = "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId);
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, root, root, fingerprint);
        return fingerprint;
    }

    private static List<String> authorityFingerprint(
            CuratorFramework client) throws Exception {
        List<String> fingerprint = new ArrayList<>();
        appendSubtreeFingerprint(client, "/rowweave", "/rowweave", fingerprint);
        return fingerprint;
    }

    private static String runRootPath(String runId) {
        return "/rowweave/run-state-v1/runs/"
                + io.rowweave.run.RunLifecycleCatalogKernel.digestHex(runId);
    }

    private static String lifecycleEntryPath(String runId) {
        String digest = io.rowweave.run.RunLifecycleCatalogKernel
                .digestHex(runId);
        return "/rowweave/run-lifecycle-v1/shards/" + digest.substring(0, 2)
                + "/entries/" + digest;
    }

    private static String lifecycleGuardPath(String runId) {
        String digest = io.rowweave.run.RunLifecycleCatalogKernel
                .digestHex(runId);
        return "/rowweave/run-lifecycle-v1/shards/" + digest.substring(0, 2);
    }

    private static void assertStableOrphanRoot(
            CuratorFramework client, String runId, Stat originalRoot)
            throws Exception {
        Stat retainedRoot = client.checkExists().forPath(runRootPath(runId));
        assertNotNull(retainedRoot, "stable orphan must retain the Run root");
        assertEquals(originalRoot.getCzxid(), retainedRoot.getCzxid(),
                "stable orphan must retain the same root incarnation");
        assertNotNull(client.checkExists().forPath(lifecycleGuardPath(runId)),
                "stable orphan must retain its shard guard");
    }

    private static void appendSubtreeFingerprint(
            CuratorFramework client, String root, String path,
            List<String> fingerprint) throws Exception {
        Stat stat = new Stat();
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        String relative = path.substring(root.length());
        fingerprint.add((relative.isEmpty() ? "/" : relative)
                + "|v=" + stat.getVersion()
                + "|cv=" + stat.getCversion()
                + "|av=" + stat.getAversion()
                + "|cz=" + stat.getCzxid()
                + "|mz=" + stat.getMzxid()
                + "|pz=" + stat.getPzxid()
                + "|data=" + Base64.getEncoder().encodeToString(data));
        List<String> children = client.getChildren().forPath(path);
        Collections.sort(children);
        for (String child : children) {
            appendSubtreeFingerprint(
                    client, root, path + "/" + child, fingerprint);
        }
    }

    private static Object resultCode(Object result, LateMutation mutation) {
        switch (mutation) {
            case PUBLISH:
            case PUBLISH_AFTER_STAGING:
                return ((StageManifestPublication) result).code();
            case ACCEPT:
                return ((StageAcceptanceDecision) result).code();
            case TERMINAL:
                return ((StageTerminalDecision) result).code();
            case MEMBER:
                return ((ManifestMemberDecision) result).code();
            case INTENT:
                return ((ManifestIntentDecision) result).code();
            case ABORT:
                return ((BatchSinkAbortDecision) result).code();
            default:
                throw new AssertionError("unhandled mutation " + mutation);
        }
    }

    private static String resultDescription(
            Object result, LateMutation mutation) {
        return result == null ? "null" : String.valueOf(
                resultCode(result, mutation));
    }

    private static Fixture fixture(
            ZkRunStateStore store, String runId,
            LateMutation mutation, boolean recovered) throws Exception {
        RunSnapshot ready = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, 0L, 1L));
        RunSnapshot active = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, ready.revision(),
                "attempt", "publication", 2L));
        RecoveryToken recovery = null;
        if (recovered) {
            RecoveryClaim claim = store.claim(
                    runId, active.revision(), 0L,
                    new RecoveryOwner("owner-" + runId, 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            recovery = claim.token();
        }
        RunAttemptFence fence = new RunAttemptFence(
                runId, active.currentAttempt().attemptId(),
                active.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 100L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 7001, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, active.artifactId(), 1L, Arrays.asList(record));
        Fixture fixture = new Fixture(runId, fence, recovery, manifest);
        if (mutation == LateMutation.PUBLISH
                || mutation == LateMutation.PUBLISH_AFTER_STAGING) return fixture;

        StageManifestPublication published = recovered
                ? store.publishRecovered(recovery, manifest)
                : store.publish(manifest);
        assertEquals(StageManifestPublication.Code.PUBLISHED, published.code());
        StageAssignmentSnapshot assignments = published.snapshot();
        fixture.acceptance = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        if (mutation == LateMutation.ACCEPT) return fixture;

        StageAcceptanceDecision accepted = recovered
                ? store.acceptRecovered(recovery, fixture.acceptance)
                : store.accept(fixture.acceptance);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, accepted.code());
        fixture.terminal = StageTerminalReceipt.succeeded(
                fixture.acceptance.token(), fixture.acceptance.commandDigest(),
                1L, 10L, 20L);
        if (mutation == LateMutation.TERMINAL) return fixture;

        StageTerminalDecision recorded = recovered
                ? store.recordTerminalRecovered(recovery, fixture.terminal)
                : store.recordTerminal(fixture.terminal);
        assertEquals(StageTerminalDecision.Code.RECORDED, recorded.code());
        fixture.member = ManifestMemberReceipt.sealed(
                fixture.acceptance.token(), fixture.acceptance.commandDigest(),
                "domain", 31L, "publication",
                new CommitManifest.Member(
                        "part-0", "data/31/part-0.data", "schema",
                        10L, digest(2)), 30L);
        if (mutation == LateMutation.MEMBER) return fixture;

        ManifestMemberDecision member = recovered
                ? store.recordManifestMemberRecovered(recovery, fixture.member)
                : store.recordManifestMember(fixture.member);
        assertEquals(ManifestMemberDecision.Code.RECORDED, member.code());
        advanceToCommitting(store, fixture);
        fixture.intent = store.manifestCommitJournal(fence).buildIntent();
        if (mutation == LateMutation.INTENT) return fixture;

        fixture.abort = BatchSinkAbortRecord.decided(
                fence, manifest.generation(),
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 31L, "publication", digest(3),
                "PREPUBLICATION_FAILURE", 40L);
        return fixture;
    }

    private static void advanceToCommitting(
            ZkRunStateStore store, Fixture fixture) {
        for (AttemptStatus target : Arrays.asList(
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING)) {
            RunSnapshot current = store.get(fixture.runId);
            RunCommand command = RunCommand.advanceAttempt(
                    target.name().toLowerCase(java.util.Locale.ROOT)
                            + "-" + fixture.runId,
                    fixture.runId, current.revision(),
                    fixture.fence.attemptId(), fixture.fence.fenceToken(),
                    target, false, current.updatedAtMillis() + 1L,
                    null, null);
            if (fixture.recoveryToken == null) accepted(store, command);
            else acceptedRecovered(store, fixture.recoveryToken, command);
        }
    }

    private static void retireAndPurge(
            ZkRunStateStore store, Fixture fixture) {
        RunSnapshot terminalSnapshot = retire(store, fixture);
        RunLifecycleRef lifecycle = store.lifecycle(fixture.runId);
        RunRetentionClaim claim = store.claim(
                fixture.runId, terminalSnapshot.revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner("retention-" + fixture.runId, 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        RetentionToken token = claim.token();
        RunPurgeProgress progress;
        do {
            progress = store.purge(token, 1000);
        } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
        assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
    }

    private static RunSnapshot retire(
            ZkRunStateStore store, Fixture fixture) {
        RunSnapshot current = store.get(fixture.runId);
        if (current.currentAttempt().status() == AttemptStatus.COMMITTING) {
            RunCommand reconciling = RunCommand.advanceAttempt(
                    "reconcile-" + fixture.runId, fixture.runId,
                    current.revision(), fixture.fence.attemptId(),
                    fixture.fence.fenceToken(), AttemptStatus.RECONCILING,
                    false, current.updatedAtMillis() + 1L,
                    "FENCED", "retention setup");
            current = fixture.recoveryToken == null
                    ? accepted(store, reconciling)
                    : acceptedRecovered(store, fixture.recoveryToken, reconciling);
        }
        RunCommand terminal = RunCommand.advanceAttempt(
                "terminal-" + fixture.runId, fixture.runId,
                current.revision(), fixture.fence.attemptId(),
                fixture.fence.fenceToken(), AttemptStatus.FAILED,
                false, current.updatedAtMillis() + 1L,
                "FAILED", "retention setup");
        RunSnapshot terminalSnapshot = fixture.recoveryToken == null
                ? accepted(store, terminal)
                : acceptedRecovered(store, fixture.recoveryToken, terminal);
        return terminalSnapshot;
    }

    private static RunSnapshot accepted(
            ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RunSnapshot acceptedRecovered(
            ZkRunStateStore store, RecoveryToken token, RunCommand command) {
        RunDecision decision = store.applyRecovered(token, command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
            client.close();
            throw new IllegalStateException("Curator did not connect");
        }
        return client;
    }

    private enum LateMutation {
        PUBLISH(ZkRunStateStore.EvidenceReadPhase
                .PUBLISH_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
                ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT),
        PUBLISH_AFTER_STAGING(ZkRunStateStore.EvidenceReadPhase
                .PUBLISH_AFTER_STAGING_BEFORE_FINAL_AUTHORITY,
                ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT),
        ACCEPT(ZkRunStateStore.EvidenceReadPhase
                .ACCEPT_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT,
                ZkRunStateStore.GuardedMutationKind.ACCEPT),
        TERMINAL(ZkRunStateStore.EvidenceReadPhase
                .TERMINAL_AFTER_LAYOUT_BEFORE_REQUIRED_SNAPSHOT,
                ZkRunStateStore.GuardedMutationKind.TERMINAL),
        MEMBER(ZkRunStateStore.EvidenceReadPhase
                .MANIFEST_MEMBER_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
                ZkRunStateStore.GuardedMutationKind.MANIFEST_MEMBER),
        INTENT(ZkRunStateStore.EvidenceReadPhase
                .MANIFEST_INTENT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
                ZkRunStateStore.GuardedMutationKind.MANIFEST_INTENT),
        ABORT(ZkRunStateStore.EvidenceReadPhase
                .SINK_ABORT_AFTER_LAYOUT_BEFORE_AUTHORITY_AND_HELPER,
                ZkRunStateStore.GuardedMutationKind.SINK_ABORT);

        private final ZkRunStateStore.EvidenceReadPhase phase;
        private final ZkRunStateStore.GuardedMutationKind kind;

        LateMutation(
                ZkRunStateStore.EvidenceReadPhase phase,
                ZkRunStateStore.GuardedMutationKind kind) {
            this.phase = phase;
            this.kind = kind;
        }
    }

    private enum StableOrphanShape {
        SNAPSHOT_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecycleEntryPath(runId));
            }
        },
        LIFECYCLE_ONLY {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(runRootPath(runId) + "/snapshot");
            }
        },
        LAYOUT_ONLY_AFTER_BOTH {
            @Override
            void corrupt(CuratorFramework client, String runId) throws Exception {
                client.delete().forPath(lifecycleEntryPath(runId));
                client.delete().forPath(runRootPath(runId) + "/snapshot");
            }
        };

        abstract void corrupt(CuratorFramework client, String runId)
                throws Exception;
    }

    private enum AckReplacementState {
        PURGED,
        PREPUBLICATION,
        ACTIVE,
        TERMINAL
    }

    private static final class TransactionWrites
            implements ZkRunStateStore.TransactionWriteObserver {
        private final List<TransactionWrite> records = new ArrayList<>();

        @Override
        public void onWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            records.add(new TransactionWrite(
                    kind, ordinal, operationCount, requestBytes));
        }
    }

    private static final class TransactionWrite {
        private final ZkRunStateStore.GuardedMutationKind kind;
        private final int ordinal;
        private final int operationCount;
        private final int requestBytes;

        private TransactionWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            this.kind = kind;
            this.ordinal = ordinal;
            this.operationCount = operationCount;
            this.requestBytes = requestBytes;
        }

        @Override
        public String toString() {
            return kind + "[" + ordinal + "] ops=" + operationCount
                    + " bytes=" + requestBytes;
        }
    }

    private static final class Fixture {
        private final String runId;
        private final RunAttemptFence fence;
        private final RecoveryToken recoveryToken;
        private final StageAssignmentManifest manifest;
        private StageAssignmentAcceptance acceptance;
        private StageTerminalReceipt terminal;
        private ManifestMemberReceipt member;
        private ManifestCommitIntent intent;
        private BatchSinkAbortRecord abort;

        private Fixture(
                String runId, RunAttemptFence fence,
                RecoveryToken recoveryToken,
                StageAssignmentManifest manifest) {
            this.runId = runId;
            this.fence = fence;
            this.recoveryToken = recoveryToken;
            this.manifest = manifest;
        }
    }
}
