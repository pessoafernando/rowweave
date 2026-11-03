package io.rowweave.plan;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ManifestFileSink;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.FileCommitGenerationAuthority;
import io.rowweave.commit.FileCommitDomainLease;
import io.rowweave.commit.ManifestCommitUncertainException;
import io.rowweave.commit.ManifestFileStore;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Coordinates prepare, publish, rollback and finalize for several Sinks in one JVM. */
final class OutputCommitCoordinator implements AutoCloseable {

    private final Sink[] outputs;
    private final Map<Integer, Sink> stageOutputs = new HashMap<>();
    private final ManifestGroup manifestGroup;
    private final PlanGraph graph;

    OutputCommitCoordinator(PlanGraph graph, List<Integer> sinkIndexes) {
        this.graph = graph;
        outputs = new Sink[sinkIndexes.size()];
        for (int i = 0; i < sinkIndexes.size(); i++) {
            int sinkIndex = sinkIndexes.get(i);
            Sink output = graph.node(sinkIndex).output();
            outputs[i] = output;
            stageOutputs.put(sinkIndex, preparingView(output));
        }
        manifestGroup = outputs.length > 0 && outputs[0] instanceof ManifestFileSink
                ? new ManifestGroup(outputs) : null;
    }

    Sink stageOutput(int sinkIndex) {
        Sink output = stageOutputs.get(sinkIndex);
        if (output == null) {
            throw new IllegalArgumentException("unknown coordinated SINK node: " + sinkIndex);
        }
        return output;
    }

    void rollbackAfterFailure(Throwable primary) {
        for (int i = outputs.length - 1; i >= 0; i--) {
            try {
                outputs[i].rollbackCommit();
            } catch (Throwable rollbackFailure) {
                primary.addSuppressed(rollbackFailure);
            }
        }
    }

    void publishAndFinalize() {
        if (manifestGroup != null) {
            manifestGroup.publish();
            finalizeOutputs();
            return;
        }
        try {
            for (Sink output : outputs) {
                output.commitPrepared();
            }
        } catch (Throwable publishFailure) {
            rollbackAfterFailure(publishFailure);
            throwUnchecked(publishFailure);
            return;
        }

        finalizeOutputs();
    }

    private void finalizeOutputs() {
        Throwable finalizeFailure = null;
        for (Sink output : outputs) {
            try {
                output.finalizeCommit();
            } catch (Throwable failure) {
                if (finalizeFailure == null) {
                    finalizeFailure = failure;
                } else {
                    finalizeFailure.addSuppressed(failure);
                }
            }
        }
        if (finalizeFailure != null) {
            throw new IllegalStateException("outputs published but finalize failed", finalizeFailure);
        }
    }

    @Override
    public void close() {
        if (manifestGroup != null) {
            manifestGroup.close();
        }
    }

    /** The manifest group has exactly one visibility point, and once it is visible the general rollback path is never entered. */
    private final class ManifestGroup {
        private final ManifestFileSink[] members;
        private final ManifestFileStore store;
        private final String attemptId;
        private final long generation;
        private final FileCommitDomainLease lease;

        private ManifestGroup(Sink[] groupOutputs) {
            members = new ManifestFileSink[groupOutputs.length];
            for (int i = 0; i < groupOutputs.length; i++) {
                if (!(groupOutputs[i] instanceof ManifestFileSink)) {
                    throw new IllegalArgumentException(
                            "manifest commit group cannot contain non-manifest outputs");
                }
                members[i] = (ManifestFileSink) groupOutputs[i];
            }
            store = new ManifestFileStore(Paths.get(members[0].domainPath()));
            for (ManifestFileSink member : members) {
                if (!store.domainId().equals(member.domainId())) {
                    throw new IllegalStateException(
                            "manifest commit domain identity changed after plan creation: expected="
                                    + member.domainId() + " actual=" + store.domainId());
                }
            }
            FileCommitDomainLease acquired = FileCommitDomainLease.tryAcquire(store.domainPath());
            if (acquired == null) {
                throw new IllegalStateException(
                        "manifest commit domain already has an active writer: " + store.domainId());
            }
            String newAttemptId = UUID.randomUUID().toString();
            long newGeneration;
            try {
                newGeneration = new FileCommitGenerationAuthority()
                        .reserve(store.domainPath(), newAttemptId);
                for (ManifestFileSink member : members) {
                    member.bindManifestAttempt(newAttemptId, newGeneration);
                }
            } catch (Throwable failure) {
                try {
                    acquired.close();
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throwUnchecked(failure);
                throw new AssertionError("unreachable");
            }
            attemptId = newAttemptId;
            generation = newGeneration;
            lease = acquired;
        }

        private void publish() {
            CommitManifest intended = null;
            try {
                List<CommitManifest.Member> descriptions = new ArrayList<>(members.length);
                for (ManifestFileSink member : members) {
                    member.commitPrepared();
                    descriptions.add(member.installedMember());
                }
                intended = new CommitManifest(
                        store.domainId(), generation, attemptId, descriptions);
                LocalQualityGate.enforce(
                        OutputCommitCoordinator.this.graph,
                        members, attemptId, generation);
                store.publish(intended);
            } catch (Throwable failure) {
                CommitManifest observed;
                try {
                    observed = store.exact(generation);
                } catch (Throwable corruptOrConflict) {
                    failure.addSuppressed(corruptOrConflict);
                    throwUnchecked(failure);
                    return;
                }
                if (intended != null && intended.equals(observed)) {
                    throw new ManifestCommitUncertainException(
                            "manifest is visible but durability acknowledgement is uncertain: generation="
                                    + generation + " attemptId=" + attemptId,
                            failure);
                }
                if (observed != null) {
                    IllegalStateException conflict = new IllegalStateException(
                            "manifest generation conflict; preserving possibly referenced data: " + generation,
                            failure);
                    throw conflict;
                }
                rollbackAfterFailure(failure);
                throwUnchecked(failure);
            }
        }

        private void close() {
            lease.close();
        }
    }

    /** outputStage still follows the existing close and abort protocol; this view rewrites the terminal action as prepare or rollback. */
    private static Sink preparingView(Sink delegate) {
        return new Sink() {
            private static final long serialVersionUID = 1L;

            @Override
            public DataSchema getSchema() {
                return delegate.getSchema();
            }

            @Override
            public void collect(DataRow row) {
                delegate.collect(row);
            }

            @Override
            public void close() {
                delegate.prepareCommit();
            }

            @Override
            public void abort() {
                delegate.rollbackCommit();
            }

            @Override
            public boolean supportsCoordinatedCommit() {
                return delegate.supportsCoordinatedCommit();
            }

            @Override
            public void prepareCommit() {
                delegate.prepareCommit();
            }

            @Override
            public void commitPrepared() {
                delegate.commitPrepared();
            }

            @Override
            public void rollbackCommit() {
                delegate.rollbackCommit();
            }

            @Override
            public void finalizeCommit() {
                delegate.finalizeCommit();
            }

            @Override
            public boolean isWorkerWritable() {
                return delegate.isWorkerWritable();
            }

            @Override
            public String exclusiveDestinationId() {
                return delegate.exclusiveDestinationId();
            }
        };
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }
}
