package io.rowweave.deployment;

import io.rowweave.DurableBatchSinkPreparedGroup;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkWriteAttempt;
import io.rowweave.DurableSinkActionContext;
import io.rowweave.DurableSinkResourceLease;
import io.rowweave.ManifestFileSink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataRow;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitOutcome;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.SinkAttemptIdentity;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Exact-core Manifest adapter for the versioned durable provider lifecycle. */
public final class ManifestDurableBatchSinkProvider
        implements DurableBatchSinkProvider {

    @Override
    public DurableBatchSinkProviderContract contract() {
        return DurableBatchSinkBuiltinContracts.manifest();
    }

    @Override
    public byte[] canonicalizeConfiguration(ProviderBatchSink output) {
        return ManifestFileSink.fromProviderSink(output)
                .toProviderSink().configurationBytes();
    }

    @Override
    public byte[] canonicalizeLocator(PreparedSinkLocator locator) {
        requireManifestLocator(locator);
        return ManifestCommitWire.encodeMember(
                ManifestCommitWire.decodeMember(locator.payload()));
    }

    @Override
    public DurableBatchSinkWriteAttempt openWriteAttempt(
            ProviderBatchSink output, SinkAttemptIdentity attemptIdentity,
            DurableSinkActionContext context) {
        requireContext(context, attemptIdentity);
        ManifestFileSink manifest = ManifestFileSink.fromProviderSink(output);
        manifest.bindManifestAttempt(
                context.publicationId(), context.publicationGeneration());
        DurableSinkResourceLease resident = context.acquireResidentLease(
                Math.max(1, output.configurationSize()));
        DurableSinkResourceLease handle = context.acquireHandleLease();
        return new WriteAttempt(manifest, attemptIdentity, context, resident, handle);
    }

    @Override
    public DurableBatchSinkPreparedGroup openPreparedGroup(
            List<ProviderBatchSink> outputs,
            List<PreparedSinkLocator> preparedLocators,
            DurableSinkActionContext context) {
        if (outputs == null || preparedLocators == null || context == null
                || outputs.isEmpty() || outputs.size() != preparedLocators.size()
                || outputs.size() > 4095) {
            throw new IllegalArgumentException("PROVIDER_GROUP_INVALID");
        }
        List<CommitManifest.Member> members = new ArrayList<>(outputs.size());
        Set<String> memberNames = new HashSet<>();
        ManifestFileStore store = null;
        for (int i = 0; i < outputs.size(); i++) {
            ManifestFileSink manifest = ManifestFileSink.fromProviderSink(
                    outputs.get(i));
            PreparedSinkLocator locator = preparedLocators.get(i);
            requireManifestLocator(locator);
            if (!manifest.domainId().equals(locator.commitDomainId())
                    || !manifest.batchSinkDescriptor().equals(
                    outputs.get(i).batchSinkDescriptor())
                    || !locator.attemptIdentity().fence().equals(
                    context.attemptIdentity().fence())) {
                throw new IllegalArgumentException("PROVIDER_GROUP_INVALID");
            }
            CommitManifest.Member member = ManifestCommitWire.decodeMember(
                    locator.payload());
            if (!manifest.memberName().equals(member.name())
                    || !memberNames.add(member.name())) {
                throw new IllegalArgumentException("PROVIDER_GROUP_INVALID");
            }
            ManifestFileStore candidate = new ManifestFileStore(
                    java.nio.file.Paths.get(manifest.domainPath()));
            if (store == null) {
                store = candidate;
            } else if (!store.domainId().equals(candidate.domainId())) {
                throw new IllegalArgumentException("PROVIDER_GROUP_INVALID");
            }
            members.add(member);
        }
        return new PreparedGroup(store, context.publicationId(),
                context.publicationGeneration(), members,
                context.acquireHandleLease());
    }

    private static void requireContext(
            DurableSinkActionContext context, SinkAttemptIdentity identity) {
        if (context == null || identity == null
                || !identity.equals(context.attemptIdentity())
                || !context.providerKey().equals(
                DurableBatchSinkBuiltinContracts.manifest().providerKey())
                || context.publicationId() == null
                || context.publicationId().isEmpty()
                || context.publicationGeneration() <= 0L) {
            throw new IllegalArgumentException("PROVIDER_CONTEXT_INVALID");
        }
    }

    private static void requireManifestLocator(PreparedSinkLocator locator) {
        DurableBatchSinkProviderContract contract =
                DurableBatchSinkBuiltinContracts.manifest();
        if (locator == null
                || !contract.providerKey().equals(locator.providerKey())
                || !contract.locatorCodec().equals(locator.locatorCodec())
                || locator.payload().length
                > contract.attemptBudget().maxLocatorBytes()) {
            throw new IllegalArgumentException("PROVIDER_LOCATOR_INVALID");
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static final class WriteAttempt
            implements DurableBatchSinkWriteAttempt {
        private final ManifestFileSink output;
        private final SinkAttemptIdentity identity;
        private final DurableSinkActionContext context;
        private final DurableSinkResourceLease resident;
        private final DurableSinkResourceLease handle;
        private final AtomicBoolean closed = new AtomicBoolean();
        private boolean prepared;

        private WriteAttempt(
                ManifestFileSink output, SinkAttemptIdentity identity,
                DurableSinkActionContext context,
                DurableSinkResourceLease resident,
                DurableSinkResourceLease handle) {
            this.output = output;
            this.identity = identity;
            this.context = context;
            this.resident = resident;
            this.handle = handle;
        }

        @Override
        public void write(DataRow row) {
            if (closed.get() || prepared || context.isCancellationRequested()) {
                throw new IllegalStateException("PROVIDER_ATTEMPT_NOT_OPEN");
            }
            output.collect(row);
        }

        @Override
        public PreparedSinkLocator prepare() {
            if (closed.get()) {
                throw new IllegalStateException("PROVIDER_ATTEMPT_NOT_OPEN");
            }
            if (!prepared) {
                output.prepareCommit();
                output.commitPrepared();
                output.sealInstalledMember();
                prepared = true;
            }
            CommitManifest.Member member = output.installedMember();
            byte[] payload = ManifestCommitWire.encodeMember(member);
            DurableBatchSinkProviderContract contract =
                    DurableBatchSinkBuiltinContracts.manifest();
            return new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    contract.providerKey().capabilityProfile(),
                    identity.commitDomainId(), contract.providerKey(),
                    contract.locatorCodec(), payload, sha256(payload),
                    member.length(), member.sha256(),
                    System.currentTimeMillis());
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            try {
                if (!prepared) output.rollbackCommit();
            } finally {
                try {
                    handle.close();
                } finally {
                    resident.close();
                }
            }
        }
    }

    private static final class PreparedGroup
            implements DurableBatchSinkPreparedGroup {
        private final ManifestFileStore store;
        private final String publicationId;
        private final long generation;
        private final CommitManifest intended;
        private final DurableSinkResourceLease handle;
        private final AtomicBoolean closed = new AtomicBoolean();

        private PreparedGroup(
                ManifestFileStore store, String publicationId, long generation,
                List<CommitManifest.Member> members,
                DurableSinkResourceLease handle) {
            this.store = store;
            this.publicationId = publicationId;
            this.generation = generation;
            this.intended = new CommitManifest(
                    store.domainId(), generation, publicationId, members);
            this.handle = handle;
        }

        @Override
        public void publish() {
            requireOpen();
            store.publish(intended);
        }

        @Override
        public void abort() {
            requireOpen();
            CommitManifest visible = store.exact(generation);
            if (visible != null && !visible.equals(intended)) {
                throw new IllegalStateException("PROVIDER_OUTCOME_CONFLICT");
            }
        }

        @Override
        public void finalizeAttempt() {
            requireOpen();
        }

        @Override
        public BatchSinkOutcome queryOutcome() {
            requireOpen();
            ManifestCommitOutcome outcome = store.queryOutcome(
                    publicationId, generation);
            switch (outcome.state()) {
                case NOT_FOUND:
                    return BatchSinkOutcome.notFound();
                case PREPARING:
                    return BatchSinkOutcome.of(
                            BatchSinkOutcome.State.PREPARING,
                            outcome.generation(), null, null);
                case COMMITTED:
                    return BatchSinkOutcome.of(
                            BatchSinkOutcome.State.COMMITTED,
                            outcome.generation(), outcome.manifestHash(), null);
                case CORRUPT_OR_CONFLICT:
                default:
                    return BatchSinkOutcome.of(
                            BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,
                            outcome.generation(), null,
                            "PROVIDER_OUTCOME_CONFLICT");
            }
        }

        private void requireOpen() {
            if (closed.get()) {
                throw new IllegalStateException("PROVIDER_GROUP_CLOSED");
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) handle.close();
        }
    }
}
