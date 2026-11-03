package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.DurableBatchSinkPreparedGroup;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableSinkActionContext;
import io.rowweave.DurableSinkResourceLease;
import io.rowweave.ProviderBatchSink;
import io.rowweave.commit.BatchSinkOutcome;
import io.rowweave.deployment.DurableBatchSinkProviderRegistry;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.SinkAttemptIdentity;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

/** Worker-only bounded execution boundary for rebuildable prepared-group actions. */
final class ProviderPreparedGroupExecutor {
    private ProviderPreparedGroupExecutor() { }

    static BatchSinkOutcome execute(
            ProviderPreparedGroupCommandCodec.Command command,
            int encodedRequestBytes,
            String publicationId, long publicationGeneration,
            DurableBatchSinkProviderRegistry registry,
            ProviderActionSupervisor supervisor,
            LongConsumer residentReservation,
            BooleanSupplier cancellationRequested) {
        return execute(command, encodedRequestBytes, publicationId,
                publicationGeneration, registry, supervisor,
                residentReservation, cancellationRequested, ignored -> { });
    }

    static BatchSinkOutcome execute(
            ProviderPreparedGroupCommandCodec.Command command,
            int encodedRequestBytes,
            String publicationId, long publicationGeneration,
            DurableBatchSinkProviderRegistry registry,
            ProviderActionSupervisor supervisor,
            LongConsumer residentReservation,
            BooleanSupplier cancellationRequested,
            java.util.function.Consumer<String> beforeProviderAction) {
        if (command == null || encodedRequestBytes <= 0
                || publicationId == null || publicationId.isEmpty()
                || publicationGeneration <= 0L || registry == null
                || supervisor == null || residentReservation == null
                || cancellationRequested == null
                || beforeProviderAction == null) {
            throw new IllegalArgumentException("provider group execution fields are required");
        }
        ProviderBatchSink first = command.outputs.get(0);
        PreparedSinkLocator firstLocator = command.locators.get(0);
        DurableBatchSinkProviderContract contract = registry.requireContract(
                firstLocator.providerKey());
        if (encodedRequestBytes > contract.attemptBudget().maxControlBytes()) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONTROL_REQUEST_BUDGET_EXCEEDED");
        }
        if (!contract.providerKey().matches(first.batchSinkDescriptor())
                || !contract.configurationCodec().equals(first.configurationCodec())
                || !contract.locatorCodec().equals(firstLocator.locatorCodec())) {
            throw new IllegalArgumentException("PROVIDER_CONTRACT_DRIFT");
        }
        SinkAttemptIdentity groupIdentity = firstLocator.attemptIdentity();
        for (int i = 0; i < command.outputs.size(); i++) {
            ProviderBatchSink output = command.outputs.get(i);
            PreparedSinkLocator locator = command.locators.get(i);
            if (!contract.providerKey().matches(output.batchSinkDescriptor())
                    || !contract.configurationCodec().equals(output.configurationCodec())
                    || !contract.providerKey().equals(locator.providerKey())
                    || !contract.locatorCodec().equals(locator.locatorCodec())
                    || !output.batchSinkDescriptor().commitDomainId().equals(
                    first.batchSinkDescriptor().commitDomainId())
                    || !locator.commitDomainId().equals(
                    first.batchSinkDescriptor().commitDomainId())
                    || !locator.attemptIdentity().fence().equals(
                    groupIdentity.fence())
                    || !MessageDigest.isEqual(
                    locator.attemptIdentity().descriptorDigest(),
                    BatchSinkDescriptorCodec.digest(output.batchSinkDescriptor()))) {
                throw new IllegalArgumentException("PROVIDER_GROUP_INVALID");
            }
        }
        long deadline = deadlineAfterMillis(
                contract.attemptBudget().actionTimeoutMillis());
        DurableBatchSinkProvider provider = registry.loadProvider(
                contract.providerKey(), supervisor, deadline);
        String actionId = "provider-group-" + publicationId + '-'
                + command.action.name().toLowerCase(java.util.Locale.ROOT);
        ActionContext context = new ActionContext(
                actionId, contract, groupIdentity, publicationId,
                publicationGeneration, command.authorityDecisionDigest,
                deadline, residentReservation, cancellationRequested);
        ProviderActionSupervisor.AttemptPermit permit =
                supervisor.openAttempt(actionId, deadline);
        DurableBatchSinkPreparedGroup group = null;
        try {
            for (int i = 0; i < command.outputs.size(); i++) {
                ProviderBatchSink output = command.outputs.get(i);
                PreparedSinkLocator locator = command.locators.get(i);
                byte[] canonicalConfig = permit.call("canonicalize-config-" + i,
                        () -> provider.canonicalizeConfiguration(output));
                byte[] canonicalLocator = permit.call("canonicalize-locator-" + i,
                        () -> provider.canonicalizeLocator(locator));
                if (!Arrays.equals(output.configurationBytes(), canonicalConfig)
                        || !Arrays.equals(locator.payload(), canonicalLocator)) {
                    throw new IllegalArgumentException("PROVIDER_GROUP_NONCANONICAL");
                }
            }
            group = permit.call("open-prepared-group",
                    () -> provider.openPreparedGroup(
                            command.outputs, command.locators, context));
            if (group == null) {
                throw new IllegalStateException("provider returned no prepared group");
            }
            final DurableBatchSinkPreparedGroup opened = group;
            switch (command.action) {
                case PUBLISH:
                    permit.call("publish", () -> {
                        beforeProviderAction.accept("publish");
                        opened.publish();
                        return null;
                    });
                    return permit.call("query-after-publish", opened::queryOutcome);
                case ABORT:
                    permit.call("abort", () -> {
                        beforeProviderAction.accept("abort");
                        opened.abort();
                        return null;
                    });
                    return BatchSinkOutcome.of(BatchSinkOutcome.State.ABORTED,
                            publicationGeneration,
                            command.authorityDecisionDigest.length == 0 ? null
                                    : command.authorityDecisionDigest,
                            "PROVIDER_ABORTED");
                case FINALIZE:
                    permit.call("finalize", () -> {
                        beforeProviderAction.accept("finalize");
                        opened.finalizeAttempt();
                        return null;
                    });
                    return permit.call("query-after-finalize", opened::queryOutcome);
                case QUERY:
                    return permit.call("query", () -> {
                        beforeProviderAction.accept("query");
                        return opened.queryOutcome();
                    });
                default:
                    throw new AssertionError(command.action);
            }
        } finally {
            if (group != null) {
                DurableBatchSinkPreparedGroup opened = group;
                try {
                    permit.call("close", () -> { opened.close(); return null; });
                } finally {
                    context.closeAll();
                    permit.close();
                }
            } else {
                context.closeAll();
                permit.close();
            }
        }
    }

    private static long deadlineAfterMillis(long timeoutMillis) {
        long now = System.nanoTime();
        long delta = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        return Long.MAX_VALUE - now < delta ? Long.MAX_VALUE : now + delta;
    }

    private static final class ActionContext implements DurableSinkActionContext {
        private final String actionId;
        private final DurableBatchSinkProviderContract contract;
        private final SinkAttemptIdentity identity;
        private final String publicationId;
        private final long generation;
        private final byte[] decisionDigest;
        private final long deadline;
        private final LongConsumer residentReservation;
        private final BooleanSupplier cancellationRequested;
        private final AtomicLong resident = new AtomicLong();
        private final AtomicInteger handles = new AtomicInteger();
        private final Set<Lease> leases = ConcurrentHashMap.newKeySet();

        private ActionContext(
                String actionId, DurableBatchSinkProviderContract contract,
                SinkAttemptIdentity identity, String publicationId, long generation,
                byte[] decisionDigest, long deadline,
                LongConsumer residentReservation,
                BooleanSupplier cancellationRequested) {
            this.actionId = actionId;
            this.contract = contract;
            this.identity = identity;
            this.publicationId = publicationId;
            this.generation = generation;
            this.decisionDigest = decisionDigest.clone();
            this.deadline = deadline;
            this.residentReservation = residentReservation;
            this.cancellationRequested = cancellationRequested;
        }

        @Override public String actionId() { return actionId; }
        @Override public io.rowweave.DurableSinkProviderKey providerKey() {
            return contract.providerKey();
        }
        @Override public SinkAttemptIdentity attemptIdentity() { return identity; }
        @Override public String publicationId() { return publicationId; }
        @Override public long publicationGeneration() { return generation; }
        @Override public byte[] authorityDecisionDigest() { return decisionDigest.clone(); }
        @Override public long deadlineNanos() { return deadline; }
        @Override public boolean isCancellationRequested() {
            return cancellationRequested.getAsBoolean();
        }
        @Override public long maxResidentBytes() {
            return contract.attemptBudget().maxResidentBytes();
        }
        @Override public int maxOpenHandles() {
            return contract.attemptBudget().maxOpenHandles();
        }
        @Override public int maxRequestBytes() {
            return contract.attemptBudget().maxControlBytes();
        }
        @Override public int maxResponseBytes() {
            return contract.attemptBudget().maxControlBytes();
        }
        @Override public DurableSinkResourceLease acquireResidentLease(long bytes) {
            if (bytes <= 0L) throw new IllegalArgumentException("lease bytes must be positive");
            long total = resident.addAndGet(bytes);
            if (total > maxResidentBytes()) {
                resident.addAndGet(-bytes);
                throw new IllegalStateException("PROVIDER_RESIDENT_BUDGET_EXCEEDED");
            }
            residentReservation.accept(bytes);
            Lease lease = new Lease(bytes, 0, resident, handles, leases);
            leases.add(lease);
            return lease;
        }
        @Override public DurableSinkResourceLease acquireHandleLease() {
            int total = handles.incrementAndGet();
            if (total > maxOpenHandles()) {
                handles.decrementAndGet();
                throw new IllegalStateException("PROVIDER_HANDLE_BUDGET_EXCEEDED");
            }
            Lease lease = new Lease(0L, 1, resident, handles, leases);
            leases.add(lease);
            return lease;
        }
        void closeAll() {
            for (Lease lease : leases.toArray(new Lease[0])) lease.close();
        }
    }

    private static final class Lease implements DurableSinkResourceLease {
        private final long bytes;
        private final int handles;
        private final AtomicLong resident;
        private final AtomicInteger handleCount;
        private final Set<Lease> owner;
        private final AtomicBoolean open = new AtomicBoolean(true);

        private Lease(long bytes, int handles, AtomicLong resident,
                      AtomicInteger handleCount, Set<Lease> owner) {
            this.bytes = bytes;
            this.handles = handles;
            this.resident = resident;
            this.handleCount = handleCount;
            this.owner = owner;
        }
        @Override public long residentBytes() { return bytes; }
        @Override public int openHandles() { return handles; }
        @Override public boolean isOpen() { return open.get(); }
        @Override public void close() {
            if (!open.compareAndSet(true, false)) return;
            resident.addAndGet(-bytes);
            handleCount.addAndGet(-handles);
            owner.remove(this);
        }
    }
}
