package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSink;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkWriteAttempt;
import io.rowweave.DurableSinkActionContext;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkResourceLease;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.SinkAttemptIdentity;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

/** Worker-only synchronous Push adapter for one fenced provider write attempt. */
final class ProviderSinkWriteSession implements BatchSink {
    private static final long serialVersionUID = 1L;

    private enum State { OPEN, PREPARED, CLOSED, FAILED }

    private final ProviderBatchSink proxy;
    private final DurableBatchSinkProvider provider;
    private final DurableBatchSinkProviderContract contract;
    private final SinkAttemptIdentity identity;
    private final ProviderActionSupervisor supervisor;
    private final String actionId;
    private final long deadlineNanos;
    private final ActionContext context;
    private final java.util.function.Consumer<String> beforeProviderAction;
    private final ReentrantLock actionLock = new ReentrantLock();
    private volatile ProviderActionSupervisor.AttemptPermit permit;
    private volatile DurableBatchSinkWriteAttempt attempt;
    private volatile PreparedSinkLocator preparedLocator;
    private volatile State state = State.OPEN;

    private ProviderSinkWriteSession(
            ProviderBatchSink proxy, DurableBatchSinkProvider provider,
            DurableBatchSinkProviderContract contract,
            SinkAttemptIdentity identity,
            ProviderActionSupervisor supervisor, String actionId,
            long deadlineNanos, ActionContext context,
            java.util.function.Consumer<String> beforeProviderAction) {
        this.proxy = proxy;
        this.provider = provider;
        this.contract = contract;
        this.identity = identity;
        this.supervisor = supervisor;
        this.actionId = actionId;
        this.deadlineNanos = deadlineNanos;
        this.context = context;
        this.beforeProviderAction = beforeProviderAction;
    }

    static ProviderSinkWriteSession open(
            DurableBatchSinkProvider provider,
            DurableBatchSinkProviderContract contract,
            ProviderBatchSink proxy,
            SinkAttemptIdentity identity,
            String publicationId, long publicationGeneration,
            ProviderActionSupervisor supervisor,
            LongConsumer residentReservation,
            BooleanSupplier cancellationRequested) {
        return admit(provider, contract, proxy, identity, supervisor)
                .open(publicationId, publicationGeneration,
                        residentReservation, cancellationRequested);
    }

    static Admission admit(
            DurableBatchSinkProvider provider,
            DurableBatchSinkProviderContract contract,
            ProviderBatchSink proxy,
            SinkAttemptIdentity identity,
            ProviderActionSupervisor supervisor) {
        return admit(provider, contract, proxy, identity, supervisor,
                ignored -> { });
    }

    static Admission admit(
            DurableBatchSinkProvider provider,
            DurableBatchSinkProviderContract contract,
            ProviderBatchSink proxy,
            SinkAttemptIdentity identity,
            ProviderActionSupervisor supervisor,
            java.util.function.Consumer<String> beforeProviderAction) {
        if (provider == null || contract == null || proxy == null
                || identity == null || supervisor == null
                || beforeProviderAction == null
                ) {
            throw new IllegalArgumentException(
                    "provider Sink admission fields are required");
        }
        if (!contract.providerKey().matches(proxy.batchSinkDescriptor())
                || !contract.configurationCodec().equals(
                proxy.configurationCodec())
                || !MessageDigest.isEqual(identity.descriptorDigest(),
                BatchSinkDescriptorCodec.digest(proxy.batchSinkDescriptor()))) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONTRACT_DRIFT: provider Sink binding mismatch");
        }
        long deadline = deadlineAfterMillis(
                contract.attemptBudget().actionTimeoutMillis());
        byte[] canonical = supervisor.call(
                "provider-config-" + identity.stageExecutionId(), deadline,
                () -> provider.canonicalizeConfiguration(proxy));
        if (!Arrays.equals(proxy.configurationBytes(), canonical)) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        return new Admission(provider, contract, proxy, identity, supervisor,
                beforeProviderAction);
    }

    private static ProviderSinkWriteSession openAdmitted(
            Admission admission, String publicationId,
            long publicationGeneration, LongConsumer residentReservation,
            BooleanSupplier cancellationRequested) {
        if (publicationId == null || publicationId.isEmpty()
                || publicationGeneration <= 0L || residentReservation == null
                || cancellationRequested == null) {
            throw new IllegalArgumentException(
                    "provider Sink write session fields are required");
        }
        DurableBatchSinkProvider provider = admission.provider;
        DurableBatchSinkProviderContract contract = admission.contract;
        ProviderBatchSink proxy = admission.proxy;
        SinkAttemptIdentity identity = admission.identity;
        ProviderActionSupervisor supervisor = admission.supervisor;
        long deadline = deadlineAfterMillis(
                contract.attemptBudget().actionTimeoutMillis());
        String actionId = "sink-attempt-" + identity.stageExecutionId();
        ActionContext context = new ActionContext(
                actionId, contract, identity, publicationId,
                publicationGeneration, deadline, residentReservation,
                cancellationRequested);
        // Do not consume the process-wide attempt permit on the assignment RPC.
        // A multi-Sink graph is dispatched before its stages can drain upstream;
        // eagerly holding one permit per accepted assignment can therefore prevent
        // the final member from being accepted and deadlock the whole group.
        return new ProviderSinkWriteSession(
                proxy, provider, contract, identity, supervisor,
                actionId, deadline, context, admission.beforeProviderAction);
    }

    static final class Admission {
        private final DurableBatchSinkProvider provider;
        private final DurableBatchSinkProviderContract contract;
        private final ProviderBatchSink proxy;
        private final SinkAttemptIdentity identity;
        private final ProviderActionSupervisor supervisor;
        private final java.util.function.Consumer<String> beforeProviderAction;

        private Admission(
                DurableBatchSinkProvider provider,
                DurableBatchSinkProviderContract contract,
                ProviderBatchSink proxy, SinkAttemptIdentity identity,
                ProviderActionSupervisor supervisor,
                java.util.function.Consumer<String> beforeProviderAction) {
            this.provider = provider;
            this.contract = contract;
            this.proxy = proxy;
            this.identity = identity;
            this.supervisor = supervisor;
            this.beforeProviderAction = beforeProviderAction;
        }

        ProviderSinkWriteSession open(
                String publicationId, long publicationGeneration,
                LongConsumer residentReservation,
                BooleanSupplier cancellationRequested) {
            return ProviderSinkWriteSession.openAdmitted(
                    this, publicationId, publicationGeneration,
                    residentReservation, cancellationRequested);
        }

        ProviderBatchSink proxy() { return proxy; }

        boolean matchesProxy(ProviderBatchSink candidate) {
            return candidate != null
                    && proxy.batchSinkDescriptor().equals(
                    candidate.batchSinkDescriptor())
                    && proxy.configurationCodec().equals(
                    candidate.configurationCodec())
                    && Arrays.equals(proxy.configurationBytes(),
                    candidate.configurationBytes());
        }
    }

    @Override public DataSchema getSchema() { return proxy.getSchema(); }

    @Override public BatchSinkDescriptor batchSinkDescriptor() {
        return proxy.batchSinkDescriptor();
    }

    @Override public void collect(DataRow row) {
        if (row == null) throw new IllegalArgumentException("row is required");
        withActionLock("write", () -> {
            requireOpen();
            ensureAttemptOpen();
            attemptPermit().call("write", () -> {
                beforeProviderAction.accept("write");
                attempt.write(row);
                return null;
            });
            return null;
        });
    }

    @Override public void close() { prepareCommit(); }

    @Override public void prepareCommit() {
        withActionLock("prepare", () -> {
            if (state == State.PREPARED) return null;
            requireOpen();
            try {
                ensureAttemptOpen();
                PreparedSinkLocator candidate = attemptPermit().call(
                        "prepare", () -> {
                            beforeProviderAction.accept("prepare");
                            return attempt.prepare();
                        });
                validateLocator(candidate);
                byte[] canonical = attemptPermit().call("canonicalize-locator",
                        () -> provider.canonicalizeLocator(candidate));
                if (!Arrays.equals(candidate.payload(), canonical)) {
                    throw new IllegalArgumentException(
                            "PROVIDER_LOCATOR_NONCANONICAL");
                }
                closeAttempt("close-after-prepare");
                preparedLocator = candidate;
                state = State.PREPARED;
                return null;
            } catch (RuntimeException | Error failure) {
                state = State.FAILED;
                try {
                    closeAttempt("close-after-prepare-failure");
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            } finally {
                if (state != State.OPEN) {
                    context.closeAll();
                    closePermit();
                }
            }
        });
    }

    @Override public void abort() { rollbackCommit(); }

    @Override public void rollbackCommit() {
        withActionLock("rollback", () -> {
            if (state == State.CLOSED) return null;
            try {
                closeAttempt("close-on-rollback");
                state = State.CLOSED;
                return null;
            } catch (RuntimeException | Error failure) {
                state = State.FAILED;
                throw failure;
            } finally {
                context.closeAll();
                closePermit();
            }
        });
    }

    @Override public boolean supportsCoordinatedCommit() { return true; }

    @Override public void commitPrepared() {
        throw new UnsupportedOperationException(
                "provider control requires durable coordinator");
    }

    @Override public void finalizeCommit() { commitPrepared(); }

    @Override public boolean isWorkerWritable() { return true; }

    @Override public String exclusiveDestinationId() {
        return proxy.exclusiveDestinationId();
    }

    PreparedSinkLocator preparedLocator() {
        PreparedSinkLocator current = preparedLocator;
        if (current == null || state != State.PREPARED) {
            throw new IllegalStateException(
                    "provider Sink attempt is not prepared");
        }
        return current;
    }

    boolean matchesProxy(ProviderBatchSink candidate) {
        return candidate != null
                && proxy.batchSinkDescriptor().equals(
                candidate.batchSinkDescriptor())
                && proxy.configurationCodec().equals(
                candidate.configurationCodec())
                && Arrays.equals(proxy.configurationBytes(),
                candidate.configurationBytes());
    }

    private void validateLocator(PreparedSinkLocator locator) {
        if (locator == null || locator.locatorVersion()
                != PreparedSinkLocator.CURRENT_VERSION
                || !identity.equals(locator.attemptIdentity())
                || !contract.providerKey().equals(locator.providerKey())
                || !contract.locatorCodec().equals(locator.locatorCodec())
                || locator.payload().length
                > contract.attemptBudget().maxLocatorBytes()) {
            throw new IllegalArgumentException(
                    "PROVIDER_LOCATOR_INVALID");
        }
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException(
                    "provider Sink write attempt is not open");
        }
        if (context.isCancellationRequested()) {
            throw new IllegalStateException(
                    "provider Sink write attempt is cancelled");
        }
    }

    private void closeAttempt(String action) {
        DurableBatchSinkWriteAttempt current = attempt;
        if (current == null) return;
        attempt = null;
        attemptPermit().call(action, () -> {
            current.close();
            return null;
        });
    }

    private void ensureAttemptOpen() {
        if (attempt != null) return;
        ProviderActionSupervisor.AttemptPermit acquired =
                supervisor.openAttempt(actionId, deadlineNanos);
        DurableBatchSinkWriteAttempt opened = null;
        try {
            opened = acquired.call("open-write",
                    () -> {
                        beforeProviderAction.accept("open");
                        return provider.openWriteAttempt(proxy, identity, context);
                    });
            if (opened == null) {
                throw new IllegalStateException(
                        "provider returned no write attempt");
            }
            permit = acquired;
            attempt = opened;
        } catch (RuntimeException | Error failure) {
            if (opened != null) {
                try {
                    DurableBatchSinkWriteAttempt failedAttempt = opened;
                    acquired.call("close-after-open-failure", () -> {
                        failedAttempt.close();
                        return null;
                    });
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            context.closeAll();
            acquired.close();
            throw failure;
        }
    }

    private ProviderActionSupervisor.AttemptPermit attemptPermit() {
        ProviderActionSupervisor.AttemptPermit current = permit;
        if (current == null) {
            throw new IllegalStateException(
                    "provider Sink write attempt has no permit");
        }
        return current;
    }

    private void closePermit() {
        ProviderActionSupervisor.AttemptPermit current = permit;
        permit = null;
        if (current != null) current.close();
    }

    private <T> T withActionLock(String action, Callable<T> body) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0L) {
            throw new IllegalStateException(
                    "PROVIDER_ACTION_TIMEOUT: " + action);
        }
        boolean locked;
        try {
            locked = actionLock.tryLock(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "PROVIDER_ACTION_INTERRUPTED", interrupted);
        }
        if (!locked) {
            throw new IllegalStateException(
                    "PROVIDER_ACTION_TIMEOUT: " + action);
        }
        try {
            return body.call();
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception impossible) {
            throw new IllegalStateException("provider action failed", impossible);
        } finally {
            actionLock.unlock();
        }
    }

    private static long deadlineAfterMillis(long timeoutMillis) {
        long now = System.nanoTime();
        long delta = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        return Long.MAX_VALUE - now < delta ? Long.MAX_VALUE : now + delta;
    }

    private static final class ActionContext implements DurableSinkActionContext {
        private final String actionId;
        private final DurableBatchSinkProviderContract contract;
        private final SinkAttemptIdentity identity;
        private final String publicationId;
        private final long publicationGeneration;
        private final long deadlineNanos;
        private final LongConsumer residentReservation;
        private final BooleanSupplier cancellationRequested;
        private final AtomicLong residentBytes = new AtomicLong();
        private final AtomicInteger openHandles = new AtomicInteger();
        private final Set<ResourceLease> leases = ConcurrentHashMap.newKeySet();

        ActionContext(
                String actionId, DurableBatchSinkProviderContract contract,
                SinkAttemptIdentity identity, String publicationId,
                long publicationGeneration, long deadlineNanos,
                LongConsumer residentReservation,
                BooleanSupplier cancellationRequested) {
            this.actionId = actionId;
            this.contract = contract;
            this.identity = identity;
            this.publicationId = publicationId;
            this.publicationGeneration = publicationGeneration;
            this.deadlineNanos = deadlineNanos;
            this.residentReservation = residentReservation;
            this.cancellationRequested = cancellationRequested;
        }

        @Override public String actionId() { return actionId; }
        @Override public io.rowweave.DurableSinkProviderKey providerKey() {
            return contract.providerKey();
        }
        @Override public SinkAttemptIdentity attemptIdentity() { return identity; }
        @Override public String publicationId() { return publicationId; }
        @Override public long publicationGeneration() {
            return publicationGeneration;
        }
        @Override public byte[] authorityDecisionDigest() { return new byte[0]; }
        @Override public long deadlineNanos() { return deadlineNanos; }
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
            if (bytes <= 0L) {
                throw new IllegalArgumentException(
                        "provider resident lease must be positive");
            }
            reserve(residentBytes, bytes, maxResidentBytes(),
                    "provider resident budget exceeded");
            try {
                residentReservation.accept(bytes);
            } catch (RuntimeException | Error failure) {
                residentBytes.addAndGet(-bytes);
                throw failure;
            }
            return register(new ResourceLease(this, bytes, 0));
        }

        @Override public DurableSinkResourceLease acquireHandleLease() {
            int value = openHandles.incrementAndGet();
            if (value > maxOpenHandles()) {
                openHandles.decrementAndGet();
                throw new IllegalStateException(
                        "provider open handle budget exceeded");
            }
            return register(new ResourceLease(this, 0L, 1));
        }

        private ResourceLease register(ResourceLease lease) {
            leases.add(lease);
            return lease;
        }

        private static void reserve(
                AtomicLong counter, long delta, long maximum, String error) {
            for (;;) {
                long current = counter.get();
                if (current > maximum - delta) {
                    throw new IllegalStateException(error);
                }
                if (counter.compareAndSet(current, current + delta)) return;
            }
        }

        void closeAll() {
            for (ResourceLease lease : leases) lease.close();
        }

        void release(ResourceLease lease) {
            leases.remove(lease);
            if (lease.residentBytes != 0L) {
                residentBytes.addAndGet(-lease.residentBytes);
            }
            if (lease.openHandles != 0) {
                openHandles.addAndGet(-lease.openHandles);
            }
        }
    }

    private static final class ResourceLease implements DurableSinkResourceLease {
        private final ActionContext owner;
        private final long residentBytes;
        private final int openHandles;
        private final AtomicBoolean open = new AtomicBoolean(true);

        ResourceLease(ActionContext owner, long residentBytes, int openHandles) {
            this.owner = owner;
            this.residentBytes = residentBytes;
            this.openHandles = openHandles;
        }

        @Override public long residentBytes() { return residentBytes; }
        @Override public int openHandles() { return openHandles; }
        @Override public boolean isOpen() { return open.get(); }
        @Override public void close() {
            if (open.compareAndSet(true, false)) owner.release(this);
        }
    }
}
