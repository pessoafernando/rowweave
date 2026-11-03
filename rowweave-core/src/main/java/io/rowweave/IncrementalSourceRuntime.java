package io.rowweave;

import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalChangelogRowValidator;
import io.rowweave.incremental.IncrementalSourceBindingMaterializer;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.internal.ProviderActionSupervisor;

import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/** Worker-only bridge from an inert incremental split to a managed reader. */
public final class IncrementalSourceRuntime {
    private IncrementalSourceRuntime() {
    }

    public static BatchExecutableSource bind(
            BatchSourceSplitBinding binding,
            IncrementalSourceProviderContract frozenContract,
            IncrementalSourceConfig frozenConfiguration,
            CapturedTargetReceipt frozenTarget, byte[] partitionSetDigest,
            IncrementalSourceProvider provider,
            ProviderActionSupervisor supervisor, long deadlineNanos,
            BooleanSupplier cancellationRequested) {
        return bind(binding, frozenContract, frozenConfiguration,
                frozenTarget, partitionSetDigest, provider, supervisor,
                deadlineNanos, cancellationRequested, null);
    }

    /** Same exact binding with the per-split UPSERT validator attached. */
    public static BatchExecutableSource bind(
            BatchSourceSplitBinding binding,
            IncrementalSourceProviderContract frozenContract,
            IncrementalSourceConfig frozenConfiguration,
            CapturedTargetReceipt frozenTarget, byte[] partitionSetDigest,
            IncrementalSourceProvider provider,
            ProviderActionSupervisor supervisor, long deadlineNanos,
            BooleanSupplier cancellationRequested,
            IncrementalChangelogRowValidator rowValidator) {
        if (provider == null || supervisor == null
                || cancellationRequested == null) {
            throw new IllegalArgumentException(
                    "incremental source runtime fields are required");
        }
        if (deadlineNanos <= System.nanoTime()) {
            throw new IllegalArgumentException(
                    "incremental source runtime deadline has expired");
        }
        IncrementalSourceSplit proxy =
                IncrementalSourceBindingMaterializer.verifyPreOpen(
                        binding, frozenContract, frozenConfiguration,
                        frozenTarget, partitionSetDigest);
        String actionPrefix = "incremental-source-bind-"
                + binding.descriptor().splitId();
        IncrementalSourceProviderContract runtimeContract = supervisor.call(
                actionPrefix + "-contract", deadlineNanos,
                provider::contract);
        if (runtimeContract == null
                || !MessageDigest.isEqual(
                IncrementalSourceProviderContractCodec.digest(frozenContract),
                IncrementalSourceProviderContractCodec.digest(runtimeContract))) {
            throw new IllegalArgumentException(
                    "INCREMENTAL_SOURCE_CONTRACT_DRIFT");
        }
        byte[] canonicalConfiguration = supervisor.call(
                actionPrefix + "-configuration", deadlineNanos,
                () -> provider.canonicalizeConfiguration(
                        frozenConfiguration));
        if (!Arrays.equals(frozenConfiguration.configurationBytes(),
                canonicalConfiguration)) {
            throw new IllegalArgumentException(
                    "INCREMENTAL_SOURCE_CONFIG_NONCANONICAL");
        }
        return new RuntimeInput(proxy, provider, supervisor, deadlineNanos,
                cancellationRequested, rowValidator);
    }

    private static final class RuntimeInput
            implements BatchExecutableSource {
        private static final long serialVersionUID = 1L;

        private final IncrementalSourceSplit proxy;
        private final transient IncrementalSourceProvider provider;
        private final transient ProviderActionSupervisor supervisor;
        private final long deadlineNanos;
        private final transient BooleanSupplier cancellationRequested;
        private final transient IncrementalChangelogRowValidator rowValidator;

        private RuntimeInput(
                IncrementalSourceSplit proxy,
                IncrementalSourceProvider provider,
                ProviderActionSupervisor supervisor, long deadlineNanos,
                BooleanSupplier cancellationRequested,
                IncrementalChangelogRowValidator rowValidator) {
            this.proxy = proxy;
            this.provider = provider;
            this.supervisor = supervisor;
            this.deadlineNanos = deadlineNanos;
            this.cancellationRequested = cancellationRequested;
            this.rowValidator = rowValidator;
        }

        @Override public DataSchema getSchema() { return proxy.getSchema(); }

        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return proxy.batchSourceSplit();
        }

        @Override public SourceExecutor<?> createSourceExecutor() {
            if (provider == null || supervisor == null
                    || cancellationRequested == null) {
                throw new IllegalStateException(
                        "incremental runtime input lost its Worker binding");
            }
            return new RuntimeExecutor(this);
        }

        private Object writeReplace() throws ObjectStreamException {
            throw new NotSerializableException(
                    "incremental runtime input must not enter an Artifact");
        }
    }

    private static final class RuntimeExecutor
            extends SourceExecutor<RuntimeInput> {
        private final AtomicBoolean closed = new AtomicBoolean();
        private ProviderActionSupervisor.AttemptPermit permit;
        private SourceExecutor<IncrementalSourceSplit> delegate;
        private boolean opened;

        private RuntimeExecutor(RuntimeInput input) {
            super(input, input.getSchema());
        }

        @Override public void open() {
            if (opened && !closed.get()) return;
            if (closed.get()) throw failure("INCREMENTAL_SOURCE_CLOSED");
            requireLive();
            ProviderActionSupervisor.AttemptPermit acquired =
                    input.supervisor.openAttempt(
                            "incremental-source-open-"
                                    + input.proxy.batchSourceSplit().splitId(),
                            input.deadlineNanos);
            SourceExecutor<IncrementalSourceSplit> candidate = null;
            try {
                candidate = acquired.call("create-reader",
                        () -> input.provider.openSplit(input.proxy));
                if (candidate == null) {
                    throw new IllegalArgumentException(
                            "INCREMENTAL_SOURCE_READER_MISSING");
                }
                if (candidate.input != input.proxy) {
                    closeCandidate(acquired, candidate, null);
                    throw new IllegalArgumentException(
                            "INCREMENTAL_SOURCE_READER_BINDING_DRIFT");
                }
                SourceExecutor<IncrementalSourceSplit> opening = candidate;
                acquired.call("open-reader", () -> {
                    opening.open();
                    return null;
                });
                permit = acquired;
                delegate = candidate;
                opened = true;
            } catch (RuntimeException | Error failure) {
                if (candidate != null && candidate.input == input.proxy) {
                    closeCandidate(acquired, candidate, failure);
                }
                acquired.close();
                throw failure;
            }
        }

        @Override public boolean hasNext() {
            requireReadable();
            try {
                boolean more = delegate.hasNext();
                if (!more) close();
                return more;
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        @Override public DataRow next() {
            requireReadable();
            try {
                DataRow row = delegate.next();
                if (row == null) {
                    throw failure("INCREMENTAL_SOURCE_NULL_ROW");
                }
                return input.rowValidator == null
                        ? row : input.rowValidator.validate(row);
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        @Override public void close() {
            if (!closed.compareAndSet(false, true)) return;
            SourceExecutor<IncrementalSourceSplit> current = delegate;
            ProviderActionSupervisor.AttemptPermit currentPermit = permit;
            delegate = null;
            permit = null;
            try {
                if (current != null && currentPermit != null) {
                    currentPermit.call("close-reader", () -> {
                        current.close();
                        return null;
                    });
                }
            } finally {
                if (currentPermit != null) currentPermit.close();
            }
        }

        private void requireReadable() {
            if (!opened || delegate == null || closed.get()) {
                throw failure("INCREMENTAL_SOURCE_NOT_OPEN");
            }
            try {
                requireLive();
            } catch (RuntimeException | Error failure) {
                closeAfterFailure(failure);
                throw failure;
            }
        }

        private void requireLive() {
            if (input.cancellationRequested.getAsBoolean()) {
                throw failure("INCREMENTAL_SOURCE_CANCELED");
            }
            if (System.nanoTime() >= input.deadlineNanos) {
                throw failure("INCREMENTAL_SOURCE_DEADLINE_EXCEEDED");
            }
        }

        private void closeAfterFailure(Throwable primary) {
            try {
                close();
            } catch (Throwable closeFailure) {
                primary.addSuppressed(closeFailure);
            }
        }

        private static void closeCandidate(
                ProviderActionSupervisor.AttemptPermit permit,
                SourceExecutor<IncrementalSourceSplit> candidate,
                Throwable primary) {
            try {
                permit.call("close-rejected-reader", () -> {
                    candidate.close();
                    return null;
                });
            } catch (Throwable closeFailure) {
                if (primary != null) primary.addSuppressed(closeFailure);
                else if (closeFailure instanceof RuntimeException) {
                    throw (RuntimeException) closeFailure;
                } else if (closeFailure instanceof Error) {
                    throw (Error) closeFailure;
                } else {
                    throw new IllegalStateException(closeFailure);
                }
            }
        }

        private static IllegalStateException failure(String code) {
            return new IllegalStateException(code);
        }
    }
}
