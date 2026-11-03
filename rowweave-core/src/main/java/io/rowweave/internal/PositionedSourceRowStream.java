package io.rowweave.internal;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact bounded-source reader that attaches a split-local position to each decoded row. */
public final class PositionedSourceRowStream implements RowStream.CloseableRowStream {

    private final BatchSourceSplitBinding[] bindings;
    private final SourceRecordPosition.SplitFactory[] positionFactories;
    private int nextBinding;
    private volatile SourceExecutor<?> current;
    private volatile SourceExecutor<?> opening;
    private SourceRecordPosition.SplitFactory currentPositionFactory;
    private long nextOrdinal;
    private boolean ready;
    private volatile boolean closed;

    public PositionedSourceRowStream(
            String sourceOperatorUid, List<BatchSourceSplitBinding> sourceBindings) {
        if (sourceBindings == null || sourceBindings.isEmpty()) {
            throw invalid("source bindings must not be empty", null);
        }
        this.bindings = new BatchSourceSplitBinding[sourceBindings.size()];
        this.positionFactories =
                new SourceRecordPosition.SplitFactory[sourceBindings.size()];
        Set<String> splitIds = new HashSet<>();
        for (int i = 0; i < sourceBindings.size(); i++) {
            BatchSourceSplitBinding binding = sourceBindings.get(i);
            if (binding == null) {
                throw invalid("source binding is required at position " + i, null);
            }
            BatchSourceSplitDescriptor descriptor = binding.descriptor();
            if (descriptor.replayGuarantee()
                    != BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START) {
                throw invalid("source binding must provide exact ordered replay at position "
                        + i, null);
            }
            if (!(binding.input() instanceof BatchSource)) {
                throw invalid("exact source binding is not a BatchSource at position "
                        + i, null);
            }
            if (!splitIds.add(descriptor.splitId())) {
                throw invalid("duplicate source split at position " + i, null);
            }
            BatchSourceSplitDescriptor currentDescriptor =
                    ((BatchSource) binding.input()).batchSourceSplit();
            if (!descriptor.equals(currentDescriptor)) {
                throw invalid("source split descriptor changed at position " + i, null);
            }
            bindings[i] = binding;
            positionFactories[i] =
                    SourceRecordPosition.forSplit(sourceOperatorUid, descriptor);
        }
    }

    @Override
    public StreamEvent next() {
        if (closed) return null;
        try {
            if (!ensureAvailable()) {
                close();
                return null;
            }
            final SourceExecutor<?> active;
            final SourceRecordPosition.SplitFactory positionFactory;
            final long ordinal;
            synchronized (this) {
                if (closed || current == null || currentPositionFactory == null) return null;
                if (nextOrdinal == Long.MAX_VALUE) {
                    throw new IllegalStateException(
                            "SOURCE_POSITION_INVALID: split-local ordinal overflow");
                }
                ready = false;
                active = current;
                positionFactory = currentPositionFactory;
                ordinal = nextOrdinal++;
            }
            DataRow row = active.next();
            SourceRecordPosition position = positionFactory.at(ordinal);
            return StreamEvent.record(row, position);
        } catch (RuntimeException | Error primary) {
            return fail(primary);
        }
    }

    private boolean ensureAvailable() {
        if (ready) return true;
        while (true) {
            if (closed) return false;
            SourceExecutor<?> active = current;
            if (active != null) {
                final boolean available;
                try {
                    available = active.hasNext();
                } catch (RuntimeException | Error primary) {
                    return fail(primary);
                }
                if (available) {
                    synchronized (this) {
                        if (closed || current != active) return false;
                        ready = true;
                        return true;
                    }
                }
                closeExhaustedCurrent(active);
            }
            if (closed) return false;
            if (nextBinding >= bindings.length) return false;
            int bindingIndex = nextBinding++;
            SourceExecutor<?> candidate = null;
            try {
                candidate = SourceExecutorFactory.create(bindings[bindingIndex].input());
                synchronized (this) {
                    if (closed) {
                        candidate.close();
                        return false;
                    }
                    opening = candidate;
                }
                candidate.open();
            } catch (RuntimeException | Error primary) {
                if (candidate != null) {
                    synchronized (this) {
                        if (opening == candidate) opening = null;
                    }
                    try {
                        candidate.close();
                    } catch (RuntimeException | Error closeFailure) {
                        if (closeFailure != primary) primary.addSuppressed(closeFailure);
                    }
                }
                return fail(primary);
            }
            boolean publish;
            synchronized (this) {
                publish = !closed && opening == candidate;
                if (opening == candidate) opening = null;
                if (publish) {
                    current = candidate;
                    currentPositionFactory = positionFactories[bindingIndex];
                    nextOrdinal = 0L;
                }
            }
            if (!publish) {
                candidate.close();
                return false;
            }
        }
    }

    @Override
    public void close() {
        SourceExecutor<?> active;
        SourceExecutor<?> pending;
        synchronized (this) {
            if (closed) return;
            closed = true;
            ready = false;
            active = current;
            pending = opening;
            current = null;
            opening = null;
            currentPositionFactory = null;
        }
        closeExecutors(active, pending);
    }

    private void closeExhaustedCurrent(SourceExecutor<?> exhausted) {
        synchronized (this) {
            if (current != exhausted) return;
            current = null;
            currentPositionFactory = null;
            ready = false;
        }
        try {
            exhausted.close();
        } catch (RuntimeException | Error closeFailure) {
            closed = true;
            throw closeFailure;
        }
    }

    private <T> T fail(Throwable primary) {
        SourceExecutor<?> active;
        SourceExecutor<?> pending;
        synchronized (this) {
            closed = true;
            ready = false;
            active = current;
            pending = opening;
            current = null;
            opening = null;
            currentPositionFactory = null;
        }
        closeUnderPrimary(active, primary);
        if (pending != active) closeUnderPrimary(pending, primary);
        if (primary instanceof RuntimeException) throw (RuntimeException) primary;
        if (primary instanceof Error) throw (Error) primary;
        throw new IllegalStateException("unexpected checked source failure", primary);
    }

    private static void closeExecutors(SourceExecutor<?> first, SourceExecutor<?> second) {
        Throwable failure = null;
        SourceExecutor<?>[] executors = first == second
                ? new SourceExecutor<?>[]{first} : new SourceExecutor<?>[]{first, second};
        for (SourceExecutor<?> executor : executors) {
            if (executor == null) continue;
            try {
                executor.close();
            } catch (RuntimeException | Error closeFailure) {
                if (failure == null) failure = closeFailure;
                else if (failure != closeFailure) failure.addSuppressed(closeFailure);
            }
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
    }

    private static void closeUnderPrimary(SourceExecutor<?> executor, Throwable primary) {
        if (executor == null) return;
        try {
            executor.close();
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != primary) primary.addSuppressed(closeFailure);
        }
    }

    private static IllegalArgumentException invalid(String detail, Throwable cause) {
        return new IllegalArgumentException("SOURCE_POSITION_INVALID: " + detail, cause);
    }
}
