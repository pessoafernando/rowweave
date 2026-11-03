package io.rowweave.internal;

import io.rowweave.Source;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/** Shared one-child-at-a-time lifecycle core for local and assigned batch source execution. */
public final class SequentialInputExecutorCursor implements AutoCloseable {

    private final List<Source> inputs;
    private int nextInput;
    private volatile SourceExecutor<?> current;
    private volatile SourceExecutor<?> opening;
    private boolean ready;
    private boolean opened;
    private volatile boolean closed;

    public SequentialInputExecutorCursor(List<? extends Source> inputs) {
        if (inputs == null) {
            throw new IllegalArgumentException("sequential inputs must not be null");
        }
        this.inputs = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            Source input = inputs.get(i);
            if (input == null) {
                throw new IllegalArgumentException(
                        "sequential input[" + i + "] must not be null");
            }
            this.inputs.add(input);
        }
    }

    public void open() {
        if (opened) {
            throw new IllegalStateException("sequential input cursor is already open");
        }
        if (closed) {
            throw new IllegalStateException("sequential input cursor is closed");
        }
        opened = true;
    }

    public boolean hasNext() {
        if (closed) return false;
        requireOpen();
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
            if (nextInput >= inputs.size()) {
                return false;
            }
            SourceExecutor<?> candidate;
            try {
                candidate = SourceExecutorFactory.create(inputs.get(nextInput++));
            } catch (RuntimeException | Error primary) {
                return fail(primary);
            }
            synchronized (this) {
                if (closed) {
                    candidate.close();
                    return false;
                }
                opening = candidate;
            }
            try {
                candidate.open();
            } catch (RuntimeException | Error primary) {
                synchronized (this) {
                    if (opening == candidate) opening = null;
                }
                try {
                    candidate.close();
                } catch (RuntimeException | Error closeFailure) {
                    if (closeFailure != primary) primary.addSuppressed(closeFailure);
                }
                return fail(primary);
            }
            boolean publish;
            synchronized (this) {
                publish = !closed && opening == candidate;
                if (opening == candidate) opening = null;
                if (publish) current = candidate;
            }
            if (!publish) {
                candidate.close();
                return false;
            }
        }
    }

    public DataRow next() {
        if (!ready && !hasNext()) {
            throw new NoSuchElementException("sequential input cursor is exhausted");
        }
        ready = false;
        try {
            return current.next();
        } catch (RuntimeException | Error primary) {
            return fail(primary);
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
        }
        closeExecutors(active, pending);
    }

    private void closeExhaustedCurrent(SourceExecutor<?> exhausted) {
        synchronized (this) {
            if (current != exhausted) return;
            current = null;
            ready = false;
        }
        try {
            exhausted.close();
        } catch (RuntimeException | Error closeFailure) {
            closed = true;
            throw closeFailure;
        }
    }

    private void requireOpen() {
        if (!opened) {
            throw new IllegalStateException("sequential input cursor must be opened first");
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
}
