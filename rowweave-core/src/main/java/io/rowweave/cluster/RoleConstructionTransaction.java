package io.rowweave.cluster;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Construction-only LIFO ownership scope for role resources.
 *
 * <p>The resource factory runs inside {@link #acquire}. If registering the
 * corresponding close action fails, the freshly returned resource is closed
 * synchronously before the registration failure is rethrown.</p>
 */
final class RoleConstructionTransaction {

    @FunctionalInterface
    interface Factory<T> {
        T create();
    }

    @FunctionalInterface
    interface Closer<T> {
        void close(T resource) throws Exception;
    }

    @FunctionalInterface
    interface ActionRegistrar {
        void register(Deque<CloseAction> actions, CloseAction action);
    }

    @FunctionalInterface
    interface CloseAction {
        void close() throws Exception;
    }

    private enum State {
        OPEN,
        COMMITTED,
        ROLLED_BACK
    }

    private final Deque<CloseAction> actions = new ArrayDeque<>();
    private final ActionRegistrar registrar;
    private State state = State.OPEN;

    RoleConstructionTransaction() {
        this((registered, action) -> registered.addFirst(action));
    }

    RoleConstructionTransaction(ActionRegistrar registrar) {
        if (registrar == null) {
            throw new IllegalArgumentException("construction action registrar is required");
        }
        this.registrar = registrar;
    }

    <T> T acquire(Factory<T> factory, Closer<? super T> closer) {
        requireOpen();
        if (factory == null || closer == null) {
            throw new IllegalArgumentException(
                    "construction resource factory and closer are required");
        }
        T resource = factory.create();
        try {
            CloseAction action = () -> closer.close(resource);
            registrar.register(actions, action);
        } catch (Throwable registrationFailure) {
            try {
                closer.close(resource);
            } catch (Throwable cleanupFailure) {
                appendSuppressed(registrationFailure, cleanupFailure);
            }
            rethrow(registrationFailure);
        }
        return resource;
    }

    void commit() {
        requireOpen();
        state = State.COMMITTED;
        actions.clear();
    }

    <T extends Throwable> T rollback(T primary) {
        if (primary == null) {
            throw new IllegalArgumentException("construction failure is required");
        }
        if (state == State.COMMITTED) {
            throw new IllegalStateException(
                    "committed construction ownership cannot be rolled back");
        }
        if (state == State.ROLLED_BACK) {
            return primary;
        }
        state = State.ROLLED_BACK;
        while (!actions.isEmpty()) {
            try {
                actions.removeFirst().close();
            } catch (Throwable cleanupFailure) {
                appendSuppressed(primary, cleanupFailure);
            }
        }
        return primary;
    }

    private void requireOpen() {
        if (state != State.OPEN) {
            throw new IllegalStateException(
                    "construction transaction is no longer open: " + state);
        }
    }

    private static void appendSuppressed(Throwable primary, Throwable cleanup) {
        if (primary != cleanup) {
            primary.addSuppressed(cleanup);
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }
}
