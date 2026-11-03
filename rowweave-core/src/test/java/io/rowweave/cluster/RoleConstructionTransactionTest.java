package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RoleConstructionTransactionTest {

    @Test
    void rollbackClosesOnlyAcquiredResourcesInReverseOrderAndKeepsPrimary() {
        List<String> closed = new ArrayList<>();
        RoleConstructionTransaction transaction = new RoleConstructionTransaction();
        assertEquals("first", transaction.acquire(() -> "first", closed::add));
        assertEquals("second", transaction.acquire(() -> "second", closed::add));

        RuntimeException primary = new RuntimeException("construction-primary");
        assertSame(primary, transaction.rollback(primary));
        assertSame(primary, transaction.rollback(primary));
        assertEquals(Arrays.asList("second", "first"), closed);
    }

    @Test
    void cleanupFailuresAreSuppressedInRollbackOrder() {
        RoleConstructionTransaction transaction = new RoleConstructionTransaction();
        RuntimeException firstClose = new RuntimeException("first-close");
        RuntimeException secondClose = new RuntimeException("second-close");
        transaction.acquire(() -> "first", ignored -> { throw firstClose; });
        transaction.acquire(() -> "second", ignored -> { throw secondClose; });

        Error primary = new AssertionError("primary");
        assertSame(primary, transaction.rollback(primary));
        assertEquals(2, primary.getSuppressed().length);
        assertSame(secondClose, primary.getSuppressed()[0]);
        assertSame(firstClose, primary.getSuppressed()[1]);
    }

    @Test
    void commitTransfersOwnershipAndRejectsFurtherAcquire() {
        AtomicInteger closes = new AtomicInteger();
        RoleConstructionTransaction transaction = new RoleConstructionTransaction();
        transaction.acquire(Object::new, ignored -> closes.incrementAndGet());

        transaction.commit();
        assertEquals(0, closes.get());
        assertThrows(IllegalStateException.class,
                () -> transaction.acquire(Object::new, ignored -> { }));
        assertThrows(IllegalStateException.class,
                () -> transaction.rollback(new RuntimeException("late")));
    }

    @Test
    void registrationFailureImmediatelyClosesFreshResourceAndPreservesFailure() {
        Error registrationFailure = new AssertionError("register-action");
        RuntimeException cleanupFailure = new RuntimeException("fresh-close");
        AtomicInteger closes = new AtomicInteger();
        RoleConstructionTransaction transaction = new RoleConstructionTransaction(
                (actions, action) -> { throw registrationFailure; });

        Error observed = assertThrows(Error.class, () -> transaction.acquire(
                Object::new,
                ignored -> {
                    closes.incrementAndGet();
                    throw cleanupFailure;
                }));

        assertSame(registrationFailure, observed);
        assertEquals(1, closes.get());
        assertEquals(1, observed.getSuppressed().length);
        assertSame(cleanupFailure, observed.getSuppressed()[0]);
    }

    @Test
    void factoryFailureDoesNotRunCloserOrRegisterAction() {
        RuntimeException factoryFailure = new RuntimeException("factory");
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger registrations = new AtomicInteger();
        RoleConstructionTransaction transaction = new RoleConstructionTransaction(
                (actions, action) -> {
                    registrations.incrementAndGet();
                    actions.addFirst(action);
                });

        RuntimeException observed = assertThrows(RuntimeException.class,
                () -> transaction.acquire(
                        () -> { throw factoryFailure; },
                        ignored -> closes.incrementAndGet()));

        assertSame(factoryFailure, observed);
        assertEquals(0, closes.get());
        assertEquals(0, registrations.get());
    }
}
