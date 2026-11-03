package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.cluster.rpc.ControlResidentOwner;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.plan.StageExecutionTelemetry;

import java.lang.ref.WeakReference;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

/** The Worker's bounded idempotent ledger for at-least-once ASSIGN_STAGE deliveries. */
final class WorkerAssignmentLedger {
    @FunctionalInterface
    interface PreparedResourceFactory {
        AutoCloseable prepare();
    }

    enum Decision { NEW, DUPLICATE, CONFLICT, CANCELLED, CAPACITY_EXHAUSTED, FULL }
    enum AdmissionWait { ADMITTED, REJECTED, TIMED_OUT }
    enum ReconciliationDecision {
        ACTIVE, TERMINAL_PENDING, TERMINAL_ACKNOWLEDGED, ABSENT, CONFLICT
    }
    private enum State { PREPARING, QUEUED, RUNNING, TERMINAL, CANCELLED }
    private enum Admission { PREPARING, ADMITTED, REJECTED }

    static final class Reconciliation {
        final ReconciliationDecision decision;
        final Slot slot;
        final StageTerminalReceipt receipt;
        final PreparedSinkReceipt preparedSinkReceipt;

        Reconciliation(ReconciliationDecision decision, Slot slot,
                       StageTerminalReceipt receipt,
                       PreparedSinkReceipt preparedSinkReceipt) {
            this.decision = decision;
            this.slot = slot;
            this.receipt = receipt;
            this.preparedSinkReceipt = preparedSinkReceipt;
        }
    }

    static final class Registration {
        final Decision decision;
        final Slot slot;
        final String error;

        Registration(Decision decision, Slot slot, String error) {
            this.decision = decision;
            this.slot = slot;
            this.error = error;
        }
    }

    static final class Cancellation {
        final Slot slot;
        final boolean reportCancelled;
        final Future<?> future;
        final StageTask task;
        final StageCommand command;

        Cancellation(Slot slot, boolean reportCancelled, Future<?> future,
                     StageTask task, StageCommand command) {
            this.slot = slot;
            this.reportCancelled = reportCancelled;
            this.future = future;
            this.task = task;
            this.command = command;
        }
    }

    static final class Slot {
        final long stageId;
        final Fingerprint fingerprint;
        final RunAttemptFence runAttemptFence;
        final boolean hadAssignment;
        final boolean durableAssignment;
        final CountDownLatch terminal = new CountDownLatch(1);
        final CountDownLatch admissionTerminal = new CountDownLatch(1);
        State state;
        Admission admission = Admission.PREPARING;
        boolean cancelRequested;
        boolean reportClaimed;
        long terminalAt;
        Future<?> future;
        StageTask task;
        StageCommand command;
        AutoCloseable preparedAuthorization;
        String admissionError;
        StageAssignmentToken durableToken;
        byte[] durableCommandDigest;
        StageTerminalReceipt terminalReceipt;
        PreparedSinkReceipt preparedSinkReceipt;
        StageExecutionTelemetry terminalTelemetry;
        boolean terminalTelemetryDiscarded;
        boolean terminalReportAcknowledged;
        ControlResidentLease residentLease;
        ControlResidentLease.OwnerToken residentOwner;

        Slot(long stageId, Fingerprint fingerprint, RunAttemptFence runAttemptFence,
             boolean hadAssignment, boolean durableAssignment, State state,
             ControlResidentLease residentLease,
             ControlResidentLease.OwnerToken residentOwner) {
            this.stageId = stageId;
            this.fingerprint = fingerprint;
            this.runAttemptFence = runAttemptFence;
            this.hadAssignment = hadAssignment;
            this.durableAssignment = durableAssignment;
            this.state = state;
            this.residentLease = residentLease;
            this.residentOwner = residentOwner;
        }
    }

    private static final class Fingerprint {
        final byte[] sha256;
        final WeakReference<Object> object;

        private Fingerprint(byte[] sha256, Object object) {
            this.sha256 = sha256;
            this.object = object == null ? null : new WeakReference<>(object);
        }

        static Fingerprint socket(byte[] blob) {
            if (blob == null) {
                throw new IllegalArgumentException("stage command blob must not be null");
            }
            try {
                return new Fingerprint(MessageDigest.getInstance("SHA-256").digest(blob), null);
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        }

        static Fingerprint inProcess(Object command) {
            if (command == null) {
                throw new IllegalArgumentException("stage command must not be null");
            }
            return new Fingerprint(null, command);
        }

        boolean matches(Fingerprint other) {
            if (sha256 != null || other.sha256 != null) {
                return sha256 != null && other.sha256 != null && Arrays.equals(sha256, other.sha256);
            }
            return object.get() != null && object.get() == other.object.get();
        }
    }

    private final Map<Long, Slot> slots = new LinkedHashMap<>();
    private final int activeLimit;
    private final long retentionMs;
    private final int maxEntries;
    private final java.util.concurrent.atomic.AtomicInteger active = new java.util.concurrent.atomic.AtomicInteger();

    WorkerAssignmentLedger(int activeLimit) {
        this(activeLimit, Long.getLong("rowweave.worker.assignmentRetentionMs", 300_000L),
                Integer.getInteger("rowweave.worker.assignmentMaxEntries", 100_000));
    }

    WorkerAssignmentLedger(int activeLimit, long retentionMs, int maxEntries) {
        if (activeLimit < 1 || retentionMs < 1 || maxEntries < 1) {
            throw new IllegalArgumentException("invalid assignment ledger bounds");
        }
        this.activeLimit = activeLimit;
        this.retentionMs = retentionMs;
        this.maxEntries = maxEntries;
    }

    Registration registerSocket(long stageId, byte[] blob, RunAttemptFence runAttemptFence) {
        return registerSocket(stageId, blob, runAttemptFence, false);
    }

    Registration registerSocket(long stageId, byte[] blob, RunAttemptFence runAttemptFence,
                                boolean durableAssignment) {
        if (durableAssignment) {
            throw new IllegalArgumentException(
                    "durable assignment must publish its terminal identity atomically");
        }
        return register(stageId, Fingerprint.socket(blob), requireFence(runAttemptFence),
                false, null, null, null, null);
    }

    Registration registerSocket(long stageId, byte[] blob, RunAttemptFence runAttemptFence,
                                boolean durableAssignment, ControlResidentLease residentLease,
                                ControlResidentLease.OwnerToken handlerOwner) {
        if (durableAssignment) {
            throw new IllegalArgumentException(
                    "durable assignment must publish its terminal identity atomically");
        }
        if ((residentLease == null) != (handlerOwner == null)) {
            throw new IllegalArgumentException("resident lease and owner must be provided together");
        }
        return register(stageId, Fingerprint.socket(blob), requireFence(runAttemptFence),
                false, null, null, residentLease, handlerOwner);
    }

    Registration registerDurableSocket(
            long stageId, byte[] blob, RunAttemptFence runAttemptFence,
            StageAssignmentToken token, byte[] commandDigest,
            ControlResidentLease residentLease,
            ControlResidentLease.OwnerToken handlerOwner) {
        if (token == null || commandDigest == null) {
            throw new IllegalArgumentException("durable terminal identity is required");
        }
        if ((residentLease == null) != (handlerOwner == null)) {
            throw new IllegalArgumentException("resident lease and owner must be provided together");
        }
        return register(stageId, Fingerprint.socket(blob), requireFence(runAttemptFence),
                true, token, commandDigest, residentLease, handlerOwner);
    }

    Registration registerInProcess(long stageId, Object command, RunAttemptFence runAttemptFence) {
        return register(stageId, Fingerprint.inProcess(command), requireFence(runAttemptFence),
                false, null, null, null, null);
    }

    private synchronized Registration register(
            long stageId, Fingerprint fingerprint, RunAttemptFence runAttemptFence,
            boolean durableAssignment, StageAssignmentToken durableToken,
            byte[] durableCommandDigest, ControlResidentLease residentLease,
            ControlResidentLease.OwnerToken handlerOwner) {
        boolean hasDurableIdentity = durableToken != null && durableCommandDigest != null;
        if (durableAssignment != hasDurableIdentity
                || (durableToken == null) != (durableCommandDigest == null)
                || durableCommandDigest != null
                && durableCommandDigest.length != StageTerminalReceipt.DIGEST_BYTES) {
            throw new IllegalArgumentException("durable terminal identity is incomplete");
        }
        if (durableToken != null && (!durableAssignment
                || durableToken.runtimeStageId() != stageId
                || !durableToken.fence().equals(runAttemptFence))) {
            throw new IllegalArgumentException("durable terminal identity does not match slot");
        }
        Slot existing = slots.get(stageId);
        if (existing != null) {
            boolean replaceTerminal = false;
            synchronized (existing) {
                if (!existing.runAttemptFence.equals(runAttemptFence)
                        && (existing.state == State.TERMINAL
                        || existing.state == State.CANCELLED)) {
                    replaceTerminal = true;
                }
            }
            if (replaceTerminal) {
                slots.remove(stageId);
                existing = null;
            } else if (!existing.hadAssignment) {
                return new Registration(Decision.CANCELLED, existing,
                        "stage " + stageId + " was cancelled before assignment");
            } else if (existing.fingerprint.matches(fingerprint)
                    && existing.runAttemptFence.equals(runAttemptFence)
                    && existing.durableAssignment == durableAssignment) {
                if (durableToken != null && (existing.durableToken == null
                        || !existing.durableToken.equals(durableToken)
                        || !MessageDigest.isEqual(
                        existing.durableCommandDigest, durableCommandDigest))) {
                    return new Registration(Decision.CONFLICT, existing,
                            "conflicting durable assignment identity for stage " + stageId);
                }
                return new Registration(Decision.DUPLICATE, existing, null);
            } else {
                return new Registration(Decision.CONFLICT, existing,
                        "conflicting assignment for stage " + stageId);
            }
        }
        int activeCount = active.get();
        if (activeCount >= activeLimit) {
            return new Registration(Decision.CAPACITY_EXHAUSTED, null,
                    "stage assignment capacity exhausted: stageId=" + stageId
                            + " active=" + activeCount + " slots=" + activeLimit);
        }
        if (!makeRoom()) {
            return new Registration(Decision.FULL, null, "assignment ledger is full");
        }
        ControlResidentLease.OwnerToken assignmentOwner = residentLease == null ? null
                : residentLease.transfer(handlerOwner, ControlResidentOwner.ASSIGNMENT_OWNED);
        Slot slot = new Slot(stageId, fingerprint, runAttemptFence,
                true, durableAssignment, State.PREPARING, residentLease, assignmentOwner);
        try {
            if (durableToken != null) {
                // Identity is installed before slots.put(): no cancel/close observer can ever
                // see a durable assignment that cannot form its terminal receipt.
                bindDurableIdentity(slot, durableToken, durableCommandDigest);
            }
            // handoff CAS happens-before the slot becomes visible in the ledger.
            slots.put(stageId, slot);
            active.incrementAndGet();
            return new Registration(Decision.NEW, slot, null);
        } catch (RuntimeException | Error publishFailure) {
            if (residentLease != null) residentLease.release(assignmentOwner);
            throw publishFailure;
        }
    }

    synchronized Cancellation cancel(long stageId, RunAttemptFence runAttemptFence) {
        requireFence(runAttemptFence);
        Slot slot = slots.get(stageId);
        if (slot == null) {
            if (!makeRoom()) {
                throw new IllegalStateException("assignment ledger is full; cannot record cancellation");
            }
            slot = new Slot(stageId, null, runAttemptFence, false, false, State.CANCELLED,
                    null, null);
            slot.cancelRequested = true;
            slot.terminalAt = System.currentTimeMillis();
            slot.terminal.countDown();
            slots.put(stageId, slot);
            return new Cancellation(slot, false, null, null, null);
        }
        synchronized (slot) {
            if (!slot.runAttemptFence.equals(runAttemptFence)) {
                throw new IllegalArgumentException(
                        "run attempt fence mismatch for stage " + stageId);
            }
            if (slot.state == State.TERMINAL || slot.state == State.CANCELLED) {
                return new Cancellation(slot, false, null, null, null);
            }
            slot.cancelRequested = true;
            boolean queued = slot.state == State.PREPARING || slot.state == State.QUEUED;
            Future<?> future = slot.future;
            StageTask task = slot.task;
            StageCommand command = slot.command;
            if (queued) {
                if (slot.admission == Admission.PREPARING) {
                    slot.admission = Admission.REJECTED;
                    slot.admissionError = "stage assignment was cancelled during admission";
                    slot.admissionTerminal.countDown();
                }
                slot.state = State.CANCELLED;
                active.decrementAndGet();
                clearLargeReferences(slot);
                slot.terminalAt = System.currentTimeMillis();
                slot.terminal.countDown();
            }
            return new Cancellation(slot, queued && slot.hadAssignment, future, task, command);
        }
    }

    void attachFuture(Slot slot, Future<?> future) {
        boolean cancel;
        synchronized (slot) {
            cancel = slot.state == State.CANCELLED || slot.state == State.TERMINAL;
            if (!cancel) {
                slot.future = future;
            }
        }
        if (cancel) {
            future.cancel(true);
        }
    }

    /** Future ownership is installed before PREPARING becomes visible as ADMITTED/QUEUED. */
    boolean attachFutureAndAdmit(Slot slot, Future<?> future) {
        if (future == null) throw new IllegalArgumentException("stage future is required");
        boolean admitted = false;
        synchronized (slot) {
            if (slot.state == State.PREPARING
                    && slot.admission == Admission.PREPARING
                    && !slot.cancelRequested) {
                slot.future = future;
                slot.state = State.QUEUED;
                slot.admission = Admission.ADMITTED;
                slot.admissionTerminal.countDown();
                admitted = true;
            }
        }
        if (!admitted) future.cancel(true);
        return admitted;
    }

    void rejectAdmission(Slot slot, String error) {
        Future<?> future = null;
        synchronized (slot) {
            if (slot.admission != Admission.PREPARING) return;
            slot.admission = Admission.REJECTED;
            slot.admissionError = error == null ? "stage assignment admission rejected" : error;
            slot.cancelRequested = true;
            future = slot.future;
            slot.state = State.CANCELLED;
            active.decrementAndGet();
            clearLargeReferences(slot);
            slot.terminalAt = System.currentTimeMillis();
            slot.admissionTerminal.countDown();
            slot.terminal.countDown();
        }
        if (future != null) future.cancel(true);
    }

    AdmissionWait awaitAdmissionUntil(Slot slot, long deadlineNanos) {
        for (;;) {
            synchronized (slot) {
                if (slot.admission == Admission.ADMITTED) return AdmissionWait.ADMITTED;
                if (slot.admission == Admission.REJECTED) return AdmissionWait.REJECTED;
            }
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) return AdmissionWait.TIMED_OUT;
            try {
                if (!slot.admissionTerminal.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS)) {
                    return AdmissionWait.TIMED_OUT;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return AdmissionWait.TIMED_OUT;
            }
        }
    }

    boolean awaitStartPermission(Slot slot) {
        boolean interrupted = false;
        try {
            while (slot.admissionTerminal.getCount() != 0L) {
                try {
                    slot.admissionTerminal.await();
                } catch (InterruptedException stop) {
                    interrupted = true;
                    return false;
                }
            }
            synchronized (slot) {
                return slot.admission == Admission.ADMITTED
                        && slot.state == State.QUEUED && !slot.cancelRequested;
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    boolean isPreparing(Slot slot) {
        synchronized (slot) {
            return slot.admission == Admission.PREPARING && slot.state == State.PREPARING;
        }
    }

    void resolveDurableAdmission(Slot slot, boolean admitted, String error) {
        if (!admitted) rejectAdmission(slot, error);
    }

    boolean awaitDurableAdmission(Slot slot) {
        return awaitAdmissionUntil(slot, Long.MAX_VALUE) == AdmissionWait.ADMITTED;
    }

    String durableAdmissionError(Slot slot) {
        synchronized (slot) {
            return slot.admissionError;
        }
    }

    boolean isCancellationRequested(Slot slot) {
        synchronized (slot) {
            return slot.cancelRequested
                    || slot.state == State.CANCELLED;
        }
    }

    void bindDurableIdentity(Slot slot, StageAssignmentToken token,
                             byte[] commandDigest) {
        if (token == null || commandDigest == null
                || commandDigest.length != StageTerminalReceipt.DIGEST_BYTES) {
            throw new IllegalArgumentException("durable terminal identity is incomplete");
        }
        synchronized (slot) {
            if (!slot.durableAssignment) {
                throw new IllegalStateException("legacy assignment has no durable terminal identity");
            }
            if (token.runtimeStageId() != slot.stageId
                    || !token.fence().equals(slot.runAttemptFence)) {
                throw new IllegalArgumentException("durable terminal identity does not match slot");
            }
            if (slot.durableToken != null) {
                if (!slot.durableToken.equals(token)
                        || !MessageDigest.isEqual(slot.durableCommandDigest, commandDigest)) {
                    throw new IllegalArgumentException("conflicting durable terminal identity");
                }
                return;
            }
            slot.durableToken = token;
            slot.durableCommandDigest = commandDigest.clone();
        }
    }

    StageTerminalReceipt freezeTerminal(Slot slot, StageTerminalReceipt candidate) {
        return freezeTerminal(slot, candidate, null);
    }

    StageTerminalReceipt freezeTerminal(
            Slot slot, StageTerminalReceipt candidate,
            PreparedSinkReceipt preparedSinkReceipt) {
        if (candidate == null) throw new IllegalArgumentException("terminal receipt is required");
        synchronized (slot) {
            requireTerminalIdentity(slot, candidate);
            requirePreparedIdentity(slot, candidate, preparedSinkReceipt);
            if (slot.terminalReceipt == null) {
                slot.terminalReceipt = candidate;
                slot.preparedSinkReceipt = preparedSinkReceipt;
            }
            return slot.terminalReceipt;
        }
    }

    StageTerminalReceipt terminalReceipt(Slot slot) {
        synchronized (slot) {
            return slot.terminalReceipt;
        }
    }

    PreparedSinkReceipt preparedSinkReceipt(Slot slot) {
        synchronized (slot) {
            return slot.preparedSinkReceipt;
        }
    }

    boolean hasPendingTerminalReport(Slot slot) {
        synchronized (slot) {
            return slot.terminalReceipt != null && !slot.terminalReportAcknowledged;
        }
    }

    boolean acknowledgeTerminalReport(Slot slot, StageTerminalReceipt receipt) {
        return acknowledgeTerminalReport(slot, receipt, null);
    }

    boolean acknowledgeTerminalReport(
            Slot slot, StageTerminalReceipt receipt,
            PreparedSinkReceipt preparedSinkReceipt) {
        if (receipt == null) throw new IllegalArgumentException("terminal receipt is required");
        synchronized (slot) {
            requireTerminalIdentity(slot, receipt);
            if (slot.terminalReceipt == null || !slot.terminalReceipt.equals(receipt)) {
                throw new IllegalArgumentException("terminal ACK does not match frozen receipt");
            }
            if (!java.util.Objects.equals(
                    slot.preparedSinkReceipt, preparedSinkReceipt)) {
                throw new IllegalArgumentException(
                        "terminal ACK does not match frozen prepared Sink receipt");
            }
            if (slot.terminalReportAcknowledged) return false;
            slot.terminalReportAcknowledged = true;
            return true;
        }
    }

    boolean releaseTerminalReportClaim(Slot slot, StageTerminalReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("terminal receipt is required");
        synchronized (slot) {
            requireTerminalIdentity(slot, receipt);
            if (slot.terminalReceipt == null || !slot.terminalReceipt.equals(receipt)) {
                throw new IllegalArgumentException("report claim does not match frozen receipt");
            }
            if (slot.terminalReportAcknowledged || !slot.reportClaimed) return false;
            slot.reportClaimed = false;
            return true;
        }
    }

    synchronized Reconciliation reconcile(long stageId, StageAssignmentToken token,
                                            byte[] commandDigest) {
        if (token == null || commandDigest == null
                || commandDigest.length != StageTerminalReceipt.DIGEST_BYTES
                || token.runtimeStageId() != stageId) {
            return new Reconciliation(
                    ReconciliationDecision.CONFLICT, null, null, null);
        }
        Slot slot = slots.get(stageId);
        if (slot == null) {
            return new Reconciliation(
                    ReconciliationDecision.ABSENT, null, null, null);
        }
        synchronized (slot) {
            if (!slot.hadAssignment) {
                return new Reconciliation(
                        ReconciliationDecision.ABSENT, slot, null, null);
            }
            if (!slot.durableAssignment || slot.durableToken == null
                    || !slot.durableToken.equals(token)
                    || !MessageDigest.isEqual(slot.durableCommandDigest, commandDigest)) {
                return new Reconciliation(
                        ReconciliationDecision.CONFLICT, slot, null, null);
            }
            if (slot.terminalReceipt == null) {
                return new Reconciliation(
                        ReconciliationDecision.ACTIVE, slot, null, null);
            }
            return new Reconciliation(slot.terminalReportAcknowledged
                    ? ReconciliationDecision.TERMINAL_ACKNOWLEDGED
                    : ReconciliationDecision.TERMINAL_PENDING,
                    slot, slot.terminalReceipt, slot.preparedSinkReceipt);
        }
    }

    private static void requireTerminalIdentity(Slot slot, StageTerminalReceipt receipt) {
        if (!slot.durableAssignment || slot.durableToken == null
                || slot.durableCommandDigest == null) {
            throw new IllegalStateException("durable terminal identity is not bound");
        }
        if (!slot.durableToken.equals(receipt.token())
                || !MessageDigest.isEqual(slot.durableCommandDigest,
                receipt.commandDigest())) {
            throw new IllegalArgumentException("terminal receipt does not match accepted assignment");
        }
    }

    private static void requirePreparedIdentity(
            Slot slot, StageTerminalReceipt terminal,
            PreparedSinkReceipt preparedSinkReceipt) {
        if (preparedSinkReceipt == null) return;
        if (!terminal.succeeded()
                || !preparedSinkReceipt.token().equals(terminal.token())
                || !MessageDigest.isEqual(preparedSinkReceipt.commandDigest(),
                terminal.commandDigest())) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt does not match terminal receipt");
        }
    }

    boolean tryStart(Slot slot) {
        synchronized (slot) {
            if (slot.state != State.QUEUED) {
                return false;
            }
            slot.state = State.RUNNING;
            return true;
        }
    }

    boolean attachCommand(Slot slot, StageCommand command) {
        synchronized (slot) {
            if (slot.state == State.CANCELLED || slot.state == State.TERMINAL) {
                return true;
            }
            slot.command = command;
            return slot.cancelRequested;
        }
    }

    boolean attachPreparedAuthorization(Slot slot, AutoCloseable preparedAuthorization) {
        if (preparedAuthorization == null) {
            throw new IllegalArgumentException("prepared authorization is required");
        }
        synchronized (slot) {
            if (slot.state == State.CANCELLED || slot.state == State.TERMINAL
                    || slot.cancelRequested) {
                closeAuthorization(preparedAuthorization);
                return true;
            }
            if (slot.preparedAuthorization != null) {
                closeAuthorization(preparedAuthorization);
                throw new IllegalStateException(
                        "stage assignment authorization is already attached");
            }
            slot.preparedAuthorization = preparedAuthorization;
            return false;
        }
    }

    /**
     * Runs publication while holding the slot cancellation linearization, then installs
     * the only owning reference before cancellation can observe the published resource.
     */
    boolean prepareOwned(Slot slot, PreparedResourceFactory factory) {
        if (factory == null) {
            throw new IllegalArgumentException("prepared resource factory is required");
        }
        synchronized (slot) {
            if (slot.state == State.CANCELLED || slot.state == State.TERMINAL
                    || slot.cancelRequested) {
                return true;
            }
            if (slot.preparedAuthorization != null) {
                throw new IllegalStateException(
                        "stage assignment authorization is already attached");
            }
            AutoCloseable prepared = factory.prepare();
            if (prepared == null) {
                throw new IllegalStateException(
                        "prepared resource factory returned no owner");
            }
            slot.preparedAuthorization = prepared;
            return false;
        }
    }

    boolean attachTask(Slot slot, StageTask task) {
        synchronized (slot) {
            slot.task = task;
            return slot.cancelRequested;
        }
    }

    void reserveResident(Slot slot, long bytes) {
        synchronized (slot) {
            if (slot.residentLease != null) {
                slot.residentLease.reserve(slot.residentOwner, bytes);
            }
        }
    }

    void complete(Slot slot) {
        synchronized (slot) {
            if (slot.state == State.TERMINAL || slot.state == State.CANCELLED) {
                return;
            }
            slot.state = State.TERMINAL;
            active.decrementAndGet();
            clearLargeReferences(slot);
            slot.terminalAt = System.currentTimeMillis();
            slot.terminal.countDown();
        }
    }

    boolean claimReport(Slot slot) {
        synchronized (slot) {
            if (slot.reportClaimed) {
                return false;
            }
            slot.reportClaimed = true;
            return true;
        }
    }

    StageExecutionTelemetry freezeTelemetry(Slot slot, StageExecutionTelemetry telemetry) {
        synchronized (slot) {
            StageExecutionTelemetry value = telemetry == null
                    ? StageExecutionTelemetry.unavailable() : telemetry;
            if (slot.terminalTelemetryDiscarded) return value;
            if (slot.terminalTelemetry == null) {
                slot.terminalTelemetry = value;
            }
            return slot.terminalTelemetry;
        }
    }

    void clearTelemetry(WorkerAssignmentLedger.Slot slot) {
        synchronized (slot) {
            slot.terminalTelemetry = null;
            slot.terminalTelemetryDiscarded = true;
        }
    }

    int frozenTelemetryCount() {
        int count = 0;
        for (Slot slot : slots.values()) {
            synchronized (slot) {
                if (slot.terminalTelemetry != null) count++;
            }
        }
        return count;
    }

    void awaitTerminal(Slot slot) {
        boolean interrupted = false;
        for (;;) {
            try {
                slot.terminal.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Shutting a worker down uses one monotonic deadline, rather than a fresh wait budget per stage. */
    boolean awaitTerminalUntil(Slot slot, long deadlineNanos) {
        boolean interrupted = false;
        boolean terminal = false;
        while (!terminal) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) {
                break;
            }
            try {
                terminal = slot.terminal.await(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
                break;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return terminal || slot.terminal.getCount() == 0L;
    }

    synchronized int activeCount() {
        return active.get();
    }

    synchronized java.util.List<Slot> activeSlots() {
        java.util.List<Slot> active = new java.util.ArrayList<>();
        for (Slot slot : slots.values()) {
            synchronized (slot) {
                if (slot.state == State.PREPARING || slot.state == State.QUEUED
                        || slot.state == State.RUNNING) {
                    active.add(slot);
                }
            }
        }
        return active;
    }

    private boolean makeRoom() {
        if (slots.size() < maxEntries) {
            return true;
        }
        long cutoff = System.currentTimeMillis() - retentionMs;
        Iterator<Slot> it = slots.values().iterator();
        while (it.hasNext()) {
            Slot slot = it.next();
            synchronized (slot) {
                if ((slot.state == State.TERMINAL || slot.state == State.CANCELLED)
                        && slot.terminalAt <= cutoff) {
                    it.remove();
                }
            }
        }
        return slots.size() < maxEntries;
    }

    private static void clearLargeReferences(Slot slot) {
        slot.future = null;
        slot.task = null;
        slot.command = null;
        AutoCloseable authorization = slot.preparedAuthorization;
        slot.preparedAuthorization = null;
        ControlResidentLease lease = slot.residentLease;
        ControlResidentLease.OwnerToken owner = slot.residentOwner;
        slot.residentLease = null;
        slot.residentOwner = null;
        if (lease != null) lease.release(owner);
        closeAuthorization(authorization);
    }

    private static void closeAuthorization(AutoCloseable authorization) {
        if (authorization == null) return;
        try {
            authorization.close();
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new RuntimeException("failed to close stage authorization", failure);
        }
    }

    private static RunAttemptFence requireFence(RunAttemptFence fence) {
        if (fence == null) {
            throw new IllegalArgumentException("run attempt fence is required");
        }
        return fence;
    }
}
