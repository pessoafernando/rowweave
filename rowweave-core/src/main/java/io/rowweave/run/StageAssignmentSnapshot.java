package io.rowweave.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable authority view: physical identity plus mutable acceptance generation/receipts. */
public final class StageAssignmentSnapshot {
    private final StageAssignmentManifest manifest;
    private final long acceptanceGeneration;
    private final List<StageAssignmentAcceptance> acceptances;
    private final List<StageTerminalReceipt> terminals;
    private final List<PreparedSinkReceipt> preparedSinks;

    StageAssignmentSnapshot(StageAssignmentManifest manifest, long acceptanceGeneration,
                            List<StageAssignmentAcceptance> acceptances) {
        this(manifest, acceptanceGeneration, acceptances,
                emptyTerminals(manifest), emptyPrepared(manifest));
    }

    StageAssignmentSnapshot(StageAssignmentManifest manifest, long acceptanceGeneration,
                            List<StageAssignmentAcceptance> acceptances,
                            List<StageTerminalReceipt> terminals) {
        this(manifest, acceptanceGeneration, acceptances, terminals,
                emptyPrepared(manifest));
    }

    StageAssignmentSnapshot(StageAssignmentManifest manifest, long acceptanceGeneration,
                            List<StageAssignmentAcceptance> acceptances,
                            List<StageTerminalReceipt> terminals,
                            List<PreparedSinkReceipt> preparedSinks) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        if (acceptanceGeneration <= 0L) {
            throw new IllegalArgumentException("acceptanceGeneration must be positive");
        }
        if (acceptances == null || acceptances.size() != manifest.assignmentCount()) {
            throw new IllegalArgumentException("acceptance count must match manifest");
        }
        if (terminals == null || terminals.size() != manifest.assignmentCount()) {
            throw new IllegalArgumentException("terminal count must match manifest");
        }
        if (preparedSinks == null
                || preparedSinks.size() != manifest.assignmentCount()) {
            throw new IllegalArgumentException(
                    "prepared Sink count must match manifest");
        }
        List<StageAssignmentAcceptance> copy = new ArrayList<>(acceptances.size());
        List<StageTerminalReceipt> terminalCopy = new ArrayList<>(terminals.size());
        List<PreparedSinkReceipt> preparedCopy =
                new ArrayList<>(preparedSinks.size());
        for (int i = 0; i < acceptances.size(); i++) {
            StageAssignmentAcceptance acceptance = acceptances.get(i);
            if (acceptance != null) {
                StageAssignmentToken token = acceptance.token();
                StageAssignmentRecord record = manifest.assignment(i);
                if (!token.fence().equals(manifest.fence())
                        || token.manifestGeneration() != manifest.generation()
                        || token.physicalOrdinal() != i
                        || !token.stageExecutionId().equals(record.stageExecutionId())
                        || token.runtimeStageId() != record.runtimeStageId()
                        || !token.workerId().equals(record.workerId())
                        || !token.workerIncarnationId().equals(record.workerIncarnationId())) {
                    throw new IllegalArgumentException("acceptance identity mismatch at " + i);
                }
            }
            copy.add(acceptance);
            StageTerminalReceipt terminal = terminals.get(i);
            if (terminal != null) {
                if (acceptance == null) {
                    throw new IllegalArgumentException(
                            "terminal receipt requires acceptance at " + i);
                }
                if (!terminal.token().equals(acceptance.token())
                        || !java.security.MessageDigest.isEqual(
                        terminal.commandDigest(), acceptance.commandDigest())) {
                    throw new IllegalArgumentException(
                            "terminal receipt identity mismatch at " + i);
                }
            }
            terminalCopy.add(terminal);
            PreparedSinkReceipt prepared = preparedSinks.get(i);
            if (prepared != null && (terminal == null || !terminal.succeeded()
                    || !prepared.token().equals(terminal.token())
                    || !java.security.MessageDigest.isEqual(
                    prepared.commandDigest(), terminal.commandDigest()))) {
                throw new IllegalArgumentException(
                        "prepared Sink requires matching successful terminal at " + i);
            }
            preparedCopy.add(prepared);
        }
        this.manifest = manifest;
        this.acceptanceGeneration = acceptanceGeneration;
        this.acceptances = Collections.unmodifiableList(copy);
        this.terminals = Collections.unmodifiableList(terminalCopy);
        this.preparedSinks = Collections.unmodifiableList(preparedCopy);
    }

    public static StageAssignmentSnapshot initial(StageAssignmentManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        return new StageAssignmentSnapshot(manifest, 1L,
                Collections.nCopies(manifest.assignmentCount(),
                        (StageAssignmentAcceptance) null));
    }

    public StageAssignmentManifest manifest() { return manifest; }
    public long acceptanceGeneration() { return acceptanceGeneration; }
    public StageAssignmentAcceptance acceptance(int ordinal) { return acceptances.get(ordinal); }
    public StageTerminalReceipt terminal(int ordinal) { return terminals.get(ordinal); }
    public PreparedSinkReceipt preparedSink(int ordinal) {
        return preparedSinks.get(ordinal);
    }
    public StageAssignmentToken token(int ordinal) {
        return StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(ordinal), acceptanceGeneration);
    }
    public int acceptedCount() {
        int count = 0;
        for (StageAssignmentAcceptance acceptance : acceptances) {
            if (acceptance != null) count++;
        }
        return count;
    }
    public boolean allNone() { return acceptedCount() == 0; }
    public int terminalCount() {
        int count = 0;
        for (StageTerminalReceipt terminal : terminals) {
            if (terminal != null) count++;
        }
        return count;
    }
    public boolean allTerminal() { return terminalCount() == manifest.assignmentCount(); }

    /** Returns a new immutable view with one previously-NONE receipt installed. */
    public StageAssignmentSnapshot withAcceptance(StageAssignmentAcceptance acceptance) {
        if (acceptance == null) throw new IllegalArgumentException("acceptance is required");
        int ordinal = acceptance.token().physicalOrdinal();
        if (ordinal < 0 || ordinal >= acceptances.size() || acceptances.get(ordinal) != null) {
            throw new IllegalArgumentException("assignment is already accepted or out of bounds");
        }
        List<StageAssignmentAcceptance> copy = new ArrayList<>(acceptances);
        copy.set(ordinal, acceptance);
        return new StageAssignmentSnapshot(
                manifest, acceptanceGeneration, copy, terminals, preparedSinks);
    }

    /** Returns a new immutable view with one accepted assignment's first terminal receipt. */
    public StageAssignmentSnapshot withTerminal(StageTerminalReceipt terminal) {
        if (terminal == null) throw new IllegalArgumentException("terminal is required");
        int ordinal = terminal.token().physicalOrdinal();
        if (ordinal < 0 || ordinal >= terminals.size()
                || terminals.get(ordinal) != null) {
            throw new IllegalArgumentException(
                    "assignment is already terminal or out of bounds");
        }
        if (acceptances.get(ordinal) == null) {
            throw new IllegalArgumentException("terminal receipt requires acceptance");
        }
        List<StageTerminalReceipt> copy = new ArrayList<>(terminals);
        copy.set(ordinal, terminal);
        return new StageAssignmentSnapshot(
                manifest, acceptanceGeneration, acceptances, copy, preparedSinks);
    }

    public StageAssignmentSnapshot withPreparedSink(PreparedSinkReceipt prepared) {
        if (prepared == null) throw new IllegalArgumentException("prepared is required");
        int ordinal = prepared.token().physicalOrdinal();
        if (ordinal < 0 || ordinal >= preparedSinks.size()
                || preparedSinks.get(ordinal) != null) {
            throw new IllegalArgumentException(
                    "prepared Sink is already recorded or out of bounds");
        }
        List<PreparedSinkReceipt> copy = new ArrayList<>(preparedSinks);
        copy.set(ordinal, prepared);
        return new StageAssignmentSnapshot(
                manifest, acceptanceGeneration, acceptances, terminals, copy);
    }

    /** Recovery claim fences future first accepts without rewriting existing receipts. */
    public StageAssignmentSnapshot withAcceptanceGeneration(long generation) {
        return new StageAssignmentSnapshot(
                manifest, generation, acceptances, terminals, preparedSinks);
    }

    private static List<StageTerminalReceipt> emptyTerminals(
            StageAssignmentManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        return Collections.nCopies(manifest.assignmentCount(),
                (StageTerminalReceipt) null);
    }

    private static List<PreparedSinkReceipt> emptyPrepared(
            StageAssignmentManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        return Collections.nCopies(manifest.assignmentCount(),
                (PreparedSinkReceipt) null);
    }
}
