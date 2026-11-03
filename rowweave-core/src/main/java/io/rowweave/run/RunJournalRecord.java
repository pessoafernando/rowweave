package io.rowweave.run;

import java.security.MessageDigest;

/** One journal fact for an accepted mutation: the command, its next snapshot and its event. */
public final class RunJournalRecord {
    private final RunCommand command;
    private final RunSnapshot snapshot;
    private final ExecutionEvent event;

    public RunJournalRecord(RunCommand command, RunSnapshot snapshot, ExecutionEvent event) {
        if (command == null || snapshot == null || event == null) {
            throw new IllegalArgumentException("journal record fields are required");
        }
        if (!command.runId.equals(snapshot.runId()) || !command.runId.equals(event.runId())) {
            throw new IllegalArgumentException("journal record run identity mismatch");
        }
        if (!command.commandId.equals(event.commandId())
                || !MessageDigest.isEqual(command.commandDigestUnsafe(), event.commandDigestUnsafe())) {
            throw new IllegalArgumentException("journal record command identity mismatch");
        }
        long expectedNextRevision = command.kind == RunCommand.Kind.CREATE_RUN
                ? 1L : command.expectedRevision + 1L;
        if (snapshot.revision() != expectedNextRevision
                || event.sequence() != snapshot.revision()) {
            throw new IllegalArgumentException("journal record revision mismatch");
        }
        this.command = command;
        this.snapshot = snapshot;
        this.event = event;
    }

    public RunCommand command() { return command; }
    public RunSnapshot snapshot() { return snapshot; }
    public ExecutionEvent event() { return event; }
}
