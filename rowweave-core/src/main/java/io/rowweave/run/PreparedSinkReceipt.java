package io.rowweave.run;

import java.security.MessageDigest;
import java.util.Objects;

/** Immutable authority evidence for one prepared provider Sink assignment. */
public final class PreparedSinkReceipt {
    private final StageAssignmentToken token;
    private final byte[] commandDigest;
    private final PreparedSinkLocator locator;

    public PreparedSinkReceipt(StageAssignmentToken token, byte[] commandDigest,
                               PreparedSinkLocator locator) {
        if (token == null || locator == null || commandDigest == null
                || commandDigest.length != StageTerminalReceipt.DIGEST_BYTES) {
            throw new IllegalArgumentException(
                    "prepared Sink token, command digest and locator are required");
        }
        SinkAttemptIdentity identity = locator.attemptIdentity();
        if (!token.fence().equals(identity.fence())
                || token.manifestGeneration() != identity.assignmentGeneration()
                || !token.stageExecutionId().equals(identity.stageExecutionId())
                || token.logicalStage() != identity.logicalStage()
                || token.instanceIndex() != identity.instanceIndex()
                || token.role() != StageAssignmentRole.SINK
                && token.role() != StageAssignmentRole.WHOLE_GRAPH
                && token.role() != StageAssignmentRole.SYSTEM_CONTROL) {
            throw new IllegalArgumentException(
                    "prepared Sink locator does not match assignment token");
        }
        this.token = token;
        this.commandDigest = commandDigest.clone();
        this.locator = locator;
    }

    public StageAssignmentToken token() { return token; }
    public byte[] commandDigest() { return commandDigest.clone(); }
    public PreparedSinkLocator locator() { return locator; }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PreparedSinkReceipt)) return false;
        PreparedSinkReceipt that = (PreparedSinkReceipt) other;
        return token.equals(that.token)
                && MessageDigest.isEqual(commandDigest, that.commandDigest)
                && locator.equals(that.locator);
    }

    @Override public int hashCode() {
        return Objects.hash(token, java.util.Arrays.hashCode(commandDigest), locator);
    }
}
