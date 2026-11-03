package io.rowweave.run;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Successor attempt, publication and command identity shared by the normal and recovery paths. */
public final class RetryAttemptIdentity {
    private static final String ATTEMPT_PREFIX = "retry-attempt-v1-";
    private static final String PUBLICATION_PREFIX = "retry-publication-v1-";
    private static final String COMMAND_PREFIX = "retry-command-v1-";

    private final String runId;
    private final String artifactId;
    private final int attemptNumber;
    private final String suffix;

    private RetryAttemptIdentity(
            String runId, String artifactId, int attemptNumber, String suffix) {
        this.runId = runId;
        this.artifactId = artifactId;
        this.attemptNumber = attemptNumber;
        this.suffix = suffix;
    }

    public static RetryAttemptIdentity derive(
            String runId, String artifactId, int attemptNumber) {
        runId = RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
        artifactId = RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (attemptNumber < 2 || attemptNumber > WholeAttemptRetryPolicy.MAX_ATTEMPTS) {
            throw new IllegalArgumentException("retry attempt number must be in [2,3]");
        }
        String identity = "retry-attempt-v1" + '\0' + runId + '\0'
                + artifactId + '\0' + attemptNumber;
        return new RetryAttemptIdentity(
                runId, artifactId, attemptNumber, sha256Hex(identity));
    }

    public String runId() { return runId; }
    public String artifactId() { return artifactId; }
    public int attemptNumber() { return attemptNumber; }
    public long fenceToken() { return attemptNumber; }
    public String attemptId() { return ATTEMPT_PREFIX + suffix; }
    public String publicationId() { return PUBLICATION_PREFIX + suffix; }

    public String commandId(String phase) {
        phase = RunText.required(phase, "phase", RunText.MAX_ID_BYTES);
        return COMMAND_PREFIX + sha256Hex(runId + '\0' + artifactId + '\0'
                + attemptNumber + '\0' + phase);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RetryAttemptIdentity)) return false;
        RetryAttemptIdentity that = (RetryAttemptIdentity) other;
        return attemptNumber == that.attemptNumber
                && runId.equals(that.runId)
                && artifactId.equals(that.artifactId)
                && suffix.equals(that.suffix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runId, artifactId, attemptNumber, suffix);
    }

    private static String sha256Hex(String value) {
        try {
            return RunCommandCanonical.hex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
