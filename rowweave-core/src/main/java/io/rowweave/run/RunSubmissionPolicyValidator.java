package io.rowweave.run;

/** The wire model can express policies a future runtime would run; the current durable runtime accepts only the narrow set it actually executes. */
public final class RunSubmissionPolicyValidator {

    private RunSubmissionPolicyValidator() {
    }

    public static void requireSupported(RunSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException("run submission is required");
        }
        requireSupported(submission.maxAttempts(), submission.deadlineEpochMillis());
    }

    public static boolean isSupported(RunSnapshot snapshot) {
        return snapshot != null
                && isSupported(snapshot.maxAttempts(), snapshot.deadlineEpochMillis());
    }

    public static boolean isSupported(int maxAttempts, long deadlineEpochMillis) {
        return WholeAttemptRetryPolicy.supports(maxAttempts)
                && deadlineEpochMillis >= 0L;
    }

    public static void requireSupported(int maxAttempts, long deadlineEpochMillis) {
        if (!isSupported(maxAttempts, deadlineEpochMillis)) {
            throw new IllegalArgumentException(
                    "unsupported durable run policy: maxAttempts=" + maxAttempts
                            + " (supported=1.." + WholeAttemptRetryPolicy.MAX_ATTEMPTS
                            + "), deadlineEpochMillis=" + deadlineEpochMillis
                            + " (supported>=0)");
        }
    }
}
