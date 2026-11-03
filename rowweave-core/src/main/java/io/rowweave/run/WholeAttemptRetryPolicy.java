package io.rowweave.run;

/** Hard ceilings on v1 attempts and backoff, consistent across Masters and not changeable at run time. */
public final class WholeAttemptRetryPolicy {
    public static final int MAX_ATTEMPTS = 3;
    private static final long SECOND_ATTEMPT_DELAY_MILLIS = 1_000L;
    private static final long THIRD_ATTEMPT_DELAY_MILLIS = 5_000L;

    private WholeAttemptRetryPolicy() {
    }

    public static boolean supports(int maxAttempts) {
        return maxAttempts >= 1 && maxAttempts <= MAX_ATTEMPTS;
    }

    public static long delayAfterAttempt(int attemptNumber) {
        if (attemptNumber == 1) return SECOND_ATTEMPT_DELAY_MILLIS;
        if (attemptNumber == 2) return THIRD_ATTEMPT_DELAY_MILLIS;
        throw new IllegalArgumentException(
                "retry delay requires a prior attempt in [1,2]");
    }

    public static long notBefore(int attemptNumber, long scheduledAtMillis) {
        if (scheduledAtMillis <= 0L) {
            throw new IllegalArgumentException("retry schedule time must be > 0");
        }
        try {
            return Math.addExact(scheduledAtMillis, delayAfterAttempt(attemptNumber));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("retry not-before overflows", overflow);
        }
    }
}
