package io.rowweave.mem;

/** Closes several resources in one call: always attempts every cleanup, keeps the original failure as the cause and attaches cleanup failures as suppressed. */
public final class ResourceCloser {

    private ResourceCloser() {
    }

    public static void close(Throwable primary, AutoCloseable... resources) {
        Throwable cleanupFailure = null;
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Throwable failure) {
                cleanupFailure = append(cleanupFailure, failure);
            }
        }
        if (cleanupFailure == null) {
            return;
        }
        if (primary != null) {
            if (primary != cleanupFailure && !alreadySuppressed(primary, cleanupFailure)) {
                primary.addSuppressed(cleanupFailure);
            }
            return;
        }
        rethrow(cleanupFailure);
    }

    private static Throwable append(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next && !alreadySuppressed(first, next)) {
            first.addSuppressed(next);
        }
        return first;
    }

    private static boolean alreadySuppressed(Throwable target, Throwable candidate) {
        for (Throwable suppressed : target.getSuppressed()) {
            if (suppressed == candidate) {
                return true;
            }
        }
        return false;
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
