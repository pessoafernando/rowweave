package io.rowweave;

/** Declared keyed timer access; registration and deletion are idempotent. */
public interface StreamTimerService {

    void registerTimer(String timerFamilyId, byte[] namespace,
            long timestamp);

    void deleteTimer(String timerFamilyId, byte[] namespace,
            long timestamp);
}
