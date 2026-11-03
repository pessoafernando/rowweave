package io.rowweave;

import io.rowweave.stream.api.StreamTimerSpec;

/** Context for one deterministic keyed timer callback. */
public interface StreamTimerContext extends StreamProcessContext {

    String timerFamilyId();

    /** Defensive canonical namespace bytes supplied at registration. */
    byte[] timerNamespace();

    StreamTimerSpec.TimeDomain timeDomain();

    long timerTimestamp();
}
