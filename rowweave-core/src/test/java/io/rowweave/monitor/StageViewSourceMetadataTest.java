package io.rowweave.monitor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** source identity is observable without exposing source secrets. */
class StageViewSourceMetadataTest {

    @Test
    void sourceSummaryExposesOnlyCountCapabilityAndBoundedDigestPrefix() {
        StageView source = StageView.builder(3)
                .role("SOURCE")
                .sourceSummary(64, StageView.SOURCE_REPLAY_EXACT,
                        "0123456789abcdef")
                .build();

        assertEquals(64, source.sourceSplitCount());
        assertEquals(StageView.SOURCE_REPLAY_EXACT, source.sourceReplayMode());
        assertEquals("0123456789abcdef",
                source.sourceDescriptorDigestPrefix());
    }

    @Test
    void nonSourceDefaultsEmptyAndMalformedSummaryFailsClosed() {
        StageView stage = StageView.builder(1).build();
        assertEquals(0, stage.sourceSplitCount());
        assertNull(stage.sourceReplayMode());
        assertNull(stage.sourceDescriptorDigestPrefix());

        assertThrows(IllegalArgumentException.class, () -> StageView.builder(1)
                .sourceSummary(0, StageView.SOURCE_REPLAY_EXACT,
                        "0123456789abcdef"));
        assertThrows(IllegalArgumentException.class, () -> StageView.builder(1)
                .sourceSummary(1, "snapshot-id-is-secret",
                        "0123456789abcdef"));
        assertThrows(IllegalArgumentException.class, () -> StageView.builder(1)
                .sourceSummary(1, StageView.SOURCE_REPLAY_EXACT,
                        "not-a-safe-prefix"));
    }
}
