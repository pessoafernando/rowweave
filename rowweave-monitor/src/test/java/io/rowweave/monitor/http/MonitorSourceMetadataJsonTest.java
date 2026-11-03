package io.rowweave.monitor.http;

import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StageView;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HTTP monitor serializes only the safe source summary. */
class MonitorSourceMetadataJsonTest {

    @Test
    void jobDetailCarriesSafeSourceSummaryWithoutIdentityPayload() {
        StageView source = StageView.builder(0)
                .role("SOURCE")
                .sourceSummary(2, StageView.SOURCE_REPLAY_EXACT,
                        "0123456789abcdef")
                .build();
        JobView job = JobView.builder("job", "job").addStage(source).build();

        String json = MonitorJson.jobDetail(job);

        assertTrue(json.contains("\"sourceSplitCount\":2"), json);
        assertTrue(json.contains(
                "\"sourceReplayMode\":\"EXACT_ORDERED_FROM_START\""), json);
        assertTrue(json.contains(
                "\"sourceDescriptorDigestPrefix\":\"0123456789abcdef\""), json);
        assertFalse(json.contains("splitId"), json);
        assertFalse(json.contains("snapshotId"), json);
    }
}
