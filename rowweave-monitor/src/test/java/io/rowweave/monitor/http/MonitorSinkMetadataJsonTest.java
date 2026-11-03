package io.rowweave.monitor.http;

import io.rowweave.monitor.BatchSinkView;
import io.rowweave.monitor.JobView;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HTTP monitor exposes only the bounded, redacted Sink summary. */
class MonitorSinkMetadataJsonTest {

    @Test
    void jobDetailCarriesSafeSinkSummaryWithoutDomainOrLocatorPayload() {
        BatchSinkView sink = BatchSinkView.of(
                2,
                Arrays.asList("MANIFEST_ATOMIC_GROUP_V1",
                        "MANIFEST_ATOMIC_GROUP_V1"),
                BatchSinkView.DOMAIN_SAME,
                Arrays.asList("0123456789abcdef", "fedcba9876543210"),
                2,
                "COMMITTED");
        JobView job = JobView.builder("job", "job")
                .batchSink(sink)
                .build();

        String json = MonitorJson.jobDetail(job);

        assertTrue(json.contains("\"sinkCount\":2"), json);
        assertTrue(json.contains(
                "\"capabilityProfiles\":[\"MANIFEST_ATOMIC_GROUP_V1\","), json);
        assertTrue(json.contains("\"commitDomainDisposition\":\"SAME_DOMAIN\""),
                json);
        assertTrue(json.contains(
                "\"descriptorDigestPrefixes\":[\"0123456789abcdef\""), json);
        assertTrue(json.contains("\"locatorClosureCount\":2"), json);
        assertTrue(json.contains("\"outcomeState\":\"COMMITTED\""), json);
        assertFalse(json.contains("/private/manifest-domain"), json);
        assertFalse(json.contains("publicationId"), json);
        assertFalse(json.contains("locatorPayload"), json);
    }
}
