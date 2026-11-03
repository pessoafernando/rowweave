package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** A hand-built provider map must never attest a signed connector set. */
class StreamSignedWorkerBootstrapTest {
    @Test
    void manualWorkerConstructorCannotAdvertiseSignedConnectorCapability() {
        StreamWorkerCapabilityProfile profile =
                new StreamWorkerCapabilityProfile(
                        "worker", "incarnation", 2, 1,
                        Arrays.asList(
                                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1,
                                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                                        + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                        1, 4, 4096, 1024 * 1024L, 8);
        assertThrows(IllegalArgumentException.class,
                () -> new StreamRemoteWorkerServer(profile,
                        Collections.emptyMap(), Collections.emptyMap()));
    }
}
