package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** a remote logical producer owns one bounded, redacted 256-bit grant. */
class ShuffleGrantContractTest {

    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-grant", "attempt-grant", 19L);
    private static final UUID TARGET = new UUID(17L, 29L);

    @Test
    void generatedGrantIsExactly256BitsDistinctAndDefensivelyCopied() {
        SecureRandom random = new SecureRandom();
        ShuffleProducerGrant first = ShuffleProducerGrant.generate(random);
        ShuffleProducerGrant second = ShuffleProducerGrant.generate(random);

        byte[] exposed = first.copyBytes();
        org.junit.jupiter.api.Assertions.assertEquals(32, exposed.length);
        assertFalse(Arrays.equals(exposed, second.copyBytes()));
        byte original = exposed[0];
        exposed[0] ^= 0x7f;
        org.junit.jupiter.api.Assertions.assertEquals(original, first.copyBytes()[0]);

        assertThrows(IllegalArgumentException.class,
                () -> ShuffleProducerGrant.fromBytes(new byte[31]));
    }

    @Test
    void authorizationDigestBindsDomainFenceTargetOrdinalAndRawGrant() {
        byte[] secret = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(secret, (byte) 0x5a);
        ShuffleProducerGrant grant = ShuffleProducerGrant.fromBytes(secret);

        byte[] baseline = grant.authorizationDigest(FENCE, TARGET, 7);
        org.junit.jupiter.api.Assertions.assertEquals(32, baseline.length);
        assertArrayEquals(baseline, grant.authorizationDigest(FENCE, TARGET, 7));
        assertFalse(Arrays.equals(baseline, grant.authorizationDigest(
                new RunAttemptFence("run-grant", "attempt-grant", 20L), TARGET, 7)));
        assertFalse(Arrays.equals(baseline, grant.authorizationDigest(
                FENCE, new UUID(17L, 30L), 7)));
        assertFalse(Arrays.equals(baseline, grant.authorizationDigest(FENCE, TARGET, 8)));
        secret[0] ^= 1;
        assertFalse(Arrays.equals(baseline,
                ShuffleProducerGrant.fromBytes(secret).authorizationDigest(FENCE, TARGET, 7)));
        assertThrows(IllegalArgumentException.class,
                () -> grant.authorizationDigest(FENCE, TARGET, 0));
        assertThrows(IllegalArgumentException.class,
                () -> grant.authorizationDigest(FENCE, TARGET, 4097));
    }

    @Test
    void producerEndpointAndConsumerRecordCarryDisjointSecretMaterial() {
        byte[] secret = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(secret, (byte) 0x2d);
        ShuffleProducerGrant grant = ShuffleProducerGrant.fromBytes(secret);
        WorkerDataIdentity destination = identity("consumer", (byte) 3);
        WorkerDataIdentity source = identity("producer", (byte) 4);
        byte[] cohort = new byte[32];
        Arrays.fill(cohort, (byte) 0x6c);

        ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthorized(
                "127.0.0.1", 7401, TARGET, destination,
                grant, 2, cohort, 2);
        ShuffleAuthorizationRecord record = ShuffleAuthorizationRecord.authorize(
                FENCE, TARGET, 7, source, grant);

        assertArrayEquals(secret, endpoint.producerGrant().copyBytes());
        assertArrayEquals(grant.authorizationDigest(FENCE, TARGET, 7),
                record.authorizationDigest());
        assertNotEquals(Arrays.toString(secret), endpoint.toString());
        assertFalse(endpoint.toString().contains("45, 45"));
        assertFalse(grant.toString().contains("45"));
        assertFalse(record.toString().contains("45"));

        byte[] changedCohort = endpoint.cohortId();
        changedCohort[0] ^= 1;
        assertArrayEquals(cohort, endpoint.cohortId());
        org.junit.jupiter.api.Assertions.assertEquals(2, endpoint.producerOrdinal());
        org.junit.jupiter.api.Assertions.assertEquals(2, endpoint.expectedOrdinalCount());
        assertThrows(IllegalArgumentException.class, () -> ShuffleEndpoint.muxAuthorized(
                "127.0.0.1", 7401, TARGET, destination,
                grant, 0, cohort, 2));
    }

    private static WorkerDataIdentity identity(String workerId, byte fill) {
        byte[] fingerprint = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        Arrays.fill(fingerprint, fill);
        return WorkerDataIdentity.authenticated(workerId, "inc-" + workerId,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-a", "authz-a", fingerprint);
    }
}
