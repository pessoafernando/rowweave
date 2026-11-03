package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** versioned submit timeout extension is atomic and legacy-readable. */
class EphemeralSubmitRpcContractTest {

    private static final String SET_A = repeat('a', 64);
    private static final String SET_B = repeat('b', 64);

    @Test
    void legacyAndVersionedEphemeralSubmitFramesBothDecode() throws Exception {
        byte[] plan = {1, 2, 3};
        EphemeralSubmitTimeoutPolicy.Budget budget =
                EphemeralSubmitTimeoutPolicy.Budget.of(12_000L, 3_000L);

        RpcMessage legacyJob = roundTrip(RpcMessage.submitJob(plan, "attempt-1"));
        assertNull(legacyJob.ephemeralSubmitTimeouts());

        RpcMessage currentJob = roundTrip(
                RpcMessage.submitJob(plan, "attempt-1", budget));
        assertArrayEquals(plan, currentJob.planGraphBytes());
        assertBudget(currentJob.ephemeralSubmitTimeouts(), 12_000L, 3_000L);

        RpcMessage legacyArtifact = roundTrip(
                RpcMessage.submitPlanArtifact(plan, "publication-1"));
        assertNull(legacyArtifact.ephemeralSubmitTimeouts());

        RpcMessage currentArtifact = roundTrip(
                RpcMessage.submitPlanArtifact(plan, "publication-1", budget));
        assertBudget(currentArtifact.ephemeralSubmitTimeouts(), 12_000L, 3_000L);
    }

    @Test
    void timeoutExtensionPrecedesAndCoexistsWithSecureSuffix() throws Exception {
        EphemeralSubmitTimeoutPolicy.Budget budget =
                EphemeralSubmitTimeoutPolicy.Budget.of(8_000L, 2_000L);
        RpcSecurityMetadata security = RpcSecurityMetadata.of(SET_A, SET_B);
        RpcMessage decoded = roundTrip(RpcMessage.submitPlanArtifact(
                new byte[]{9}, "publication-secure", budget).withSecurityMetadata(security));

        assertBudget(decoded.ephemeralSubmitTimeouts(), 8_000L, 2_000L);
        assertEquals(SET_A, decoded.securityMetadata().deploymentSetId());
        assertEquals(SET_B, decoded.securityMetadata().authzSetId());
    }

    @Test
    void oldTailRuleWouldRejectNewPlainAndSecureFramesInsteadOfIgnoringBudget() {
        EphemeralSubmitTimeoutPolicy.Budget budget =
                EphemeralSubmitTimeoutPolicy.Budget.of(8_000L, 2_000L);
        RpcMessage legacy = RpcMessage.submitJob(new byte[]{1}, "attempt");
        RpcMessage current = RpcMessage.submitJob(new byte[]{1}, "attempt", budget);

        byte[] legacyPlain = RpcCodec.encode(legacy);
        byte[] currentPlain = RpcCodec.encode(current);
        assertEquals(RpcCodec.SUBMIT_TIMEOUT_EXTENSION_BYTES,
                currentPlain.length - legacyPlain.length);
        assertNotEquals(RpcCodec.SECURE_SUFFIX_BYTES,
                currentPlain.length - legacyPlain.length,
                "pre-0204 decoder treats any tail as a secure suffix and must reject this length");

        RpcSecurityMetadata security = RpcSecurityMetadata.of(SET_A, SET_B);
        byte[] legacySecure = RpcCodec.encode(legacy.withSecurityMetadata(security));
        byte[] currentSecure = RpcCodec.encode(current.withSecurityMetadata(security));
        assertEquals(RpcCodec.SUBMIT_TIMEOUT_EXTENSION_BYTES,
                currentSecure.length - legacySecure.length);
        assertNotEquals(RpcCodec.SECURE_SUFFIX_BYTES,
                currentSecure.length - legacyPlain.length,
                "extension plus secure suffix is not a legacy secure suffix");
    }

    @Test
    void rejectsTruncatedUnknownOrMisplacedTimeoutExtension() {
        EphemeralSubmitTimeoutPolicy.Budget budget =
                EphemeralSubmitTimeoutPolicy.Budget.of(8_000L, 2_000L);
        byte[] encoded = RpcCodec.encode(
                RpcMessage.submitJob(new byte[]{1}, "attempt", budget));
        int extensionStart = encoded.length - RpcCodec.SUBMIT_TIMEOUT_EXTENSION_BYTES;

        byte[] unknownMagic = encoded.clone();
        unknownMagic[extensionStart] ^= 1;
        assertThrows(IOException.class, () -> decode(unknownMagic));

        byte[] unknownVersion = encoded.clone();
        ByteBuffer.wrap(unknownVersion,
                extensionStart + RpcCodec.SUBMIT_TIMEOUT_MAGIC_BYTES, Integer.BYTES).putInt(99);
        assertThrows(IOException.class, () -> decode(unknownVersion));

        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        ByteBuffer.wrap(truncated).putInt(ByteBuffer.wrap(encoded).getInt() - 1);
        assertThrows(IOException.class, () -> decode(truncated));

        assertThrows(IllegalArgumentException.class, () -> RpcCodec.encode(
                RpcMessage.ack().withEphemeralSubmitTimeouts(budget)));
    }

    @Test
    void durableSubmitNeverCarriesEphemeralBudget() throws Exception {
        RpcMessage durable = roundTrip(RpcMessage.submitRun(new byte[]{7, 8, 9}));
        assertNull(durable.ephemeralSubmitTimeouts());
    }

    private static RpcMessage roundTrip(RpcMessage message) throws IOException {
        return decode(RpcCodec.encode(message));
    }

    private static RpcMessage decode(byte[] frame) throws IOException {
        return RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(frame)));
    }

    private static void assertBudget(EphemeralSubmitTimeoutPolicy.Budget budget,
                                     long execution, long cleanup) {
        assertEquals(execution, budget.executionTimeoutMs());
        assertEquals(cleanup, budget.cleanupTimeoutMs());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
