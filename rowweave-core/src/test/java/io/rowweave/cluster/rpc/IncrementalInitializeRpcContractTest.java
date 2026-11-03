package io.rowweave.cluster.rpc;

import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalInitializeRequestCodec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** only a client may bootstrap one bounded feed authority. */
class IncrementalInitializeRpcContractTest {

    @Test
    void initializeRequestAppendsOneOrdinalAndRoundTripsOnTheNormalLane()
            throws Exception {
        byte[] frame = IncrementalInitializeRequestCodec.encode(request());
        RpcMessage message = RpcMessage.initializeIncrementalFeed(
                "feed-init", "initialize-7", frame);

        RpcMessage decoded = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(message))));

        assertEquals(45, RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED.ordinal());
        assertEquals(46, RpcMessage.Type.RENEW_PREPARED_OUTPUT.ordinal());
        assertEquals(47, RpcMessage.Type.values().length);
        assertEquals(RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED,
                decoded.type());
        assertEquals("feed-init", decoded.runId());
        assertEquals("initialize-7", decoded.attemptId());
        assertArrayEquals(frame, decoded.incrementalInitializeBytes());
        assertTrue(RpcRequestPolicy.MASTER.allows(decoded.type()));
        assertTrue(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.CLIENT),
                decoded.type()));
        assertFalse(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.WORKER),
                decoded.type()));
    }

    private static IncrementalInitializeRequest request() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return new IncrementalInitializeRequest(
                "feed-init", 7L,
                digest.digest("definition".getBytes("UTF-8")),
                digest.digest("source".getBytes("UTF-8")),
                digest.digest("frontier".getBytes("UTF-8")),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }
}
