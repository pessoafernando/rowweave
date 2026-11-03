package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamConnectorProviderContractCodecTest {
    @Test
    void sourceAndSinkContractsRoundTripCanonically() {
        StreamConnectorProviderContract source =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.source", "example.config.v1",
                        4096, 4, 30_000L, true, false, false);
        StreamConnectorProviderContract sink =
                StreamConnectorProviderContract.sink(
                        "signed-stream", "example.sink", "example.config.v1",
                        4096, 4, 30_000L,
                        StreamConnectorProviderContract.SinkVisibility
                                .CHECKPOINT_TRANSACTIONAL);

        for (StreamConnectorProviderContract contract :
                Arrays.asList(source, sink)) {
            byte[] encoded = StreamConnectorProviderContractCodec.encode(contract);
            assertEquals(contract,
                    StreamConnectorProviderContractCodec.decode(encoded));
            assertArrayEquals(encoded, StreamConnectorProviderContractCodec.encode(
                    StreamConnectorProviderContractCodec.decode(encoded)));
            assertEquals(64,
                    StreamConnectorProviderContractCodec.digestHex(contract).length());
        }
    }

    @Test
    void rejectsAmbiguousOrUnboundedFramesAndCapabilities() {
        StreamConnectorProviderContract source =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.source", "example.config.v1",
                        4096, 4, 30_000L, true, false, false);
        byte[] encoded = StreamConnectorProviderContractCodec.encode(source);
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorProviderContractCodec.decode(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorProviderContractCodec.decode(
                        new byte[StreamConnectorProviderContractCodec
                                .MAX_FRAME_BYTES + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorProviderContract.source(
                        "signed-stream", "example.source", "example.config.v1",
                        0, 4, 30_000L, true, false, false));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorProviderContract.sink(
                        "signed-stream", "example.sink", "example.config.v1",
                        4096, 4, 30_000L, null));
    }

    @Test
    void externalCheckpointAcknowledgementIsExplicitAndV1DefaultsOff() {
        StreamConnectorProviderContract legacy =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.legacy-cdc",
                        "example.config.v1", 4096, 1, 30_000L,
                        false, false, true);
        StreamConnectorProviderContract acknowledged =
                StreamConnectorProviderContract.source(
                        "signed-stream", "example.cdc-v2",
                        "example.config.v1", 4096, 1, 30_000L,
                        false, false, true, true);

        assertFalse(StreamConnectorProviderContractCodec.decode(
                StreamConnectorProviderContractCodec.encode(legacy))
                .externalCheckpointAcknowledgement());
        assertTrue(StreamConnectorProviderContractCodec.decode(
                StreamConnectorProviderContractCodec.encode(acknowledged))
                .externalCheckpointAcknowledgement());
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorProviderContract.source(
                        "signed-stream", "example.invalid",
                        "example.config.v1", 4096, 1, 30_000L,
                        false, false, false, true));
    }
}
