package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The public bounded event must reach the existing task lifecycle unchanged. */
class PublicBoundedStreamSourceAdapterTest {

    @Test
    void publicEndOfInputBridgesToKernelTerminalEvent() throws Exception {
        StreamSourceProvider provider = new StreamSourceProvider() {
            @Override public String providerKey() { return "bounded-v1"; }
            @Override public String contractDigest() { return repeat('a'); }
            @Override public io.rowweave.stream.spi.StreamSourceReader open(
                    StreamSourceContext context) {
                return new io.rowweave.stream.spi.StreamSourceReader() {
                    @Override public io.rowweave.stream.spi.StreamSourceRecord next() {
                        throw new AssertionError("bounded reader uses nextEvent");
                    }
                    @Override public StreamSourceEvent nextEvent() {
                        return StreamSourceEvent.endOfInput();
                    }
                    @Override public byte[] checkpointPositionFrame() {
                        return new byte[] {1};
                    }
                    @Override public void cancel() { }
                    @Override public void close() { }
                };
            }
        };
        StreamSourceSplit split = new StreamSourceSplit(
                repeat('b'), "position-v1", new byte[] {2});
        byte[] configuration = new byte[] {3};
        StreamSourceBinding binding = new StreamSourceBinding(
                provider.providerKey(), provider.contractDigest(),
                sha256(configuration), configuration,
                Collections.singletonList(split),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
        StreamSourceOpenRequest request = new StreamSourceOpenRequest(
                "source", binding, split, new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, "lane", 1L));

        StreamSourceReader reader = PublicStreamSourceAdapter
                .adapt(provider).open(request);
        assertEquals(io.rowweave.stream.runtime.StreamSourceEvent
                .Kind.END_OF_INPUT, reader.nextEvent().kind());
        reader.close();
    }

    private static String sha256(byte[] value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
        StringBuilder result = new StringBuilder(64);
        for (byte current : digest) {
            result.append(Character.forDigit((current >>> 4) & 0xf, 16));
            result.append(Character.forDigit(current & 0xf, 16));
        }
        return result.toString();
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        java.util.Arrays.fill(result, value);
        return new String(result);
    }
}
