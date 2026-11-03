package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceEnumeratorContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Public dynamic providers are bridged without exposing runtime internals. */
class PublicDynamicStreamSourceAdapterTest {

    @Test
    void dynamicPublicReaderCannotEmitBoundedTerminalEvent()
            throws Exception {
        EndingProvider provider = new EndingProvider();
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = new StreamSourceSplit(
                repeat('b'), "position-v1", new byte[] {3});
        io.rowweave.stream.runtime.StreamSourceReader reader =
                PublicStreamSourceAdapter.adapt(provider).open(
                        new StreamSourceOpenRequest(
                                "source", binding, split,
                                new StreamChannelIdentity(
                                        "run", "attempt", 1L, 1L,
                                        "lane", 1L)));

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, reader::nextEvent);
        assertTrue(failure.getMessage().contains("undeclared END_OF_INPUT"));
        reader.close();
    }

    @Test
    void dynamicProviderOpensBoundedEnumeratorFromInertContext()
            throws Exception {
        CapturingProvider provider = new CapturingProvider();
        DurableStreamSourceProvider adapted =
                PublicStreamSourceAdapter.adapt(provider);
        assertTrue(adapted instanceof DynamicDurableStreamSourceProvider);
        StreamSourceBinding binding = binding();

        StreamSourceEnumerator enumerator =
                ((DynamicDurableStreamSourceProvider) adapted)
                        .openEnumerator(new StreamSourceEnumeratorOpenRequest(
                                "source", binding));
        StreamSplitDiscovery result = enumerator.discover(
                StreamDiscoveryCursor.initial(), 2);

        assertEquals(3L, result.discoveryEpoch());
        assertEquals("source", provider.context.operatorUid());
        assertArrayEquals(new byte[] {1, 2},
                provider.context.configurationFrame());
        assertEquals(2, provider.lastLimit);
        assertThrows(IllegalArgumentException.class,
                () -> new StreamSourceEnumeratorOpenRequest(
                        "source", fixedBinding()));
    }

    private static StreamSourceBinding binding() {
        byte[] configuration = new byte[] {1, 2};
        return new StreamSourceBinding("dynamic", repeat('a'),
                sha256(configuration), configuration,
                new StreamSourceDiscoveryDescriptor(
                        10L, 100L, 4, 4, 2, 16, 16),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamSourceBinding fixedBinding() {
        byte[] configuration = new byte[] {1};
        return new StreamSourceBinding("fixed", repeat('a'),
                sha256(configuration), configuration,
                Collections.singletonList(
                        new io.rowweave.stream.contract.StreamSourceSplit(
                                repeat('b'), "position-v1", new byte[] {2})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static final class CapturingProvider
            implements StreamDynamicSourceProvider {
        private StreamSourceEnumeratorContext context;
        private int lastLimit;

        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorContext context) {
            this.context = context;
            return new StreamSourceEnumerator() {
                @Override public StreamSplitDiscovery discover(
                        StreamDiscoveryCursor cursor, int limit) {
                    lastLimit = limit;
                    return new StreamSplitDiscovery(3L, cursor,
                            Collections.emptyList());
                }
                @Override public void close() { }
            };
        }
    }

    private static final class EndingProvider
            implements StreamDynamicSourceProvider {
        @Override public String providerKey() { return "dynamic"; }
        @Override public String contractDigest() { return repeat('a'); }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            return new StreamSourceReader() {
                @Override public io.rowweave.stream.spi.StreamSourceRecord
                        next() {
                    return null;
                }
                @Override public StreamSourceEvent nextEvent() {
                    return StreamSourceEvent.endOfInput();
                }
                @Override public byte[] checkpointPositionFrame() {
                    return new byte[] {0};
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = java.security.MessageDigest
                    .getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        java.util.Arrays.fill(result, value);
        return new String(result);
    }
}
