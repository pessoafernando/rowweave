package io.rowweave.stream.runtime;

import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSecretAccess;

/** Worker-local bridge from the public eager Sink SDK to the Stream kernel. */
public final class PublicStreamSinkAdapter {
    private PublicStreamSinkAdapter() { }

    public static EagerStreamSinkProvider adapt(StreamSinkProvider provider) {
        return adapt(provider, StreamSecretAccess.unavailable());
    }

    public static EagerStreamSinkProvider adapt(
            StreamSinkProvider provider, StreamSecretAccess secrets) {
        if (provider == null || empty(provider.providerKey())
                || empty(provider.contractDigest()) || secrets == null) {
            throw new IllegalArgumentException(
                    "public Stream Sink provider is invalid");
        }
        return new EagerStreamSinkProvider() {
            @Override public String providerKey() {
                return provider.providerKey();
            }

            @Override public String contractDigest() {
                return provider.contractDigest();
            }

            @Override public EagerStreamSinkWriter open(
                    StreamSinkOpenRequest request) {
                StreamSinkWriter writer;
                try {
                    writer = provider.open(new StreamSinkContext(
                            request.sinkOperatorUid(),
                            request.binding().configurationFrame(),
                            secrets));
                } catch (Exception failure) {
                    throw providerFailure("open", failure);
                }
                if (writer == null) {
                    throw new IllegalArgumentException(
                            "public Stream Sink returned no writer");
                }
                return new EagerStreamSinkWriter() {
                    @Override public void write(byte[] frame) throws Exception {
                        writer.write(frame.clone());
                    }
                    @Override public void cancel() { writer.cancel(); }
                    @Override public void close() throws Exception {
                        writer.close();
                    }
                };
            }
        };
    }

    private static IllegalStateException providerFailure(
            String action, Exception failure) {
        return new IllegalStateException(
                "public Stream Sink " + action + " failed", failure);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }
}
