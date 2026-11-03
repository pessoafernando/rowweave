package io.rowweave.stream.spi;

/** Public Worker-local provider for an eager at-least-once Stream Sink. */
public interface StreamSinkProvider {
    String providerKey();
    String contractDigest();
    StreamSinkWriter open(StreamSinkContext context) throws Exception;
}
