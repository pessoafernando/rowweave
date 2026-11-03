package io.rowweave.stream.spi;

/** Public Worker-local provider for an exact-replay Stream source. */
public interface StreamSourceProvider {
    String providerKey();
    String contractDigest();
    StreamSourceReader open(StreamSourceContext context) throws Exception;
}
