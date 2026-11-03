package io.rowweave.stream.runtime;

/** Worker-local provider for a replayable resident Stream source. */
public interface DurableStreamSourceProvider {
    String providerKey();
    String contractDigest();
    StreamSourceReader open(StreamSourceOpenRequest request) throws Exception;
}
