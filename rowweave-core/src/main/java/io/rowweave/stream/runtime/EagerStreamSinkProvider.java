package io.rowweave.stream.runtime;

/** Worker-local provider for an explicitly at-least-once eager Stream Sink. */
public interface EagerStreamSinkProvider {
    String providerKey();
    String contractDigest();
    EagerStreamSinkWriter open(StreamSinkOpenRequest request) throws Exception;
}
