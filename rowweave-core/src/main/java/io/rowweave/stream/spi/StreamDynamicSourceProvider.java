package io.rowweave.stream.spi;

/** Source provider that additionally exposes checkpointed split discovery. */
public interface StreamDynamicSourceProvider extends StreamSourceProvider {
    StreamSourceEnumerator openEnumerator(
            StreamSourceEnumeratorContext context) throws Exception;
}
