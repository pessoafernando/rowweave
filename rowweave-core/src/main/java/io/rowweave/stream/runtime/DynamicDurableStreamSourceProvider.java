package io.rowweave.stream.runtime;

import io.rowweave.stream.spi.StreamSourceEnumerator;

/** Worker-local extension for a checkpointed public split enumerator. */
public interface DynamicDurableStreamSourceProvider
        extends DurableStreamSourceProvider {
    StreamSourceEnumerator openEnumerator(
            StreamSourceEnumeratorOpenRequest request) throws Exception;
}
