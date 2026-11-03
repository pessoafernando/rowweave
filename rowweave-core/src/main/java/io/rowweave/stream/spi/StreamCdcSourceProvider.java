package io.rowweave.stream.spi;

/** Public Worker-local provider for an exact resident snapshot-to-log source. */
public interface StreamCdcSourceProvider extends StreamSourceProvider {
    StreamCdcSourceReader openCdc(StreamCdcSourceContext context)
            throws Exception;

    @Override
    default StreamSourceReader open(StreamSourceContext context) {
        throw new UnsupportedOperationException(
                "resident CDC providers require the CDC source runtime");
    }
}
