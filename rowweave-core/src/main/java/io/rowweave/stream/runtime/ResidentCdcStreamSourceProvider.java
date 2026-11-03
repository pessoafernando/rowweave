package io.rowweave.stream.runtime;

import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceReader;

/** Internal adapter marker for the public engine-owned CDC enumerator SPI. */
public interface ResidentCdcStreamSourceProvider
        extends DurableStreamSourceProvider {
    StreamCdcSourceReader openCdc(StreamCdcSourceContext context)
            throws Exception;
}
