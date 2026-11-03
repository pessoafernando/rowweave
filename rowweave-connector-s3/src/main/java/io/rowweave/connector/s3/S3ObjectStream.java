package io.rowweave.connector.s3;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

final class S3ObjectStream implements AutoCloseable {
    private final InputStream input;
    private final String versionId;
    private final String etag;
    private final long contentLength;
    private final AtomicBoolean closed = new AtomicBoolean();

    S3ObjectStream(InputStream input, String versionId, String etag,
            long contentLength) {
        if (input == null || etag == null || contentLength < 0) {
            throw new IllegalArgumentException("S3_OBJECT_RESPONSE_INVALID");
        }
        this.input = input;
        this.versionId = versionId;
        this.etag = etag;
        this.contentLength = contentLength;
    }

    InputStream input() { return input; }
    String versionId() { return versionId; }
    String etag() { return etag; }
    long contentLength() { return contentLength; }

    @Override public void close() throws Exception {
        if (closed.compareAndSet(false, true)) input.close();
    }
}
