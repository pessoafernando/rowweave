package io.rowweave.connector.s3;

import io.rowweave.DataRow;

interface S3RecordDecoder extends AutoCloseable {
    DataRow next() throws Exception;
    @Override void close() throws Exception;
}
