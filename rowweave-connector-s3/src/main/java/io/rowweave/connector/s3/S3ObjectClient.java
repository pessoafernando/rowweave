package io.rowweave.connector.s3;

interface S3ObjectClient extends AutoCloseable {
    S3ObjectStream open(S3ObjectDescriptor descriptor) throws Exception;
    @Override void close() throws Exception;
}
