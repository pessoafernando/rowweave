package io.rowweave.connector.s3;

interface S3ManifestPlanningClient extends AutoCloseable {
    S3ListingPage list(String prefix, String continuationToken,
            int maximumKeys) throws Exception;
    S3ObjectStream openCurrent(String bucket, String key) throws Exception;
    @Override void close() throws Exception;
}
