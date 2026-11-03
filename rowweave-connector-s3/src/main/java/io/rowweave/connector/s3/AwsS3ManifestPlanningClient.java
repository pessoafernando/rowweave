package io.rowweave.connector.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.util.ArrayList;
import java.util.List;

final class AwsS3ManifestPlanningClient implements S3ManifestPlanningClient {
    private final String bucket;
    private final S3Client client;

    AwsS3ManifestPlanningClient(S3ConnectorConfiguration configuration,
            S3Credentials credentials) {
        this.bucket = configuration.bucket();
        this.client = AwsS3ClientFactory.create(configuration, credentials);
    }

    @Override public S3ListingPage list(String prefix,
            String continuationToken, int maximumKeys) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix)
                .continuationToken(continuationToken)
                .maxKeys(maximumKeys).build();
        ListObjectsV2Response response = client.listObjectsV2(request);
        List<String> keys = new ArrayList<>(response.contents().size());
        for (software.amazon.awssdk.services.s3.model.S3Object object
                : response.contents()) {
            keys.add(object.key());
        }
        return new S3ListingPage(keys,
                Boolean.TRUE.equals(response.isTruncated()),
                response.nextContinuationToken());
    }

    @Override public S3ObjectStream openCurrent(String requestedBucket,
            String key) {
        if (!bucket.equals(requestedBucket)) {
            throw new IllegalArgumentException("S3_MANIFEST_LISTING_INVALID");
        }
        ResponseInputStream<GetObjectResponse> response = client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build());
        GetObjectResponse metadata = response.response();
        Long length = metadata.contentLength();
        if (metadata.eTag() == null || length == null) {
            try { response.close(); } catch (java.io.IOException ignored) { }
            throw new IllegalStateException("S3_OBJECT_RESPONSE_INVALID");
        }
        return new S3ObjectStream(response, metadata.versionId(),
                metadata.eTag(), length);
    }

    @Override public void close() { client.close(); }
}
