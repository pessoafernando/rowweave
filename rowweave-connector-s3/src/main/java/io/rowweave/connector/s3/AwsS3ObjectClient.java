package io.rowweave.connector.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

final class AwsS3ObjectClient implements S3ObjectClient {
    private final S3Client client;

    private AwsS3ObjectClient(S3Client client) { this.client = client; }

    static AwsS3ObjectClient create(S3ConnectorConfiguration configuration,
            S3Credentials credentials) {
        return new AwsS3ObjectClient(
                AwsS3ClientFactory.create(configuration, credentials, 1));
    }

    @Override public S3ObjectStream open(S3ObjectDescriptor descriptor) {
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(descriptor.bucket()).key(descriptor.objectKey())
                .ifMatch(descriptor.etag()).checksumMode(ChecksumMode.ENABLED);
        if (descriptor.versioned()) request.versionId(descriptor.versionId());
        ResponseInputStream<GetObjectResponse> response =
                client.getObject(request.build());
        GetObjectResponse metadata = response.response();
        Long length = metadata.contentLength();
        if (length == null) {
            try {
                response.close();
            } catch (java.io.IOException ignored) { }
            throw new IllegalStateException("S3_OBJECT_RESPONSE_INVALID");
        }
        return new S3ObjectStream(response, metadata.versionId(),
                metadata.eTag(), length);
    }

    @Override public void close() { client.close(); }
}
