package io.rowweave.connector.s3;

import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Production signed S3-compatible bounded snapshot source provider. */
public final class S3ConnectorProvider implements StreamSourceProvider {
    public S3ConnectorProvider() { }

    @Override public String providerKey() {
        return S3ConnectorContract.PROVIDER_KEY;
    }

    @Override public String contractDigest() {
        return S3ConnectorContract.contractDigest();
    }

    @Override public StreamSourceReader open(StreamSourceContext context)
            throws Exception {
        if (context == null
                || !S3SourcePositionCodec.CODEC_ID.equals(
                context.positionCodecId())) {
            throw new IllegalArgumentException("S3_SOURCE_CONTEXT_INVALID");
        }
        S3ConnectorConfiguration configuration =
                S3ConnectorConfiguration.decode(
                        context.configurationFrame());
        S3ObjectDescriptor descriptor = S3ObjectDescriptorCodec.decode(
                context.splitDescriptorFrame());
        if (!context.splitId().equals(descriptor.splitId())
                || !configuration.bucket().equals(descriptor.bucket())
                || configuration.format() != descriptor.format()
                || descriptor.contentLength()
                > configuration.maximumObjectBytes()
                || descriptor.contentLength()
                > configuration.temporaryDiskBudgetBytes()) {
            throw new IllegalArgumentException("S3_SOURCE_CONTEXT_INVALID");
        }
        final S3SourcePosition restored;
        if (context.hasRestoredPosition()) {
            restored = S3SourcePositionCodec.decode(
                    context.restoredPositionFrame());
            if (!descriptor.splitId().equals(restored.splitId())
                    || restored.completedRecords()
                    != context.restoredLogicalPosition()) {
                throw new IllegalArgumentException(
                        "S3_SOURCE_CONTEXT_INVALID");
            }
        } else {
            restored = null;
        }
        Path temporaryDirectory = Paths.get(
                System.getProperty("java.io.tmpdir"));
        return context.secrets().withSecret(
                S3ConnectorConfiguration.CREDENTIALS_ALIAS, secret -> {
                    S3Credentials credentials =
                            S3CredentialCodec.decode(secret);
                    AwsS3ObjectClient client = null;
                    try {
                        client = AwsS3ObjectClient.create(
                                configuration, credentials);
                        return new S3SnapshotReader(configuration, descriptor,
                                client, restored, temporaryDirectory);
                    } catch (Throwable failure) {
                        if (client != null) {
                            try { client.close(); } catch (Throwable close) {
                                failure.addSuppressed(close);
                            }
                        }
                        throw failure;
                    } finally {
                        credentials.destroy();
                    }
                });
    }
}
