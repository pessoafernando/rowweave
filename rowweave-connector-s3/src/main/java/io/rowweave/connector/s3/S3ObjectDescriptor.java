package io.rowweave.connector.s3;

import io.rowweave.stream.api.StreamSchema;

/** Immutable exact object snapshot and row-schema binding for one split. */
public final class S3ObjectDescriptor {
    static final long MAX_CONTENT_BYTES =
            S3ConnectorConfiguration.MAX_OBJECT_BYTES;
    private static final String CODE = "S3_SPLIT_INVALID";

    private final String bucket;
    private final String objectKey;
    private final String versionId;
    private final String etag;
    private final long contentLength;
    private final String contentSha256;
    private final S3ObjectFormat format;
    private final StreamSchema schema;

    private S3ObjectDescriptor(String bucket, String objectKey,
            String versionId, String etag, long contentLength,
            String contentSha256, S3ObjectFormat format,
            StreamSchema schema) {
        this.bucket = text(bucket, 255);
        this.objectKey = text(objectKey, 4096);
        this.versionId = versionId == null ? null : text(versionId, 1024);
        this.etag = text(etag, 1024);
        if (contentLength < 0 || contentLength > MAX_CONTENT_BYTES
                || format == null || schema == null) {
            throw invalid();
        }
        S3CodecSupport.parseSha256(contentSha256, CODE);
        requireSchema(schema);
        this.contentLength = contentLength;
        this.contentSha256 = contentSha256;
        this.format = format;
        this.schema = schema;
    }

    public static S3ObjectDescriptor versioned(String bucket,
            String objectKey, String versionId, String etag,
            long contentLength, String contentSha256,
            S3ObjectFormat format, StreamSchema schema) {
        return new S3ObjectDescriptor(bucket, objectKey, text(versionId, 1024),
                etag, contentLength, contentSha256, format, schema);
    }

    public static S3ObjectDescriptor unversioned(String bucket,
            String objectKey, String etag, long contentLength,
            String contentSha256, S3ObjectFormat format,
            StreamSchema schema) {
        return new S3ObjectDescriptor(bucket, objectKey, null, etag,
                contentLength, contentSha256, format, schema);
    }

    private static String text(String value, int maximum) {
        S3CodecSupport.utf8(value, maximum, false, CODE);
        return value;
    }

    private static void requireSchema(StreamSchema schema) {
        S3CodecSupport.parseSha256(schema.fingerprint(), CODE);
        for (io.rowweave.stream.api.StreamField field : schema.fields()) {
            S3CodecSupport.utf8(field.fieldId(), 1024, false, CODE);
            S3CodecSupport.utf8(field.name(), 1024, false, CODE);
            S3CodecSupport.utf8(field.alias(), 1024, false, CODE);
        }
    }

    public String bucket() { return bucket; }
    public String objectKey() { return objectKey; }
    public boolean versioned() { return versionId != null; }
    public String versionId() {
        if (versionId == null) throw new IllegalStateException(
                "S3 object snapshot is unversioned");
        return versionId;
    }
    String optionalVersionId() { return versionId; }
    public String etag() { return etag; }
    public long contentLength() { return contentLength; }
    public String contentSha256() { return contentSha256; }
    public S3ObjectFormat format() { return format; }
    public StreamSchema schema() { return schema; }
    public String schemaFingerprint() { return schema.fingerprint(); }
    public String splitId() {
        return S3CodecSupport.sha256Hex(
                S3ObjectDescriptorCodec.encode(this));
    }

    @Override public String toString() {
        return "S3ObjectDescriptor{split=" + splitId().substring(0, 12)
                + ",bytes=" + contentLength + ",format=" + format + '}';
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid(CODE);
    }
}
