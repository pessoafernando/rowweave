package io.rowweave.connector.s3;

import io.rowweave.stream.api.StreamConnectorConfiguration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/** Credential-free, canonical and resource-bounded S3 source configuration. */
public final class S3ConnectorConfiguration {
    public static final String CREDENTIALS_ALIAS = "credentials";
    public static final long MAX_OBJECT_BYTES = 1024L * 1024 * 1024 * 1024;
    public static final int MIN_ROW_BYTES = 1024;
    public static final int MAX_ROW_BYTES = 16 * 1024 * 1024;

    private static final String CODE = "S3_CONFIGURATION_INVALID";
    private static final String PAYLOAD_OPTION = "s3.config";
    private static final byte[] MAGIC = new byte[] {
            'A', 'L', 'X', 'S', '3', 'C', 'F', '1'};
    private static final int MAX_PAYLOAD_BYTES = 16 * 1024;
    private static final Pattern REGION = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9-]{0,62}");
    private static final Pattern BUCKET = Pattern.compile(
            "[A-Za-z0-9][A-Za-z0-9.-]{1,253}[A-Za-z0-9]");

    private final URI endpoint;
    private final String region;
    private final String bucket;
    private final S3ObjectFormat format;
    private final String credentialsReference;
    private final long maximumObjectBytes;
    private final int maximumRowBytes;
    private final long temporaryDiskBudgetBytes;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final int maximumAttempts;
    private final boolean pathStyleAccess;
    private final boolean allowInsecureEndpoint;
    private final String nullLiteral;

    private S3ConnectorConfiguration(Builder builder) {
        this.endpoint = requireEndpoint(builder.endpoint,
                builder.allowInsecureEndpoint);
        this.region = require(builder.region, 63, REGION);
        this.bucket = require(builder.bucket, 255, BUCKET);
        this.format = builder.format;
        this.credentialsReference = require(
                builder.credentialsReference, 512, null);
        this.maximumObjectBytes = builder.maximumObjectBytes;
        this.maximumRowBytes = builder.maximumRowBytes;
        this.temporaryDiskBudgetBytes = builder.temporaryDiskBudgetBytes;
        this.connectTimeoutMillis = builder.connectTimeoutMillis;
        this.readTimeoutMillis = builder.readTimeoutMillis;
        this.maximumAttempts = builder.maximumAttempts;
        this.pathStyleAccess = builder.pathStyleAccess;
        this.allowInsecureEndpoint = builder.allowInsecureEndpoint;
        this.nullLiteral = require(builder.nullLiteral, 128, null);
        if (format == null || maximumObjectBytes < 1
                || maximumObjectBytes > MAX_OBJECT_BYTES
                || maximumRowBytes < MIN_ROW_BYTES
                || maximumRowBytes > MAX_ROW_BYTES
                || temporaryDiskBudgetBytes < maximumObjectBytes
                || temporaryDiskBudgetBytes > MAX_OBJECT_BYTES
                || connectTimeoutMillis < 100
                || connectTimeoutMillis > 120_000
                || readTimeoutMillis < 100
                || readTimeoutMillis > 600_000
                || maximumAttempts < 1 || maximumAttempts > 10) {
            throw invalid();
        }
    }

    public static Builder builder() { return new Builder(); }

    public byte[] encode() {
        byte[] payload = S3CodecSupport.encode(MAGIC, MAX_PAYLOAD_BYTES,
                output -> writePayload(output), CODE);
        StreamConnectorConfiguration envelope =
                StreamConnectorConfiguration.of(
                        Collections.singletonMap(PAYLOAD_OPTION,
                                Base64.getEncoder().encodeToString(payload)),
                        Collections.singletonMap(CREDENTIALS_ALIAS,
                                credentialsReference));
        return envelope.encode();
    }

    public static S3ConnectorConfiguration decode(byte[] frame) {
        final StreamConnectorConfiguration envelope;
        try {
            envelope = StreamConnectorConfiguration.decode(frame);
        } catch (RuntimeException invalid) {
            throw invalid();
        }
        Map<String, String> options = envelope.options();
        Map<String, String> secrets = envelope.secretReferences();
        if (options.size() != 1 || secrets.size() != 1
                || !options.containsKey(PAYLOAD_OPTION)
                || !secrets.containsKey(CREDENTIALS_ALIAS)) {
            throw invalid();
        }
        final byte[] payload;
        try {
            payload = Base64.getDecoder().decode(options.get(PAYLOAD_OPTION));
        } catch (IllegalArgumentException malformed) {
            throw invalid();
        }
        S3ConnectorConfiguration decoded = S3CodecSupport.decode(
                payload, MAGIC, MAX_PAYLOAD_BYTES,
                input -> readPayload(input,
                        secrets.get(CREDENTIALS_ALIAS)), CODE);
        if (!java.security.MessageDigest.isEqual(frame, decoded.encode())) {
            throw invalid();
        }
        return decoded;
    }

    private void writePayload(DataOutputStream output) throws IOException {
        S3CodecSupport.writeText(output, endpoint.toASCIIString(), 2048, CODE);
        S3CodecSupport.writeText(output, region, 63, CODE);
        S3CodecSupport.writeText(output, bucket, 255, CODE);
        output.writeByte(format.ordinal());
        output.writeLong(maximumObjectBytes);
        output.writeInt(maximumRowBytes);
        output.writeLong(temporaryDiskBudgetBytes);
        output.writeInt(connectTimeoutMillis);
        output.writeInt(readTimeoutMillis);
        output.writeInt(maximumAttempts);
        output.writeBoolean(pathStyleAccess);
        output.writeBoolean(allowInsecureEndpoint);
        S3CodecSupport.writeText(output, nullLiteral, 128, CODE);
    }

    private static S3ConnectorConfiguration readPayload(DataInputStream input,
            String credentialsReference) throws IOException {
        String endpoint = S3CodecSupport.readText(input, 2048, CODE);
        String region = S3CodecSupport.readText(input, 63, CODE);
        String bucket = S3CodecSupport.readText(input, 255, CODE);
        int format = input.readUnsignedByte();
        if (format >= S3ObjectFormat.values().length) throw invalid();
        return builder().endpoint(URI.create(endpoint)).region(region)
                .bucket(bucket).format(S3ObjectFormat.values()[format])
                .credentialsReference(credentialsReference)
                .maximumObjectBytes(input.readLong())
                .maximumRowBytes(input.readInt())
                .temporaryDiskBudgetBytes(input.readLong())
                .connectTimeoutMillis(input.readInt())
                .readTimeoutMillis(input.readInt())
                .maximumAttempts(input.readInt())
                .pathStyleAccess(input.readBoolean())
                .allowInsecureEndpoint(input.readBoolean())
                .nullLiteral(S3CodecSupport.readText(input, 128, CODE))
                .build();
    }

    private static URI requireEndpoint(URI endpoint, boolean allowInsecure) {
        if (endpoint == null || !endpoint.isAbsolute()
                || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getQuery() != null || endpoint.getFragment() != null
                || (endpoint.getPath() != null
                && !endpoint.getPath().isEmpty()
                && !"/".equals(endpoint.getPath()))) {
            throw invalid();
        }
        String scheme = endpoint.getScheme();
        if (!("https".equals(scheme)
                || ("http".equals(scheme) && allowInsecure))) {
            throw invalid();
        }
        return endpoint;
    }

    private static String require(String value, int maximum,
            Pattern pattern) {
        S3CodecSupport.utf8(value, maximum, false, CODE);
        if (pattern != null && !pattern.matcher(value).matches()) {
            throw invalid();
        }
        return value;
    }

    public URI endpoint() { return endpoint; }
    public String region() { return region; }
    public String bucket() { return bucket; }
    public S3ObjectFormat format() { return format; }
    public String credentialsReference() { return credentialsReference; }
    public long maximumObjectBytes() { return maximumObjectBytes; }
    public int maximumRowBytes() { return maximumRowBytes; }
    public long temporaryDiskBudgetBytes() {
        return temporaryDiskBudgetBytes;
    }
    public int connectTimeoutMillis() { return connectTimeoutMillis; }
    public int readTimeoutMillis() { return readTimeoutMillis; }
    public int maximumAttempts() { return maximumAttempts; }
    public boolean pathStyleAccess() { return pathStyleAccess; }
    public boolean allowInsecureEndpoint() { return allowInsecureEndpoint; }
    public String nullLiteral() { return nullLiteral; }

    @Override public String toString() {
        return "S3ConnectorConfiguration{format=" + format
                + ",maxObjectBytes=" + maximumObjectBytes
                + ",maxRowBytes=" + maximumRowBytes + '}';
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid(CODE);
    }

    public static final class Builder {
        private URI endpoint;
        private String region;
        private String bucket;
        private S3ObjectFormat format = S3ObjectFormat.NDJSON_UTF8;
        private String credentialsReference;
        private long maximumObjectBytes = 1024L * 1024 * 1024;
        private int maximumRowBytes = 1024 * 1024;
        private long temporaryDiskBudgetBytes = 1024L * 1024 * 1024;
        private int connectTimeoutMillis = 10_000;
        private int readTimeoutMillis = 60_000;
        private int maximumAttempts = 3;
        private boolean pathStyleAccess;
        private boolean allowInsecureEndpoint;
        private String nullLiteral = "\\N";

        private Builder() { }
        public Builder endpoint(URI value) { endpoint = value; return this; }
        public Builder region(String value) { region = value; return this; }
        public Builder bucket(String value) { bucket = value; return this; }
        public Builder format(S3ObjectFormat value) { format = value; return this; }
        public Builder credentialsReference(String value) {
            credentialsReference = value; return this;
        }
        public Builder maximumObjectBytes(long value) {
            maximumObjectBytes = value; return this;
        }
        public Builder maximumRowBytes(int value) {
            maximumRowBytes = value; return this;
        }
        public Builder temporaryDiskBudgetBytes(long value) {
            temporaryDiskBudgetBytes = value; return this;
        }
        public Builder connectTimeoutMillis(int value) {
            connectTimeoutMillis = value; return this;
        }
        public Builder readTimeoutMillis(int value) {
            readTimeoutMillis = value; return this;
        }
        public Builder maximumAttempts(int value) {
            maximumAttempts = value; return this;
        }
        public Builder pathStyleAccess(boolean value) {
            pathStyleAccess = value; return this;
        }
        public Builder allowInsecureEndpoint(boolean value) {
            allowInsecureEndpoint = value; return this;
        }
        public Builder nullLiteral(String value) {
            nullLiteral = value; return this;
        }
        public S3ConnectorConfiguration build() {
            return new S3ConnectorConfiguration(this);
        }
    }
}
