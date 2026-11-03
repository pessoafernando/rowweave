package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/** Canonical checksummed codec for one exact S3 object split. */
public final class S3ObjectDescriptorCodec {
    public static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private static final String CODE = "S3_SPLIT_INVALID";
    private static final byte[] MAGIC =
            "RWXS3SP1".getBytes(StandardCharsets.US_ASCII);

    private S3ObjectDescriptorCodec() { }

    public static byte[] encode(S3ObjectDescriptor descriptor) {
        if (descriptor == null) throw invalid();
        return S3CodecSupport.encode(MAGIC, MAX_FRAME_BYTES,
                output -> write(output, descriptor), CODE);
    }

    public static S3ObjectDescriptor decode(byte[] frame) {
        S3ObjectDescriptor result = S3CodecSupport.decode(
                frame, MAGIC, MAX_FRAME_BYTES,
                S3ObjectDescriptorCodec::read, CODE);
        if (!MessageDigest.isEqual(frame, encode(result))) throw invalid();
        return result;
    }

    private static void write(DataOutputStream output,
            S3ObjectDescriptor descriptor) throws IOException {
        S3CodecSupport.writeText(output, descriptor.bucket(), 255, CODE);
        S3CodecSupport.writeText(output, descriptor.objectKey(), 4096, CODE);
        output.writeBoolean(descriptor.versioned());
        if (descriptor.versioned()) {
            S3CodecSupport.writeText(output, descriptor.versionId(), 1024, CODE);
        }
        S3CodecSupport.writeText(output, descriptor.etag(), 1024, CODE);
        output.writeLong(descriptor.contentLength());
        output.write(S3CodecSupport.parseSha256(
                descriptor.contentSha256(), CODE));
        output.writeByte(descriptor.format().ordinal());
        StreamSchema schema = descriptor.schema();
        output.writeInt(schema.fieldCount());
        for (StreamField field : schema.fields()) {
            S3CodecSupport.writeText(output, field.fieldId(), 1024, CODE);
            S3CodecSupport.writeText(output, field.name(), 1024, CODE);
            S3CodecSupport.writeText(output, field.alias(), 1024, CODE);
            output.writeByte(field.dataType().ordinal());
            output.writeBoolean(field.nullable());
        }
        output.write(S3CodecSupport.parseSha256(
                schema.fingerprint(), CODE));
    }

    private static S3ObjectDescriptor read(DataInputStream input)
            throws IOException {
        String bucket = S3CodecSupport.readText(input, 255, CODE);
        String key = S3CodecSupport.readText(input, 4096, CODE);
        boolean versioned = input.readBoolean();
        String version = versioned
                ? S3CodecSupport.readText(input, 1024, CODE) : null;
        String etag = S3CodecSupport.readText(input, 1024, CODE);
        long length = input.readLong();
        byte[] sha = new byte[S3CodecSupport.DIGEST_BYTES];
        input.readFully(sha);
        int format = input.readUnsignedByte();
        int fieldCount = input.readInt();
        if (format >= S3ObjectFormat.values().length
                || fieldCount < 1 || fieldCount > 1024) {
            throw invalid();
        }
        List<StreamField> fields = new ArrayList<>(fieldCount);
        for (int index = 0; index < fieldCount; index++) {
            String fieldId = S3CodecSupport.readText(input, 1024, CODE);
            String name = S3CodecSupport.readText(input, 1024, CODE);
            String alias = S3CodecSupport.readText(input, 1024, CODE);
            int type = input.readUnsignedByte();
            if (type >= DataType.values().length) throw invalid();
            fields.add(new StreamField(fieldId, name, alias,
                    DataType.values()[type], input.readBoolean()));
        }
        byte[] expectedFingerprint = new byte[S3CodecSupport.DIGEST_BYTES];
        input.readFully(expectedFingerprint);
        StreamSchema schema = new StreamSchema(fields);
        if (!MessageDigest.isEqual(expectedFingerprint,
                S3CodecSupport.parseSha256(schema.fingerprint(), CODE))) {
            throw invalid();
        }
        return versioned
                ? S3ObjectDescriptor.versioned(bucket, key, version, etag,
                length, S3CodecSupport.hex(sha),
                S3ObjectFormat.values()[format], schema)
                : S3ObjectDescriptor.unversioned(bucket, key, etag, length,
                S3CodecSupport.hex(sha), S3ObjectFormat.values()[format],
                schema);
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid(CODE);
    }
}
