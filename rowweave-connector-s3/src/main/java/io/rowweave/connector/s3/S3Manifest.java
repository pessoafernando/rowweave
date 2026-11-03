package io.rowweave.connector.s3;

import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Canonical fixed object set converted into public bounded Stream splits. */
public final class S3Manifest {
    /** Matches the portable fixed-source split envelope. */
    public static final int MAX_OBJECTS = 4_096;
    private static final String CODE = "S3_MANIFEST_INVALID";

    private final StreamSchema schema;
    private final List<S3ObjectDescriptor> objects;
    private final String digest;

    private S3Manifest(StreamSchema schema,
            List<S3ObjectDescriptor> objects) {
        if (schema == null || objects == null || objects.isEmpty()
                || objects.size() > MAX_OBJECTS) {
            throw invalid();
        }
        List<S3ObjectDescriptor> copy = new ArrayList<>(objects.size());
        Set<String> ids = new HashSet<>();
        for (S3ObjectDescriptor object : objects) {
            if (object == null || !schema.equals(object.schema())
                    || !ids.add(object.splitId())) {
                throw invalid();
            }
            copy.add(object);
        }
        copy.sort(Comparator.comparing(S3ObjectDescriptor::objectKey)
                .thenComparing(value -> value.optionalVersionId() == null
                        ? "" : value.optionalVersionId())
                .thenComparing(S3ObjectDescriptor::splitId));
        this.schema = schema;
        this.objects = Collections.unmodifiableList(copy);
        this.digest = digest(copy);
    }

    public static S3Manifest of(StreamSchema schema,
            List<S3ObjectDescriptor> objects) {
        return new S3Manifest(schema, objects);
    }

    public StreamSourceSpec toSourceSpec(
            S3ConnectorConfiguration configuration) {
        if (configuration == null) throw invalid();
        List<StreamSplitSpec> splits = new ArrayList<>(objects.size());
        for (S3ObjectDescriptor object : objects) {
            if (!configuration.bucket().equals(object.bucket())
                    || configuration.format() != object.format()
                    || object.contentLength()
                    > configuration.maximumObjectBytes()) {
                throw invalid();
            }
            byte[] descriptor = S3ObjectDescriptorCodec.encode(object);
            splits.add(new StreamSplitSpec(object.splitId(),
                    S3SourcePositionCodec.CODEC_ID, descriptor));
        }
        return StreamSourceSpec.boundedExactOrdered(schema,
                S3ConnectorContract.PROVIDER_KEY,
                S3ConnectorContract.contractDigest(),
                configuration.encode(), splits);
    }

    public StreamSchema schema() { return schema; }
    public List<S3ObjectDescriptor> objects() { return objects; }
    public String manifestDigest() { return digest; }

    private static String digest(List<S3ObjectDescriptor> objects) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(objects.size());
                for (S3ObjectDescriptor object : objects) {
                    byte[] frame = S3ObjectDescriptorCodec.encode(object);
                    output.writeInt(frame.length);
                    output.write(frame);
                }
            }
            return S3CodecSupport.sha256Hex(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid(CODE);
    }
}
