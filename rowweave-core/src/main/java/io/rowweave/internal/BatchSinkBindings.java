package io.rowweave.internal;

import io.rowweave.BatchSinkBinding;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSink;
import io.rowweave.DiscardSink;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;

import java.util.Base64;
import java.util.Collections;
import java.util.List;

/** Canonical SINK binding resolver shared by Plan construction and old-wire downgrade. */
public final class BatchSinkBindings {

    public static final int MAX_TOTAL_DESCRIPTOR_BYTES = 4 * 1024 * 1024;
    private static final String LEGACY_CONNECTOR = "legacy-output";

    private BatchSinkBindings() {
    }

    public static BatchSinkBinding freezeNew(DataSchema declaredMeta, Sink output) {
        BatchSinkDescriptor descriptor = output instanceof BatchSink
                ? ((BatchSink) output).batchSinkDescriptor()
                : legacyDescriptor(declaredMeta, output);
        if (descriptor == null) {
            throw new IllegalArgumentException("batch sink descriptor is required");
        }
        BatchSinkBinding binding = validated(declaredMeta,
                new BatchSinkBinding(output, descriptor));
        validateCurrent(binding);
        return binding;
    }

    public static BatchSinkBinding decodeFrozen(
            DataSchema declaredMeta, Sink output, List<String> encodedDescriptors) {
        if (encodedDescriptors == null || encodedDescriptors.size() != 1) {
            throw new IllegalArgumentException(
                    "batch sink metadata requires exactly one descriptor");
        }
        BatchSinkBinding binding = validated(declaredMeta, new BatchSinkBinding(
                output, decodeCanonical(encodedDescriptors.get(0))));
        validateCurrent(binding);
        return binding;
    }

    /** Only exact final built-ins retain their already-proven durable capability on old wire. */
    public static BatchSinkBinding freezeLegacyWire(DataSchema declaredMeta, Sink output) {
        BatchSinkDescriptor descriptor;
        if (output.getClass() == DiscardSink.class
                || output.getClass() == ManifestFileSink.class) {
            descriptor = ((BatchSink) output).batchSinkDescriptor();
        } else {
            descriptor = legacyDescriptor(declaredMeta, output);
        }
        return validated(declaredMeta, new BatchSinkBinding(output, descriptor));
    }

    public static List<String> encodeCanonical(BatchSinkBinding binding) {
        byte[] frame = BatchSinkDescriptorCodec.encode(binding.descriptor());
        if (frame.length > MAX_TOTAL_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException("batch sink descriptor bytes exceed aggregate limit");
        }
        return Collections.singletonList(
                Base64.getUrlEncoder().withoutPadding().encodeToString(frame));
    }

    /** Native provider-envelope reader: descriptor is decoded before any Sink exists. */
    public static BatchSinkDescriptor decodeCanonicalDescriptor(
            List<String> encodedDescriptors) {
        if (encodedDescriptors == null || encodedDescriptors.size() != 1) {
            throw new IllegalArgumentException(
                    "batch sink metadata requires exactly one descriptor");
        }
        return decodeCanonical(encodedDescriptors.get(0));
    }

    public static void validateCurrent(BatchSinkBinding binding) {
        BatchSinkDescriptor frozen = binding.descriptor();
        if (LEGACY_CONNECTOR.equals(frozen.connectorType())) return;
        if (!(binding.output() instanceof BatchSink)) {
            throw new IllegalArgumentException(
                    "described batch sink no longer implements BatchSink");
        }
        BatchSinkDescriptor current =
                ((BatchSink) binding.output()).batchSinkDescriptor();
        if (!frozen.equals(current)) {
            throw new IllegalArgumentException(
                    "batch sink descriptor changed after plan binding");
        }
    }

    private static BatchSinkBinding validated(
            DataSchema declaredMeta, BatchSinkBinding binding) {
        BatchSinkDescriptor descriptor = binding.descriptor();
        String schema = SchemaFingerprint.of(declaredMeta);
        if (!schema.equals(descriptor.schemaFingerprint())) {
            throw new IllegalArgumentException("batch sink schemaFingerprint mismatch");
        }
        BatchSinkDescriptorCodec.encode(descriptor);
        if (descriptor.capabilityProfile()
                == BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1) {
            String destination = binding.output().exclusiveDestinationId();
            if (destination == null || !destination.equals(descriptor.destinationId())) {
                throw new IllegalArgumentException(
                        "batch sink destination identity mismatch");
            }
        }
        return binding;
    }

    private static BatchSinkDescriptor decodeCanonical(String encoded) {
        if (encoded == null || encoded.isEmpty() || encoded.indexOf('=') >= 0) {
            throw canonicalFailure(null);
        }
        try {
            byte[] frame = Base64.getUrlDecoder().decode(encoded);
            if (!Base64.getUrlEncoder().withoutPadding().encodeToString(frame)
                    .equals(encoded)) {
                throw canonicalFailure(null);
            }
            return BatchSinkDescriptorCodec.decode(frame);
        } catch (IllegalArgumentException malformed) {
            if (malformed.getMessage() != null
                    && malformed.getMessage().contains("canonical")) {
                throw malformed;
            }
            throw canonicalFailure(malformed);
        }
    }

    private static IllegalArgumentException canonicalFailure(Throwable cause) {
        return new IllegalArgumentException(
                "batch sink descriptor metadata is not canonical", cause);
    }

    private static BatchSinkDescriptor legacyDescriptor(
            DataSchema declaredMeta, Sink output) {
        String destination = output.exclusiveDestinationId();
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, LEGACY_CONNECTOR, 1,
                SchemaFingerprint.of(declaredMeta),
                destination == null ? "none:legacy-output" : destination,
                "none:legacy-process",
                BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL);
    }
}
