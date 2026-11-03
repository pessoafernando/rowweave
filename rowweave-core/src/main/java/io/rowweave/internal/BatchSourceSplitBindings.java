package io.rowweave.internal;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.Source;
import io.rowweave.CollectionSource;
import io.rowweave.ManifestFileSource;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Canonical SOURCE split binding resolver shared by PlanNode construction and wire validation. */
public final class BatchSourceSplitBindings {

    public static final int MAX_TOTAL_DESCRIPTOR_BYTES = 4 * 1024 * 1024;

    private BatchSourceSplitBindings() {
    }

    /** Freeze current connector descriptors; legacy Source implementations get an explicit downgrade. */
    public static List<BatchSourceSplitBinding> freezeNew(
            DataSchema declaredMeta, List<Source> inputs) {
        String schema = SchemaFingerprint.of(declaredMeta);
        List<BatchSourceSplitBinding> bindings = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            Source input = inputs.get(i);
            BatchSourceSplitDescriptor descriptor;
            if (input instanceof BatchSource) {
                descriptor = ((BatchSource) input).batchSourceSplit();
                if (descriptor == null) {
                    if (input instanceof CollectionSource
                            || input instanceof ManifestFileSource) {
                        descriptor = legacyDescriptor(i, schema);
                    } else {
                        throw new IllegalArgumentException(
                                "batch source descriptor is required at input position " + i);
                    }
                }
            } else {
                descriptor = legacyDescriptor(i, schema);
            }
            bindings.add(new BatchSourceSplitBinding(input, descriptor));
        }
        return validated(declaredMeta, bindings, true);
    }

    /** Decode already-frozen canonical metadata without consulting mutable connector state for identity. */
    public static List<BatchSourceSplitBinding> decodeFrozen(
            DataSchema declaredMeta, List<Source> inputs, List<String> encodedDescriptors) {
        if (encodedDescriptors == null || encodedDescriptors.size() != inputs.size()) {
            throw new IllegalArgumentException(
                    "batch source descriptor count must equal input count");
        }
        List<BatchSourceSplitBinding> bindings = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            bindings.add(new BatchSourceSplitBinding(
                    inputs.get(i), decodeCanonical(encodedDescriptors.get(i), i)));
        }
        return validated(declaredMeta, bindings, true);
    }

    /** Old Plan wire has no descriptor metadata and therefore remains explicitly single-attempt legacy. */
    public static List<BatchSourceSplitBinding> freezeLegacyWire(
            DataSchema declaredMeta, List<Source> inputs) {
        String schema = SchemaFingerprint.of(declaredMeta);
        List<BatchSourceSplitBinding> bindings = new ArrayList<>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            bindings.add(new BatchSourceSplitBinding(inputs.get(i), legacyDescriptor(i, schema)));
        }
        return validated(declaredMeta, bindings, false);
    }

    public static List<String> encodeCanonical(List<BatchSourceSplitBinding> bindings) {
        List<String> encoded = new ArrayList<>(bindings.size());
        long total = 0L;
        for (BatchSourceSplitBinding binding : bindings) {
            byte[] frame = BatchSourceSplitDescriptorCodec.encode(binding.descriptor());
            total = Math.addExact(total, frame.length);
            checkTotal(total);
            encoded.add(Base64.getUrlEncoder().withoutPadding().encodeToString(frame));
        }
        return Collections.unmodifiableList(encoded);
    }

    /** Detect a mutable BatchSource changing exact identity after the Plan binding was frozen. */
    public static void validateCurrent(List<BatchSourceSplitBinding> bindings) {
        for (int i = 0; i < bindings.size(); i++) {
            BatchSourceSplitBinding binding = bindings.get(i);
            if (binding.descriptor().replayGuarantee()
                    != BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START) {
                continue;
            }
            if (!(binding.input() instanceof BatchSource)) {
                throw new IllegalArgumentException(
                        "exact batch source binding no longer has BatchSource at position " + i);
            }
            BatchSourceSplitDescriptor current =
                    ((BatchSource) binding.input()).batchSourceSplit();
            if (!binding.descriptor().equals(current)) {
                throw new IllegalArgumentException(
                        "batch source descriptor changed after plan binding at position " + i);
            }
        }
    }

    private static List<BatchSourceSplitBinding> validated(
            DataSchema declaredMeta, List<BatchSourceSplitBinding> bindings,
            boolean validateExactConnectorState) {
        String schema = SchemaFingerprint.of(declaredMeta);
        Set<String> splitIds = new HashSet<>();
        long total = 0L;
        for (int i = 0; i < bindings.size(); i++) {
            BatchSourceSplitBinding binding = bindings.get(i);
            BatchSourceSplitDescriptor descriptor = binding.descriptor();
            if (!schema.equals(descriptor.schemaFingerprint())) {
                throw new IllegalArgumentException(
                        "batch source schemaFingerprint mismatch at input position " + i);
            }
            if (!splitIds.add(descriptor.splitId())) {
                throw new IllegalArgumentException(
                        "batch source duplicate splitId: " + descriptor.splitId());
            }
            byte[] frame = BatchSourceSplitDescriptorCodec.encode(descriptor);
            total = Math.addExact(total, frame.length);
            checkTotal(total);
        }
        List<BatchSourceSplitBinding> result = Collections.unmodifiableList(
                new ArrayList<>(bindings));
        if (validateExactConnectorState) {
            validateCurrent(result);
        }
        return result;
    }

    private static BatchSourceSplitDescriptor decodeCanonical(String encoded, int position) {
        if (encoded == null || encoded.isEmpty() || encoded.indexOf('=') >= 0) {
            throw canonicalFailure(position, null);
        }
        try {
            byte[] frame = Base64.getUrlDecoder().decode(encoded);
            String canonical = Base64.getUrlEncoder().withoutPadding().encodeToString(frame);
            if (!canonical.equals(encoded)) {
                throw canonicalFailure(position, null);
            }
            return BatchSourceSplitDescriptorCodec.decode(frame);
        } catch (IllegalArgumentException malformed) {
            if (malformed.getMessage() != null
                    && malformed.getMessage().contains("canonical")) {
                throw malformed;
            }
            throw canonicalFailure(position, malformed);
        }
    }

    private static IllegalArgumentException canonicalFailure(int position, Throwable cause) {
        return new IllegalArgumentException(
                "batch source descriptor metadata is not canonical at position " + position, cause);
    }

    private static void checkTotal(long total) {
        if (total > MAX_TOTAL_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException(
                    "batch source descriptor bytes exceed aggregate limit "
                            + MAX_TOTAL_DESCRIPTOR_BYTES);
        }
    }

    private static BatchSourceSplitDescriptor legacyDescriptor(int position, String schema) {
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "legacy-input", 1, "legacy-position:" + position,
                schema, "artifact-position:" + position,
                BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT);
    }
}
