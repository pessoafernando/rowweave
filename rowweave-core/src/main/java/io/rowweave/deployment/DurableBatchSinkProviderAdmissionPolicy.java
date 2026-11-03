package io.rowweave.deployment;

import io.rowweave.DurableSinkProviderKey;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable exact allow policy; production construction remains a closed core seam. */
final class DurableBatchSinkProviderAdmissionPolicy {

    static final String CORE_DEPLOYMENT_ID = "rowweave-core";
    static final String DISCARD_PROVIDER_CLASS = "io.rowweave.DiscardSink";
    static final String MANIFEST_PROVIDER_CLASS =
            "io.rowweave.deployment.ManifestDurableBatchSinkProvider";
    private static final String DISCARD_CONTRACT_SHA256 =
            "1e8cf45776984a86484cb5ecbeebc98f135146bef6093402ccea57a95ef71349";
    private static final String MANIFEST_CONTRACT_SHA256 =
            "b251e262d62ba57a50c188da26d2718aecda42c84e0cb93c17f817a0c927ee96";

    private final List<Entry> entries;

    private DurableBatchSinkProviderAdmissionPolicy(List<Entry> entries) {
        if (entries == null || entries.size() > DurableBatchSinkProviderRegistry.MAX_CANDIDATES) {
            throw new IllegalArgumentException("provider admission policy size is invalid");
        }
        List<Entry> copy = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            if (entry == null) {
                throw new IllegalArgumentException("null provider admission policy entry");
            }
            for (Entry existing : copy) {
                if (existing.matches(entry.deploymentId,
                        entry.providerClassName, entry.providerKey)) {
                    throw new IllegalArgumentException(
                            "duplicate provider admission policy entry");
                }
            }
            copy.add(entry);
        }
        this.entries = Collections.unmodifiableList(copy);
    }

    static DurableBatchSinkProviderAdmissionPolicy production() {
        List<Entry> builtins = new ArrayList<>(2);
        builtins.add(entry(CORE_DEPLOYMENT_ID, DISCARD_PROVIDER_CLASS,
                new DurableSinkProviderKey("rowweave.discard", 1,
                        io.rowweave.BatchSinkDescriptor.CapabilityProfile
                                .SIDE_EFFECT_FREE_V1),
                digest(DISCARD_CONTRACT_SHA256)));
        builtins.add(entry(CORE_DEPLOYMENT_ID, MANIFEST_PROVIDER_CLASS,
                new DurableSinkProviderKey("rowweave.manifest-file", 1,
                        io.rowweave.BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1),
                digest(MANIFEST_CONTRACT_SHA256)));
        return new DurableBatchSinkProviderAdmissionPolicy(builtins);
    }

    static DurableBatchSinkProviderAdmissionPolicy forTesting(List<Entry> entries) {
        List<Entry> combined = new ArrayList<>(production().entries);
        combined.addAll(entries);
        return new DurableBatchSinkProviderAdmissionPolicy(combined);
    }

    static Entry entry(
            String deploymentId, String providerClassName,
            DurableSinkProviderKey providerKey, byte[] contractDigest) {
        return new Entry(deploymentId, providerClassName, providerKey, contractDigest);
    }

    Entry match(String deploymentId, String providerClassName,
                DurableSinkProviderKey providerKey) {
        for (Entry entry : entries) {
            if (entry.matches(deploymentId, providerClassName, providerKey)) {
                return entry;
            }
        }
        return null;
    }

    int size() { return entries.size(); }

    private static byte[] digest(String hex) {
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    static final class Entry {
        private final String deploymentId;
        private final String providerClassName;
        private final DurableSinkProviderKey providerKey;
        private final byte[] contractDigest;

        private Entry(
                String deploymentId, String providerClassName,
                DurableSinkProviderKey providerKey, byte[] contractDigest) {
            this.deploymentId = identity(deploymentId, "deploymentId");
            this.providerClassName = identity(
                    providerClassName, "providerClassName");
            if (providerKey == null || contractDigest == null
                    || contractDigest.length != 32) {
                throw new IllegalArgumentException(
                        "provider policy key and SHA-256 digest are required");
            }
            this.providerKey = providerKey;
            this.contractDigest = contractDigest.clone();
        }

        boolean matches(String deploymentId, String providerClassName,
                        DurableSinkProviderKey providerKey) {
            return this.deploymentId.equals(deploymentId)
                    && this.providerClassName.equals(providerClassName)
                    && this.providerKey.equals(providerKey);
        }

        boolean digestMatches(byte[] digest) {
            return MessageDigest.isEqual(contractDigest, digest);
        }

        private static String identity(String value, String field) {
            if (value == null || value.isEmpty() || value.length() > 256
                    || value.indexOf('\0') >= 0) {
                throw new IllegalArgumentException(
                        "provider policy " + field + " is invalid");
            }
            return value;
        }
    }
}
