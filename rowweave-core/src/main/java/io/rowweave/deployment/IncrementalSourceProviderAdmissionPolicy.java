package io.rowweave.deployment;

import io.rowweave.incremental.IncrementalSourceProviderKey;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable exact allow policy for signed incremental source providers. */
final class IncrementalSourceProviderAdmissionPolicy {
    static final String CORE_DEPLOYMENT_ID = "rowweave-core";
    static final String JDBC_SEQUENCE_PROVIDER_CLASS =
            "io.rowweave.incremental.JdbcSequenceIncrementalSourceProvider";
    static final String JDBC_TIMESTAMP_PROVIDER_CLASS =
            "io.rowweave.incremental.JdbcTimestampKeyIncrementalSourceProvider";
    static final String OBJECT_MANIFEST_PROVIDER_CLASS =
            "io.rowweave.incremental.ObjectManifestIncrementalSourceProvider";
    private static final String JDBC_SEQUENCE_CONTRACT_SHA256 =
            "946b983208ffd9b0971897bd5c71a9e7b8d28cb4a18dd5abdd122c21a2c76974";
    private static final String JDBC_TIMESTAMP_CONTRACT_SHA256 =
            "f04e0597ed528de19d02ce52a335fac779bdebf258dc7ad2ada0f6b9a336479c";
    private static final String OBJECT_MANIFEST_CONTRACT_SHA256 =
            "64220338f3c73543351d9fc55d62bc96cf0e8b8a27480a8128beeabb0b1d0e1d";
    private final List<Entry> entries;

    private IncrementalSourceProviderAdmissionPolicy(List<Entry> entries) {
        if (entries == null
                || entries.size() > IncrementalSourceProviderRegistry.MAX_CANDIDATES) {
            throw new IllegalArgumentException(
                    "source provider admission policy size is invalid");
        }
        List<Entry> copy = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            if (entry == null) {
                throw new IllegalArgumentException(
                        "null source provider admission policy entry");
            }
            for (Entry existing : copy) {
                if (existing.matches(entry.deploymentId,
                        entry.providerClassName, entry.providerKey)) {
                    throw new IllegalArgumentException(
                            "duplicate source provider admission policy entry");
                }
            }
            copy.add(entry);
        }
        this.entries = Collections.unmodifiableList(copy);
    }

    static IncrementalSourceProviderAdmissionPolicy production() {
        List<Entry> builtins = new ArrayList<>(3);
        builtins.add(entry(CORE_DEPLOYMENT_ID, JDBC_SEQUENCE_PROVIDER_CLASS,
                io.rowweave.incremental.JdbcIncrementalSourceProfile
                        .sequenceContract().providerKey(),
                digest(JDBC_SEQUENCE_CONTRACT_SHA256)));
        builtins.add(entry(CORE_DEPLOYMENT_ID, JDBC_TIMESTAMP_PROVIDER_CLASS,
                io.rowweave.incremental.JdbcIncrementalSourceProfile
                        .timestampKeyContract().providerKey(),
                digest(JDBC_TIMESTAMP_CONTRACT_SHA256)));
        builtins.add(entry(CORE_DEPLOYMENT_ID, OBJECT_MANIFEST_PROVIDER_CLASS,
                io.rowweave.incremental.ObjectManifestIncrementalSourceProfile
                        .builtInContract().providerKey(),
                digest(OBJECT_MANIFEST_CONTRACT_SHA256)));
        return new IncrementalSourceProviderAdmissionPolicy(builtins);
    }

    static IncrementalSourceProviderAdmissionPolicy forTesting(
            List<Entry> entries) {
        List<Entry> combined = new ArrayList<>(production().entries);
        combined.addAll(entries);
        return new IncrementalSourceProviderAdmissionPolicy(combined);
    }

    static Entry entry(String deploymentId, String providerClassName,
                       IncrementalSourceProviderKey providerKey,
                       byte[] contractDigest) {
        return new Entry(deploymentId, providerClassName,
                providerKey, contractDigest);
    }

    Entry match(String deploymentId, String providerClassName,
                IncrementalSourceProviderKey providerKey) {
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
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    hex.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }

    static final class Entry {
        private final String deploymentId;
        private final String providerClassName;
        private final IncrementalSourceProviderKey providerKey;
        private final byte[] contractDigest;

        private Entry(String deploymentId, String providerClassName,
                      IncrementalSourceProviderKey providerKey,
                      byte[] contractDigest) {
            this.deploymentId = identity(deploymentId, "deploymentId");
            this.providerClassName = identity(
                    providerClassName, "providerClassName");
            if (providerKey == null || contractDigest == null
                    || contractDigest.length != 32) {
                throw new IllegalArgumentException(
                        "source provider policy key and SHA-256 digest are required");
            }
            this.providerKey = providerKey;
            this.contractDigest = contractDigest.clone();
        }

        boolean matches(String deploymentId, String providerClassName,
                        IncrementalSourceProviderKey providerKey) {
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
                        "source provider policy " + field + " is invalid");
            }
            return value;
        }
    }
}
