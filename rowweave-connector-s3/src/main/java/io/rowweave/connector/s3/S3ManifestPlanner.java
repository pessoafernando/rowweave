package io.rowweave.connector.s3;

import io.rowweave.stream.api.StreamSchema;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Prefix planner that freezes current objects into exact immutable splits. */
public final class S3ManifestPlanner {
    private static final int PAGE_KEYS = 1000;
    private static final int BUFFER_BYTES = 64 * 1024;

    private S3ManifestPlanner() { }

    public static S3Manifest planPrefix(
            S3ConnectorConfiguration configuration, StreamSchema schema,
            String prefix, byte[] credentialFrame) throws Exception {
        S3Credentials credentials = S3CredentialCodec.decode(credentialFrame);
        AwsS3ManifestPlanningClient client = null;
        Throwable primary = null;
        try {
            client = new AwsS3ManifestPlanningClient(
                    configuration, credentials);
            return planPrefix(configuration, schema, prefix, client);
        } catch (Throwable failure) {
            primary = failure;
            throw failure;
        } finally {
            credentials.destroy();
            if (client != null) {
                try {
                    client.close();
                } catch (Throwable closeFailure) {
                    if (primary != null) primary.addSuppressed(closeFailure);
                    else throw closeFailure;
                }
            }
        }
    }

    static S3Manifest planPrefix(S3ConnectorConfiguration configuration,
            StreamSchema schema, String prefix,
            S3ManifestPlanningClient client) throws Exception {
        if (configuration == null || schema == null || client == null) {
            throw failure("S3_MANIFEST_INVALID");
        }
        S3CodecSupport.utf8(prefix, 4096, true,
                "S3_MANIFEST_PREFIX_INVALID");
        List<String> keys = listKeys(prefix, client);
        if (keys.isEmpty()) throw failure("S3_MANIFEST_EMPTY");
        Collections.sort(keys);
        List<S3ObjectDescriptor> descriptors = new ArrayList<>(keys.size());
        for (String key : keys) {
            descriptors.add(freeze(configuration, schema, key, client));
        }
        return S3Manifest.of(schema, descriptors);
    }

    private static List<String> listKeys(String prefix,
            S3ManifestPlanningClient client) throws Exception {
        List<String> result = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();
        Set<String> tokens = new HashSet<>();
        String token = null;
        while (true) {
            final S3ListingPage page;
            try {
                page = client.list(prefix, token, PAGE_KEYS);
            } catch (Exception failure) {
                throw sanitize(failure, "S3_IO_FAILURE");
            }
            if (page == null || page.keys().size() > PAGE_KEYS) {
                throw failure("S3_MANIFEST_LISTING_INVALID");
            }
            for (String key : page.keys()) {
                if (key == null || !key.startsWith(prefix)) {
                    throw failure("S3_MANIFEST_LISTING_INVALID");
                }
                S3CodecSupport.utf8(key, 4096, false,
                        "S3_MANIFEST_LISTING_INVALID");
                if (!uniqueKeys.add(key)) {
                    throw failure("S3_MANIFEST_DUPLICATE");
                }
                result.add(key);
                if (result.size() > S3Manifest.MAX_OBJECTS) {
                    throw failure("S3_MANIFEST_OBJECT_LIMIT");
                }
            }
            if (!page.truncated()) {
                if (page.nextContinuationToken() != null) {
                    throw failure("S3_MANIFEST_PAGINATION_INVALID");
                }
                return result;
            }
            String next = page.nextContinuationToken();
            if (next == null || next.isEmpty() || !tokens.add(next)) {
                throw failure("S3_MANIFEST_PAGINATION_INVALID");
            }
            token = next;
        }
    }

    private static S3ObjectDescriptor freeze(
            S3ConnectorConfiguration configuration, StreamSchema schema,
            String key, S3ManifestPlanningClient client) throws Exception {
        S3ObjectStream object = null;
        Throwable primary = null;
        try {
            object = client.openCurrent(configuration.bucket(), key);
            if (object == null || object.etag() == null) {
                throw failure("S3_OBJECT_RESPONSE_INVALID");
            }
            long expected = object.contentLength();
            if (expected < 0 || expected > configuration.maximumObjectBytes()) {
                throw failure("S3_OBJECT_LIMIT");
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long count = 0L;
            byte[] buffer = new byte[BUFFER_BYTES];
            InputStream input = object.input();
            while (true) {
                int read = input.read(buffer);
                if (read < 0) break;
                if (read == 0) continue;
                count = Math.addExact(count, read);
                if (count > configuration.maximumObjectBytes()) {
                    throw failure("S3_OBJECT_LIMIT");
                }
                if (count > expected) throw failure("S3_OBJECT_CHANGED");
                digest.update(buffer, 0, read);
            }
            if (count != expected) throw failure("S3_OBJECT_CHANGED");
            String sha256 = S3CodecSupport.hex(digest.digest());
            return object.versionId() == null || object.versionId().isEmpty()
                    ? S3ObjectDescriptor.unversioned(
                    configuration.bucket(), key, object.etag(), expected,
                    sha256, configuration.format(), schema)
                    : S3ObjectDescriptor.versioned(
                    configuration.bucket(), key, object.versionId(),
                    object.etag(), expected, sha256,
                    configuration.format(), schema);
        } catch (Throwable failure) {
            primary = failure;
            if (failure instanceof IllegalArgumentException
                    || failure instanceof IllegalStateException) {
                throw failure;
            }
            throw new IllegalStateException("S3_IO_FAILURE");
        } finally {
            if (object != null) {
                try {
                    object.close();
                } catch (Throwable closeFailure) {
                    if (primary != null) primary.addSuppressed(closeFailure);
                    else throw new IllegalStateException(
                            "S3_RESOURCE_FAILURE");
                }
            }
        }
    }

    private static RuntimeException sanitize(Throwable actual,
            String fallback) {
        String message = actual.getMessage();
        if (actual instanceof RuntimeException && message != null
                && message.startsWith("S3_")) {
            return (RuntimeException) actual;
        }
        return new IllegalStateException(fallback);
    }

    private static IllegalArgumentException failure(String code) {
        return new IllegalArgumentException(code);
    }
}
