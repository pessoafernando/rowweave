package io.rowweave.release;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** immutable release packet, API linkage and product-doc gate. */
class ReleasePacketGateTest {

    @Test
    void signedPublishedManifestBindsExactApiBaseline() throws Exception {
        byte[] manifestFrame = hex(resource("/release/v1/release-manifest.hex").trim());
        byte[] signature = hex(resource("/release/v1/release-manifest.sig.hex").trim());
        byte[] publicKeyFrame = hex(resource("/release/v1/release-public-key.der.hex").trim());
        PublicKey publicKey = KeyFactory.getInstance("RSA").generatePublic(
                new X509EncodedKeySpec(publicKeyFrame));

        ReleaseContractManifest manifest = ReleaseContractSignature.verify(
                manifestFrame, signature, publicKey);
        byte[] baseline = resourceBytes("/release/v1/public-api-v1.sig");
        assertEquals(ReleaseContractManifestCodec.hex(
                        ReleaseContractManifestCodec.sha256(baseline)),
                manifest.publicApiBaselineDigest());
        assertEquals("rowweave-1.0.0", manifest.releaseId());
        assertEquals(1, manifest.protocolEpoch());
        assertEquals(0, manifest.releaseRevision());
        assertEquals(ReleaseContractCatalog.publicV1Baseline(
                manifest.publicApiBaselineDigest()), manifest);
        ReleaseContractCatalog.requirePublishable(manifest);
    }

    @Test
    void frozenPublicApiAndSpiDescriptorsRemainBinaryLinkable() throws Exception {
        for (String line : lines("/release/v1/public-api-v1.sig")) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] parts = line.split(" ", 3);
            assertTrue(parts.length >= 2, line);
            Class<?> owner = Class.forName(parts[1].split("#", 2)[0]);
            if ("TYPE".equals(parts[0])) {
                assertTrue(java.lang.reflect.Modifier.isPublic(owner.getModifiers()), line);
                continue;
            }
            assertEquals(3, parts.length, line);
            String member = parts[1].substring(parts[1].indexOf('#') + 1);
            String descriptor = parts[2];
            if ("METHOD".equals(parts[0])) {
                assertNotNull(method(owner, member, descriptor), line);
            } else if ("CTOR".equals(parts[0])) {
                assertNotNull(constructor(owner, descriptor), line);
            } else if ("FIELD".equals(parts[0])) {
                Field field = owner.getDeclaredField(member);
                assertTrue(java.lang.reflect.Modifier.isPublic(field.getModifiers()), line);
                assertEquals(descriptor, descriptor(field.getType()), line);
            } else {
                throw new AssertionError("unknown signature entry: " + line);
            }
        }
    }

    @Test
    void releaseAdmissionFailsClosedForTamperingAndWeakSigner() throws Exception {
        byte[] frame = hex(resource("/release/v1/release-manifest.hex").trim());
        byte[] signature = hex(resource("/release/v1/release-manifest.sig.hex").trim());
        PublicKey signer = KeyFactory.getInstance("RSA").generatePublic(
                new X509EncodedKeySpec(hex(resource(
                        "/release/v1/release-public-key.der.hex").trim())));

        byte[] changedFrame = frame.clone();
        changedFrame[changedFrame.length - 1] ^= 1;
        assertEquals("SIGNATURE_INVALID", assertThrows(ReleaseContractException.class,
                () -> ReleaseContractSignature.verify(changedFrame, signature, signer)).code());
        byte[] changedSignature = signature.clone();
        changedSignature[0] ^= 1;
        assertEquals("SIGNATURE_INVALID", assertThrows(ReleaseContractException.class,
                () -> ReleaseContractSignature.verify(frame, changedSignature, signer)).code());
        assertEquals("SIGNER_KEY_TOO_WEAK", assertThrows(ReleaseContractException.class,
                () -> ReleaseContractSignature.verify(frame, signature, weakRsaKey())).code());
    }

    @Test
    void productPacketNamesEveryOperationalAndExternalPlatformBoundary() throws Exception {
        String release = doc("docs/product/reference/versioned-release-v1.md");
        require(release, "release-manifest.hex", "public-api-v1.sig", "golden-corpus-v1.tsv",
                "PUBLIC_FROZEN", "INTERNAL_SAME_BUILD", "LEGACY_EPHEMERAL",
                "BigDecimal", "HALF_UP", "null != null");
        String rolling = doc("docs/runbooks/rolling-upgrade-v1.md");
        require(rolling, "N-1", "FREEZE_ADMISSION", "CUTOVER", "RESUME_WRITES",
                "DATA_ROLLBACK", "MARK_NEW_SEMANTICS", "ACK");
        String platform = doc("docs/product/guides/external-platform-integration.md");
        require(platform, "PORTABLE_BATCH_PLAN_V1", "submitRun", "queryRun",
                "queryRunEvents", "queryBatchSinkOutcome", "queryQualityGateDecision",
                "runId", "attemptId", "publicationId");
    }

    private static Method method(Class<?> owner, String name, String expected) {
        for (Method method : owner.getDeclaredMethods()) {
            if (method.getName().equals(name)
                    && java.lang.reflect.Modifier.isPublic(method.getModifiers())
                    && methodDescriptor(method).equals(expected)) return method;
        }
        return null;
    }

    private static Constructor<?> constructor(Class<?> owner, String expected) {
        for (Constructor<?> constructor : owner.getDeclaredConstructors()) {
            if (java.lang.reflect.Modifier.isPublic(constructor.getModifiers())
                    && constructorDescriptor(constructor).equals(expected)) return constructor;
        }
        return null;
    }

    private static String methodDescriptor(Method method) {
        return parameters(method.getParameterTypes()) + descriptor(method.getReturnType());
    }

    private static String constructorDescriptor(Constructor<?> constructor) {
        return parameters(constructor.getParameterTypes()) + "V";
    }

    private static String parameters(Class<?>[] parameters) {
        StringBuilder value = new StringBuilder("(");
        for (Class<?> parameter : parameters) value.append(descriptor(parameter));
        return value.append(')').toString();
    }

    private static String descriptor(Class<?> type) {
        if (type.isPrimitive()) {
            if (type == void.class) return "V";
            if (type == boolean.class) return "Z";
            if (type == byte.class) return "B";
            if (type == char.class) return "C";
            if (type == short.class) return "S";
            if (type == int.class) return "I";
            if (type == long.class) return "J";
            if (type == float.class) return "F";
            if (type == double.class) return "D";
        }
        if (type.isArray()) return type.getName().replace('.', '/');
        return "L" + type.getName().replace('.', '/') + ";";
    }

    private static void require(String text, String... tokens) {
        for (String token : tokens) assertTrue(text.contains(token), token);
    }

    private static List<String> lines(String name) throws Exception {
        List<String> values = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                stream(name), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) values.add(line.trim());
        }
        return values;
    }

    private static String resource(String name) throws Exception {
        return new String(resourceBytes(name), StandardCharsets.UTF_8);
    }

    private static byte[] resourceBytes(String name) throws Exception {
        try (InputStream in = stream(name)) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) >= 0) out.write(buffer, 0, read);
            return out.toByteArray();
        }
    }

    private static InputStream stream(String name) {
        InputStream in = ReleasePacketGateTest.class.getResourceAsStream(name);
        assertNotNull(in, name);
        return in;
    }

    private static String doc(String relative) throws Exception {
        Path current = Paths.get("").toAbsolutePath();
        Path root = Files.isDirectory(current.resolve("docs"))
                ? current : current.getParent();
        return new String(Files.readAllBytes(root.resolve(relative)),
                StandardCharsets.UTF_8);
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i *2 + 2), 16);
        }
        return bytes;
    }

    private static RSAPublicKey weakRsaKey() {
        return new RSAPublicKey() {
            private static final long serialVersionUID = 1L;
            @Override public BigInteger getModulus() { return BigInteger.ONE.shiftLeft(2047); }
            @Override public BigInteger getPublicExponent() { return BigInteger.valueOf(65537); }
            @Override public String getAlgorithm() { return "RSA"; }
            @Override public String getFormat() { return "X.509"; }
            @Override public byte[] getEncoded() { return new byte[0]; }
        };
    }
}
