import io.rowweave.release.ReleaseContractCatalog;
import io.rowweave.release.ReleaseContractManifest;
import io.rowweave.release.ReleaseContractManifestCodec;
import io.rowweave.release.ReleaseContractSignature;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Arrays;

/**
 * Offline regeneration of the release v1 signed packet (single-file launcher).
 *
 * The signing key is an RSA 3072 pair created here in memory and discarded when
 * this process exits. It is never written to disk, never printed, and never
 * reused for a test identity. The public key is the only half that lands in the
 * repository, and production code stays verify-only.
 *
 * Usage:
 *   java -cp rowweave-core/target/classes tools/release-signer/ReleasePacketSigner.java
 */
public final class ReleasePacketSigner {

    private static final String HEX = "0123456789abcdef";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "usage: ReleasePacketSigner <release-resource-dir>");
        }
        Path dir = Paths.get(args[0]);
        Path baselinePath = dir.resolve("public-api-v1.sig");
        byte[] baseline = Files.readAllBytes(baselinePath);
        String baselineDigest = hex(sha256(baseline));

        ReleaseContractManifest manifest =
                ReleaseContractCatalog.publicV1Baseline(baselineDigest);
        byte[] frame = ReleaseContractManifestCodec.encode(manifest);

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair pair = generator.generateKeyPair();
        PrivateKey signer = pair.getPrivate();

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer);
        signature.update(ReleaseContractSignature.signingPayload(frame));
        byte[] detached = signature.sign();

        byte[] publicKeyDer = pair.getPublic().getEncoded();

        // Re-admit the packet through the production verification path before
        // committing anything, so an unusable artifact can never be written.
        ReleaseContractManifest decoded = ReleaseContractSignature.verify(
                frame, detached,
                java.security.KeyFactory.getInstance("RSA").generatePublic(
                        new java.security.spec.X509EncodedKeySpec(publicKeyDer)));
        if (!Arrays.equals(frame, ReleaseContractManifestCodec.encode(decoded))) {
            throw new IllegalStateException("re-encoded frame differs from emitted frame");
        }

        writeHex(dir.resolve("release-manifest.hex"), frame);
        writeHex(dir.resolve("release-manifest.sig.hex"), detached);
        writeHex(dir.resolve("release-public-key.der.hex"), publicKeyDer);

        PrintStream out = new PrintStream(System.out, true, "UTF-8");
        out.println("releaseId=" + decoded.releaseId()
                + " protocolEpoch=" + decoded.protocolEpoch()
                + " releaseRevision=" + decoded.releaseRevision());
        out.println("manifestFrameBytes=" + frame.length
                + " signatureBytes=" + detached.length
                + " publicKeyDerBytes=" + publicKeyDer.length);
        out.println("manifestDigest=" + hex(sha256(frame)));
        out.println("publicApiBaselineDigest=" + baselineDigest);
        out.println("signerPublicKeySha256=" + hex(sha256(publicKeyDer)));
        out.println("publicFrozenComponents=" + decoded.componentContracts().size()
                + " capabilities=" + decoded.runtimeCapabilities());
    }

    private static void writeHex(Path path, byte[] bytes) throws Exception {
        Files.write(path, (hex(bytes) + "\n").getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] sha256(byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            value.append(HEX.charAt((b >> 4) & 0xf)).append(HEX.charAt(b & 0xf));
        }
        return value.toString();
    }
}
