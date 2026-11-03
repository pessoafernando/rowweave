package io.rowweave.release;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.RSAKey;

/** Detached release signature boundary shared by release tooling and startup admission. */
public final class ReleaseContractSignature {
    public static final int MAX_SIGNATURE_BYTES = 1024;
    private static final byte[] DOMAIN = "RWXX-RELEASE-CONTRACT-V1\0"
            .getBytes(StandardCharsets.US_ASCII);

    private ReleaseContractSignature() { }

    /**
     * Verifies the detached signature before decoding and admitting the exact public manifest.
     * The returned model is therefore both cryptographically bound and catalog-complete.
     */
    public static ReleaseContractManifest verify(
            byte[] manifestFrame, byte[] detachedSignature, PublicKey signerKey) {
        requireInputs(manifestFrame, detachedSignature, signerKey);
        try {
            Signature verifier = Signature.getInstance("SHA256withRSA");
            verifier.initVerify(signerKey);
            verifier.update(signingPayload(manifestFrame));
            if (!verifier.verify(detachedSignature)) {
                throw ReleaseContractText.failure("SIGNATURE_INVALID", "release.signature",
                        "release contract signature is invalid");
            }
        } catch (ReleaseContractException failure) {
            throw failure;
        } catch (GeneralSecurityException failure) {
            throw new ReleaseContractException("SIGNATURE_INVALID", "release.signature",
                    "release contract signature cannot be verified", failure);
        }
        ReleaseContractManifest manifest = ReleaseContractManifestCodec.decode(manifestFrame);
        ReleaseContractCatalog.requirePublishable(manifest);
        return manifest;
    }

    /** Canonical, domain-separated payload used by offline release signing tools. */
    public static byte[] signingPayload(byte[] manifestFrame) {
        if (manifestFrame == null || manifestFrame.length == 0
                || manifestFrame.length > ReleaseContractManifestCodec.MAX_FRAME_BYTES) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "release.manifest",
                    "release manifest frame length is invalid");
        }
        byte[] digest = ReleaseContractManifestCodec.sha256(manifestFrame);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    DOMAIN.length + 4 + digest.length);
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.write(DOMAIN);
                out.writeInt(manifestFrame.length);
                out.write(digest);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    private static void requireInputs(
            byte[] manifestFrame, byte[] detachedSignature, PublicKey signerKey) {
        signingPayload(manifestFrame);
        if (detachedSignature == null || detachedSignature.length == 0
                || detachedSignature.length > MAX_SIGNATURE_BYTES) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "release.signature",
                    "release signature length is invalid");
        }
        if (!(signerKey instanceof RSAKey)
                || ((RSAKey) signerKey).getModulus().bitLength() < 3072) {
            throw ReleaseContractText.failure("SIGNER_KEY_TOO_WEAK", "release.signer",
                    "release signer must be RSA with at least 3072 bits");
        }
    }
}
