package io.rowweave.deployment;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** the operations manifest has one bounded canonical wire form. */
class DeploymentManifestCodecTest {

    @Test
    void canonicalManifestRoundTripsExactly() {
        byte[] digest = new byte[32];
        digest[0] = 1;
        byte[] signature = new byte[]{1, 2, 3, 4};
        DeploymentManifest manifest = new DeploymentManifest("orders-v1",
                Paths.get("/opt/rowweave/orders.jar"), 1234L, digest, "release-key", signature);

        byte[] encoded = DeploymentManifestCodec.encode(manifest);
        DeploymentManifest decoded = DeploymentManifestCodec.decode(encoded);

        assertEquals("RWXXDEP1\n"
                        + "deploymentId=orders-v1\n"
                        + "absoluteJarPath=/opt/rowweave/orders.jar\n"
                        + "jarLength=1234\n"
                        + "sha256=0100000000000000000000000000000000000000000000000000000000000000\n"
                        + "signerKeyId=release-key\n"
                        + "detachedSignatureBase64=" + Base64.getEncoder().encodeToString(signature) + "\n",
                new String(encoded, StandardCharsets.UTF_8));
        assertEquals(manifest.deploymentId(), decoded.deploymentId());
        assertEquals(manifest.absoluteJarPath(), decoded.absoluteJarPath());
        assertEquals(manifest.jarLength(), decoded.jarLength());
        assertArrayEquals(manifest.sha256(), decoded.sha256());
        assertEquals(manifest.signerKeyId(), decoded.signerKeyId());
        assertArrayEquals(manifest.detachedSignature(), decoded.detachedSignature());
    }

    @Test
    void malformedNonCanonicalOrOversizeManifestFailsClosed() {
        String valid = "RWXXDEP1\n"
                + "deploymentId=app\n"
                + "absoluteJarPath=/opt/app.jar\n"
                + "jarLength=1\n"
                + "sha256=0000000000000000000000000000000000000000000000000000000000000000\n"
                + "signerKeyId=key\n"
                + "detachedSignatureBase64=AQ==\n";

        assertInvalid(valid.replace("signerKeyId=key\n", "signerKeyId=key\nsignerKeyId=key\n"));
        assertInvalid(valid.replace("signerKeyId=key\n", "unknown=x\nsignerKeyId=key\n"));
        assertInvalid(valid.replace("sha256=00", "sha256=0A"));
        assertInvalid(valid.replace("AQ==", "AQ"));
        assertInvalid(valid.substring(0, valid.length() - 1));
        assertInvalid(valid.replace("/opt/app.jar", "/opt/../opt/app.jar"));
        assertInvalid(valid.replace("deploymentId=app", "deploymentId=app\r"));
        assertThrows(DeploymentValidationException.class,
                () -> DeploymentManifestCodec.decode(new byte[]{(byte) 0xc3, 0x28}));
        assertThrows(DeploymentValidationException.class,
                () -> DeploymentManifestCodec.decode(new byte[16 * 1024 + 1]));
    }

    private static void assertInvalid(String value) {
        DeploymentValidationException failure = assertThrows(DeploymentValidationException.class,
                () -> DeploymentManifestCodec.decode(value.getBytes(StandardCharsets.UTF_8)));
        assertEquals("INVALID_DEPLOYMENT_MANIFEST", failure.code());
    }
}
