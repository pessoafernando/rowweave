package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.security.Provider;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exact build entries never imply admission of a vendor's other releases. */
class CertifiedTlsBuildMatrixTest {
    private static final String VM = "OpenJDK 64-Bit Server VM";

    @Test
    void exactBuildsRequireCertifiedPlatformsAndTransportArtifacts() {
        ProcessTlsEnvelope homebrew = envelope("Homebrew", "17.0.15+0", VM);
        assertTrue(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(homebrew, "Mac OS X", "aarch64"));
        assertFalse(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(homebrew, "Linux", "aarch64"));
        assertFalse(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(homebrew, "Mac OS X", "amd64"));
        if ("Mac OS X".equals(System.getProperty("os.name"))
                && "aarch64".equals(System.getProperty("os.arch"))) {
            assertDoesNotThrow(() -> CertifiedTlsBuildMatrix.requireCertified(homebrew));
        } else {
            assertThrows(IllegalStateException.class, () -> CertifiedTlsBuildMatrix.requireCertified(homebrew));
        }
        ProcessTlsEnvelope linux = envelope("Eclipse Adoptium", "17.0.20+8", VM);
        assertTrue(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(linux, "Linux", "aarch64"));
        assertTrue(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(linux, "Mac OS X", "aarch64"));
        assertFalse(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(linux, "Windows", "aarch64"));
        assertFalse(CertifiedTlsBuildMatrix.hasCertifiedJavaBuild(linux, "Linux", "amd64"));
        if (("Linux".equals(System.getProperty("os.name"))
                || "Mac OS X".equals(System.getProperty("os.name")))
                && "aarch64".equals(System.getProperty("os.arch"))) {
            assertDoesNotThrow(() -> CertifiedTlsBuildMatrix.requireCertified(linux));
        } else {
            assertThrows(IllegalStateException.class, () -> CertifiedTlsBuildMatrix.requireCertified(linux));
        }
    }

    @Test
    void neighbouringVersionsAndCrossVendorPairsRemainUncertified() {
        String[][] unsupported = {
                {"Eclipse Adoptium", "17.0.20+7"},
                {"Eclipse Adoptium", "17.0.21+8"},
                {"Eclipse Adoptium", "17.0.15+0"},
                {"Homebrew", "17.0.20+8"},
                {"Oracle Corporation", "17.0.20+8"}
        };
        for (String[] build : unsupported) {
            assertThrows(IllegalStateException.class,
                    () -> CertifiedTlsBuildMatrix.requireCertified(
                            envelope(build[0], build[1], VM)));
        }
        assertThrows(IllegalStateException.class,
                () -> CertifiedTlsBuildMatrix.requireCertified(envelope(
                        "Eclipse Adoptium", "17.0.20+8", "Other VM")));
    }

    @Test
    void matchingProviderNameWithoutExactProviderClassRemainsRejected() {
        Provider impostor = new Provider("SunJSSE", 17.0, "not SunJSSE") { };
        ProcessTlsEnvelope proof = new ProcessTlsEnvelope(
                "Homebrew", "17.0.15+0", VM, impostor, null, null);
        assertThrows(IllegalStateException.class,
                () -> CertifiedTlsBuildMatrix.requireCertified(proof));
    }

    private static ProcessTlsEnvelope envelope(
            String vendor, String version, String vm) {
        return new ProcessTlsEnvelope(vendor, version, vm,
                Security.getProvider("SunJSSE"), null, null);
    }
}
