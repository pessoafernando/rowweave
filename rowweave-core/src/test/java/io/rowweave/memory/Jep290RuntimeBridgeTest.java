package io.rowweave.memory;

import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.TrustedObjectInputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** An ordinary class file cannot link directly against the JDK9 ObjectInputFilter. */
class Jep290RuntimeBridgeTest {

    @Test
    void ordinaryObjectStreamClassesContainNoDirectJdk9FilterSymbol() throws Exception {
        assertNoDirectFilterSymbol(ResidentObjectInputStream.class);
        assertNoDirectFilterSymbol(TrustedObjectInputStream.class);
    }

    @Test
    void clusterTrustRequiresTheRuntimeBridgeUpFront() {
        assertNotNull(ObjectTrustContext.loopbackCluster());
        assertNotNull(ObjectTrustContext.mtlsBuiltIns());
    }

    private static void assertNoDirectFilterSymbol(Class<?> type) throws Exception {
        String resource = "/" + type.getName().replace('.', '/') + ".class";
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream input = type.getResourceAsStream(resource)) {
            assertNotNull(input);
            byte[] buffer = new byte[4096];
            for (int read; (read = input.read(buffer)) >= 0; ) bytes.write(buffer, 0, read);
        }
        String constants = new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
        assertFalse(constants.contains("java/io/ObjectInputFilter"), type.getName());
    }
}
