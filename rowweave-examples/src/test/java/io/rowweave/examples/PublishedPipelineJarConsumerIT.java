package io.rowweave.examples;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Black-box gate: an external-platform consumer compiles and runs from packaged public JARs only. */
class PublishedPipelineJarConsumerIT {
    @Test
    void packagedCoreAndPipelineJarsCompileAndRunExternalConsumer() throws Exception {
        assertEquals("batch=1,stream=1,mixed=[batch, stream],"
                        + "shared=[shared, a-batch, b-stream],outputs=2,"
                        + "revisionChanged=true,handoffBound=true",
                PublishedPipelineJarConsumerVerifier.verify(
                        Paths.get(required("rowweave.published.core.jar")),
                        Paths.get(required("rowweave.published.pipeline.jar")),
                        Paths.get(required("rowweave.stream.example.sources"))));
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException(property + " is required");
        }
        return value;
    }
}
