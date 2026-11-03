package io.rowweave.artifact;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class FileArtifactFencedGcTest extends ArtifactFencedGcContract {
    @TempDir
    Path temporary;

    @Override
    ArtifactLifecycleRepository createRepository() {
        return new FileArtifactRepository(temporary);
    }
}
