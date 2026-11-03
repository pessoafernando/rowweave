package io.rowweave.artifact;

class InMemoryArtifactFencedGcTest extends ArtifactFencedGcContract {
    @Override
    ArtifactLifecycleRepository createRepository() {
        return new InMemoryArtifactRepository(32, 32L * 1024L * 1024L);
    }
}
