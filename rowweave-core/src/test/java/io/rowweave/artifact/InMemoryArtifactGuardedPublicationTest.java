package io.rowweave.artifact;

class InMemoryArtifactGuardedPublicationTest
        extends ArtifactGuardedPublicationContract {

    private final InMemoryArtifactRepository shared =
            new InMemoryArtifactRepository(32, 8L * 1024L * 1024L);

    @Override
    protected ArtifactLifecycleRepository newRepository() {
        return shared;
    }
}
