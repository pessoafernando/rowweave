package io.rowweave.portable;

/**
 * Metadata-only admission boundary for portable bindings.
 *
 * <p>Implementations must validate canonical provider/deployment contracts without
 * loading application classes, opening connectors, or creating runtime objects.
 */
public interface PortableBindingVerifier {
    /** Exact signed deployment set represented by this verifier, if available. */
    default String deploymentSetId() {
        return null;
    }

    void verifySource(PortableSourceBinding binding);
    void verifyFunction(FunctionDeploymentReference reference);
    void verifySink(PortableSinkBinding binding);
}
