package io.rowweave.incremental;

/** Registry capability exposing one non-splittable incremental authority. */
public interface IncrementalAuthorityGatewayProvider {
    IncrementalAuthorityGateway incrementalAuthorityGateway();
}
