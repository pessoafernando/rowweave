package io.rowweave.incremental;

import io.rowweave.cluster.registry.InProcessRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** F5 cluster control may only receive one non-splittable authority gateway. */
class IncrementalInMemoryGatewayContractTest {

    @Test
    void referenceAuthorityExposesTheSameInstanceAsTheUnifiedGateway() {
        InMemoryIncrementalAuthorityStore authority =
                new InMemoryIncrementalAuthorityStore();

        assertInstanceOf(IncrementalAuthorityGateway.class, authority);
    }

    @Test
    void inProcessRegistryPublishesOneSharedIncrementalGateway() {
        Object registry = new InProcessRegistry();

        assertInstanceOf(IncrementalAuthorityGatewayProvider.class, registry);
        IncrementalAuthorityGateway first =
                ((IncrementalAuthorityGatewayProvider) registry)
                        .incrementalAuthorityGateway();
        assertSame(first,
                ((IncrementalAuthorityGatewayProvider) registry)
                        .incrementalAuthorityGateway());
    }
}
