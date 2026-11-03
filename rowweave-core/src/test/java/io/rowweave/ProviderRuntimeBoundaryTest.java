package io.rowweave;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** provider codecs and observable resources are mandatory SPI. */
class ProviderRuntimeBoundaryTest {

    @Test
    void providerMustCanonicalizeLocatorPayloadBeforeAuthorityMutation()
            throws Exception {
        assertEquals(byte[].class,
                DurableBatchSinkProvider.class.getMethod(
                        "canonicalizeLocator",
                        io.rowweave.run.PreparedSinkLocator.class)
                        .getReturnType());
    }

    @Test
    void actionContextMustExposeEnforceableResidentAndHandleLeases()
            throws Exception {
        Class<?> lease = Class.forName("io.rowweave.DurableSinkResourceLease");
        assertEquals(lease, DurableSinkActionContext.class.getMethod(
                "acquireResidentLease", long.class).getReturnType());
        assertEquals(lease, DurableSinkActionContext.class.getMethod(
                "acquireHandleLease").getReturnType());
        assertEquals(void.class, lease.getMethod("close").getReturnType());
    }
}
