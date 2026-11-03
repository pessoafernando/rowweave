package io.rowweave.release;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReleaseContractAdmissionTest {

    @Test
    void legacyPlanPayloadAndRpcOrdinalCannotEnterPublicV1() {
        assertRejected("rowweave.java-object-plan-payload", "LEGACY_NOT_PUBLIC");
        assertRejected("rowweave.rpc-enum-ordinal", "LEGACY_NOT_PUBLIC");
    }

    @Test
    void remainingSameBuildComponentsCannotEnterPublicV1() {
        assertRejected("rowweave.plan-graph", "SAME_BUILD_ONLY");
    }

    @Test
    void portablePlanFunctionAndConnectorContractsArePublicV1() {
        assertPublic("rowweave.portable-batch-plan");
        assertPublic("rowweave.function-provider");
        assertPublic("rowweave.portable-connector-provider");
        assertPublic("rowweave.control-rpc");
        assertPublic("rowweave.control-protocol");
        assertPublic("rowweave.stage-command");
    }

    @Test
    void frozenPublicComponentIsAdmittedWithItsExactCatalogContract() {
        ReleaseComponentContract admitted =
                ReleaseContractCatalog.requirePublicV1Component(
                        "rowweave.plan-artifact-envelope");

        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                admitted.scope());
        assertEquals("rowweave.plan-artifact-envelope", admitted.componentId());
        assertEquals(catalogComponent("rowweave.plan-artifact-envelope"), admitted);
    }

    @Test
    void unknownComponentFailsClosedWithoutEchoingTheIdentifier() {
        String secretLikeId = "rowweave.unknown.customer-secret";
        ReleaseContractException failure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublicV1Component(
                        secretLikeId));

        assertEquals("UNKNOWN_COMPONENT", failure.code());
        assertEquals("componentId", failure.path());
        org.junit.jupiter.api.Assertions.assertFalse(
                failure.getMessage().contains(secretLikeId));
    }

    private static void assertRejected(String componentId, String code) {
        ReleaseContractException failure = assertThrows(
                ReleaseContractException.class,
                () -> ReleaseContractCatalog.requirePublicV1Component(
                        componentId));
        assertEquals(code, failure.code());
        assertEquals("componentId", failure.path());
    }

    private static void assertPublic(String componentId) {
        assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                ReleaseContractCatalog.requirePublicV1Component(
                        componentId).scope());
    }

    private static ReleaseComponentContract catalogComponent(String componentId) {
        for (ReleaseComponentContract component :
                ReleaseContractCatalog.preV1Inventory()) {
            if (componentId.equals(component.componentId())) return component;
        }
        throw new AssertionError("missing catalog component");
    }
}
