package io.rowweave.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.DynamicTest;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** freeze the shared kit before any production provider surface exists. */
class DurableBatchSinkProviderConformanceRedTest {

    @TestFactory
    Stream<DynamicTest> manifestBackendMustRunTheSharedBlackBoxMatrix() {
        return DurableBatchSinkProviderConformance.scenarios(
                new ManifestConformanceBackendFactory());
    }

    @TestFactory
    Stream<DynamicTest> signedAdversarialBackendMustRunTheSharedBlackBoxMatrix() {
        return DurableBatchSinkProviderConformance.scenarios(
                new AdversarialConformanceBackendFactory());
    }

    @Test
    void catalogCoversTheAcceptedFaultMatrixAndBothBackendFactories() {
        Set<DurableSinkConformanceScenario.Fault> faults = new HashSet<>();
        for (DurableSinkConformanceScenario scenario
                : DurableSinkConformanceScenario.values()) {
            faults.add(scenario.fault());
        }
        assertEquals(new HashSet<>(Arrays.asList(
                DurableSinkConformanceScenario.Fault.values())), faults);
        assertTrue(DurableSinkConformanceScenario.BLOCKING_OPEN.requiresProcessIsolation());
        assertTrue(DurableSinkConformanceScenario.BLOCKING_WRITE.requiresProcessIsolation());
        assertTrue(DurableSinkConformanceScenario.BLOCKING_PREPARE.requiresProcessIsolation());
        assertTrue(DurableSinkConformanceScenario.CROSS_PROVIDER_COMPOSITION
                .rejectsBeforeSideEffect());
        assertTrue(DurableSinkConformanceScenario.CROSS_DOMAIN_COMPOSITION
                .rejectsBeforeSideEffect());
        assertTrue(DurableSinkConformanceScenario.MIXED_DISCARD_PROVIDER_COMPOSITION
                .rejectsBeforeSideEffect());
        assertTrue(DurableSinkConformanceScenario.DUPLICATE_MEMBER_COMPOSITION
                .rejectsBeforeSideEffect());

        DurableSinkConformanceBackendFactory manifest =
                new ManifestConformanceBackendFactory();
        DurableSinkConformanceBackendFactory adversarial =
                new AdversarialConformanceBackendFactory();
        assertTrue(manifest.productionSideEffectfulBackend());
        assertTrue(!adversarial.productionSideEffectfulBackend());
        assertEquals("manifest", manifest.backendId());
        assertEquals("signed-adversarial", adversarial.backendId());
    }

    @Test
    void versionedProviderSurfaceMustExistBeforeAFactoryCanRun() throws Exception {
        Class<?> provider = Class.forName("io.rowweave.DurableBatchSinkProvider");
        assertTrue(provider.isInterface(), "provider must be an interface");
        Set<String> methods = new HashSet<>();
        for (Method method : provider.getMethods()) {
            if (Modifier.isAbstract(method.getModifiers())) methods.add(method.getName());
        }
        assertTrue(methods.containsAll(Arrays.asList(
                "contract", "openWriteAttempt", "openPreparedGroup")),
                "provider surface does not expose the accepted lifecycle roots: " + methods);
    }
}
