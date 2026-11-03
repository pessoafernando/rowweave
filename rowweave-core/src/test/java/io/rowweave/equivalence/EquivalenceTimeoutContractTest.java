package io.rowweave.equivalence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EquivalenceTimeoutContractTest {

    @Test
    void equivalenceClassesDeclareFiveMinuteTimeout() {
        for (Class<?> testClass : Arrays.asList(
                ComplexDagMultiInputMultiOutputEquivalenceTest.class,
                ThreeWayEquivalenceTest.class,
                ThreeWayTopoEquivalenceTest.class,
                ThreeWayTypesEquivalenceTest.class)) {
            Timeout timeout = testClass.getAnnotation(Timeout.class);
            assertNotNull(timeout, testClass.getSimpleName() + " must declare a hard timeout");
            assertEquals(5, timeout.value(), testClass.getSimpleName());
            assertEquals(TimeUnit.MINUTES, timeout.unit(), testClass.getSimpleName());
        }
    }
}
