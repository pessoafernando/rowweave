package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlResidentConfigTest {

    @Test
    void defaultBudgetMatchesPeakFormulaFloor() {
        ControlResidentConfig config = ControlResidentConfig.fromProperties(
                new Properties(), 1024L * 1024 * 1024);
        assertEquals(256L * 1024 * 1024, config.residentBytes());
    }

    @Test
    void budgetBelowPeakFloorOrAboveHalfHeapFailsClosed() {
        Properties tooSmall = new Properties();
        tooSmall.setProperty("rowweave.control.residentBytes", String.valueOf(223L * 1024 * 1024));
        assertThrows(IllegalArgumentException.class,
                () -> ControlResidentConfig.fromProperties(tooSmall, 1024L * 1024 * 1024));

        Properties tooLarge = new Properties();
        tooLarge.setProperty("rowweave.control.residentBytes", String.valueOf(257L * 1024 * 1024));
        assertThrows(IllegalArgumentException.class,
                () -> ControlResidentConfig.fromProperties(tooLarge, 512L * 1024 * 1024));
    }
}

