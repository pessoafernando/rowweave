package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunDeadlineSubmissionPolicyTest {
    @Test
    void productionPolicyAllowsOnlySingleAttemptWithOptionalAbsoluteDeadline() {
        assertTrue(RunSubmissionPolicyValidator.isSupported(1, 0L));
        assertTrue(RunSubmissionPolicyValidator.isSupported(1, 1L));
        assertTrue(RunSubmissionPolicyValidator.isSupported(1, Long.MAX_VALUE));
        assertTrue(RunSubmissionPolicyValidator.isSupported(2, 0L));
        assertTrue(RunSubmissionPolicyValidator.isSupported(2, 1L));
        assertTrue(RunSubmissionPolicyValidator.isSupported(3, Long.MAX_VALUE));
        assertFalse(RunSubmissionPolicyValidator.isSupported(4, 0L));
        assertFalse(RunSubmissionPolicyValidator.isSupported(0, 1L));
        assertFalse(RunSubmissionPolicyValidator.isSupported(1, -1L));
    }
}
