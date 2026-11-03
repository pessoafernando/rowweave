package io.rowweave.sched;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskTest {

    @Test
    void finishFailureCannotLeaveTaskPermanentlyIncomplete() throws Exception {
        RuntimeException finishFailure = new RuntimeException("finish failed");
        Task task = new Task("finish-failure", () -> { }, ignored -> {
            throw finishFailure;
        });

        task.start();
        task.awaitCompletion(10_000);

        assertTrue(task.isCompleted());
        assertSame(finishFailure, task.failure());
    }

    @Test
    void bodyFailureRemainsPrimaryWhenFinishAlsoFails() throws Exception {
        RuntimeException bodyFailure = new RuntimeException("body failed");
        RuntimeException finishFailure = new RuntimeException("finish failed");
        Task task = new Task("body-and-finish-failure", () -> {
            throw bodyFailure;
        }, ignored -> {
            throw finishFailure;
        });

        task.start();
        task.awaitCompletion(10_000);

        assertTrue(task.isCompleted());
        assertSame(bodyFailure, task.failure());
        assertArrayEquals(new Throwable[]{finishFailure}, bodyFailure.getSuppressed());
    }
}
