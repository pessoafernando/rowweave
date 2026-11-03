package io.rowweave.ops.join;

/** Fresh-process probe for the 30-round small-input timing gate. */
public final class ConcurrentBinaryDrainPerformanceProbe {

    private ConcurrentBinaryDrainPerformanceProbe() {
    }

    public static void main(String[] arguments) {
        System.out.println(ConcurrentBinaryDrainPerformanceTest
                .measureSmallInputAverageOverheadNanos());
    }
}
