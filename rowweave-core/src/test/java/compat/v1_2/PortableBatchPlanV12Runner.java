package compat.v1_2;

/** Frozen independent v1.2 portable-plan runner. */
public final class PortableBatchPlanV12Runner {
    private PortableBatchPlanV12Runner() { }

    public static void main(String[] args) throws Exception {
        PortableContractsV12Wire.run(PortableContractsV12Wire.PLAN, args);
    }
}
