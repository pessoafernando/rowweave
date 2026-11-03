package io.rowweave.cluster;

/** Fresh-process probe for the public JVM input-argument capture path. */
public final class JvmCapacityProbe {

    private JvmCapacityProbe() {
    }

    public static void main(String[] args) {
        JvmCapacityEnvelope envelope = JvmCapacityEnvelope.capture(System.getProperties());
        envelope.requireRssCapacity(100);
        System.out.println("CAPACITY_OK codeCache=" + envelope.reservedCodeCacheBytes());
    }
}
