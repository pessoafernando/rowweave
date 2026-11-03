package io.rowweave.portable;

import java.util.Arrays;

/** Canonical sink provider/descriptor/config binding; it never contains an Sink object. */
public final class PortableSinkBinding implements PortableNodeBinding {
    private final String providerKey;
    private final String providerContractDigest;
    private final byte[] descriptorFrame;
    private final byte[] configurationFrame;

    public PortableSinkBinding(
            String providerKey, String providerContractDigest,
            byte[] descriptorFrame, byte[] configurationFrame) {
        this.providerKey = PortableBindingSupport.key(
                providerKey, "sink.providerKey");
        this.providerContractDigest = PortableBindingSupport.digest(
                providerContractDigest, "sink.providerContractDigest");
        this.descriptorFrame = PortableBindingSupport.frame(
                descriptorFrame, PortableBindingLimits.MAX_DESCRIPTOR_BYTES,
                "sink.descriptorFrame");
        this.configurationFrame = PortableBindingSupport.frame(
                configurationFrame, PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                "sink.configurationFrame");
    }

    @Override
    public PortableOperatorCode.BindingKind kind() {
        return PortableOperatorCode.BindingKind.SINK;
    }

    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public byte[] descriptorFrame() { return descriptorFrame.clone(); }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    byte[] descriptorFrameUnsafe() { return descriptorFrame; }
    byte[] configurationFrameUnsafe() { return configurationFrame; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableSinkBinding)) return false;
        PortableSinkBinding that = (PortableSinkBinding) other;
        return providerKey.equals(that.providerKey)
                && providerContractDigest.equals(that.providerContractDigest)
                && Arrays.equals(descriptorFrame, that.descriptorFrame)
                && Arrays.equals(configurationFrame, that.configurationFrame);
    }

    @Override
    public int hashCode() {
        int result = 31 * providerKey.hashCode() + providerContractDigest.hashCode();
        result = 31 * result + Arrays.hashCode(descriptorFrame);
        return 31 * result + Arrays.hashCode(configurationFrame);
    }
}
