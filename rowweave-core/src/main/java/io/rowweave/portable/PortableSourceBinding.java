package io.rowweave.portable;

import java.util.Arrays;
import java.util.List;

/** Canonical source provider/config/split binding; it never contains an Source object. */
public final class PortableSourceBinding implements PortableNodeBinding {
    private final String providerKey;
    private final String providerContractDigest;
    private final byte[] configurationFrame;
    private final List<byte[]> splitFrames;

    public PortableSourceBinding(
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, List<byte[]> splitFrames) {
        this.providerKey = PortableBindingSupport.key(
                providerKey, "source.providerKey");
        this.providerContractDigest = PortableBindingSupport.digest(
                providerContractDigest, "source.providerContractDigest");
        this.configurationFrame = PortableBindingSupport.frame(
                configurationFrame, PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                "source.configurationFrame");
        this.splitFrames = PortableBindingSupport.splitFrames(splitFrames);
    }

    @Override
    public PortableOperatorCode.BindingKind kind() {
        return PortableOperatorCode.BindingKind.SOURCE;
    }

    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    public List<byte[]> splitFrames() {
        return PortableBindingSupport.copyFrames(splitFrames);
    }
    byte[] configurationFrameUnsafe() { return configurationFrame; }
    List<byte[]> splitFramesUnsafe() { return splitFrames; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableSourceBinding)) return false;
        PortableSourceBinding that = (PortableSourceBinding) other;
        if (!providerKey.equals(that.providerKey)
                || !providerContractDigest.equals(that.providerContractDigest)
                || !Arrays.equals(configurationFrame, that.configurationFrame)
                || splitFrames.size() != that.splitFrames.size()) return false;
        for (int index = 0; index < splitFrames.size(); index++) {
            if (!Arrays.equals(splitFrames.get(index), that.splitFrames.get(index))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 31 * providerKey.hashCode() + providerContractDigest.hashCode();
        result = 31 * result + Arrays.hashCode(configurationFrame);
        for (byte[] frame : splitFrames) result = 31 * result + Arrays.hashCode(frame);
        return result;
    }
}
