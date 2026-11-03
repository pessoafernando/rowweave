package io.rowweave.portable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Exact signed deployment and configuration identity for one portable function. */
public final class FunctionDeploymentReference implements PortableNodeBinding {
    private final String deploymentSetId;
    private final String deploymentId;
    private final String deploymentJarDigest;
    private final String className;
    private final FunctionAbi functionAbi;
    private final String configurationCodecId;
    private final String configurationDigest;
    private final byte[] configurationFrame;
    private final FunctionResourceProfile resourceProfile;

    public FunctionDeploymentReference(
            String deploymentSetId, String deploymentId,
            String deploymentJarDigest, String className,
            FunctionAbi functionAbi, String configurationCodecId,
            String configurationDigest, byte[] configurationFrame,
            FunctionResourceProfile resourceProfile) {
        this.deploymentSetId = PortableBindingSupport.digest(
                deploymentSetId, "function.deploymentSetId");
        this.deploymentId = PortableBindingSupport.key(
                deploymentId, "function.deploymentId");
        this.deploymentJarDigest = PortableBindingSupport.digest(
                deploymentJarDigest, "function.deploymentJarDigest");
        this.className = PortableBindingSupport.className(className);
        if (functionAbi == null) {
            throw PortablePlanText.failure("INVALID_BINDING", "function.abi",
                    "function ABI is required");
        }
        this.functionAbi = functionAbi;
        this.configurationCodecId = PortableBindingSupport.key(
                configurationCodecId, "function.configurationCodecId");
        this.configurationDigest = PortableBindingSupport.digest(
                configurationDigest, "function.configurationDigest");
        this.configurationFrame = PortableBindingSupport.frame(
                configurationFrame, PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                "function.configurationFrame");
        String actualConfigurationDigest = PortablePlanText.hex(
                PortablePlanText.sha256(this.configurationFrame));
        if (!MessageDigest.isEqual(
                this.configurationDigest.getBytes(StandardCharsets.US_ASCII),
                actualConfigurationDigest.getBytes(StandardCharsets.US_ASCII))) {
            throw PortablePlanText.failure("CONFIG_DIGEST_MISMATCH",
                    "function.configurationDigest",
                    "function configuration digest does not match its frame");
        }
        if (resourceProfile == null) {
            throw PortablePlanText.failure("INVALID_BINDING", "function.resources",
                    "function resource profile is required");
        }
        this.resourceProfile = resourceProfile;
    }

    @Override
    public PortableOperatorCode.BindingKind kind() {
        return PortableOperatorCode.BindingKind.FUNCTION;
    }

    public String deploymentSetId() { return deploymentSetId; }
    public String deploymentId() { return deploymentId; }
    public String deploymentJarDigest() { return deploymentJarDigest; }
    public String className() { return className; }
    public FunctionAbi functionAbi() { return functionAbi; }
    public String configurationCodecId() { return configurationCodecId; }
    public String configurationDigest() { return configurationDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    public FunctionResourceProfile resourceProfile() { return resourceProfile; }
    byte[] configurationFrameUnsafe() { return configurationFrame; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof FunctionDeploymentReference)) return false;
        FunctionDeploymentReference that = (FunctionDeploymentReference) other;
        return deploymentSetId.equals(that.deploymentSetId)
                && deploymentId.equals(that.deploymentId)
                && deploymentJarDigest.equals(that.deploymentJarDigest)
                && className.equals(that.className)
                && functionAbi == that.functionAbi
                && configurationCodecId.equals(that.configurationCodecId)
                && configurationDigest.equals(that.configurationDigest)
                && Arrays.equals(configurationFrame, that.configurationFrame)
                && resourceProfile.equals(that.resourceProfile);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(deploymentSetId, deploymentId,
                deploymentJarDigest, className, functionAbi,
                configurationCodecId, configurationDigest, resourceProfile);
        return 31 * result + Arrays.hashCode(configurationFrame);
    }
}
