package io.rowweave.portable;

import java.util.Objects;

/** Static signed-resource contract for one portable function implementation. */
public final class PortableFunctionProviderContract {
    public static final int CURRENT_VERSION = 1;

    private final int contractVersion;
    private final String functionClassName;
    private final FunctionAbi functionAbi;
    private final String configurationCodecId;
    private final int maximumConfigurationBytes;
    private final FunctionResourceProfile maximumResources;

    public PortableFunctionProviderContract(
            int contractVersion, String functionClassName,
            FunctionAbi functionAbi, String configurationCodecId,
            int maximumConfigurationBytes,
            FunctionResourceProfile maximumResources) {
        if (contractVersion != CURRENT_VERSION) {
            throw PortablePlanText.failure("UNSUPPORTED_FUNCTION_CONTRACT",
                    "functionContract.version",
                    "function provider contract version is not supported");
        }
        this.functionClassName = PortableBindingSupport.className(
                functionClassName);
        if (functionAbi == null || maximumResources == null) {
            throw PortablePlanText.failure("INVALID_FUNCTION_CONTRACT",
                    "functionContract",
                    "function ABI and resource ceiling are required");
        }
        this.configurationCodecId = PortableBindingSupport.key(
                configurationCodecId, "functionContract.configurationCodecId");
        if (maximumConfigurationBytes <= 0
                || maximumConfigurationBytes
                > PortableBindingLimits.MAX_CONFIGURATION_BYTES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED",
                    "functionContract.maximumConfigurationBytes",
                    "function configuration bound exceeds the v1 ceiling");
        }
        this.contractVersion = contractVersion;
        this.functionAbi = functionAbi;
        this.maximumConfigurationBytes = maximumConfigurationBytes;
        this.maximumResources = maximumResources;
    }

    public int contractVersion() { return contractVersion; }
    public String functionClassName() { return functionClassName; }
    public FunctionAbi functionAbi() { return functionAbi; }
    public String configurationCodecId() { return configurationCodecId; }
    public int maximumConfigurationBytes() {
        return maximumConfigurationBytes;
    }
    public FunctionResourceProfile maximumResources() {
        return maximumResources;
    }

    public void validateReference(FunctionDeploymentReference reference) {
        if (reference == null
                || !functionClassName.equals(reference.className())
                || functionAbi != reference.functionAbi()
                || !configurationCodecId.equals(
                        reference.configurationCodecId())
                || reference.configurationFrame().length
                        > maximumConfigurationBytes
                || reference.resourceProfile().maximumHeapBytes()
                        > maximumResources.maximumHeapBytes()
                || reference.resourceProfile().maximumDirectBytes()
                        > maximumResources.maximumDirectBytes()
                || reference.resourceProfile().maximumOutputRowsPerInput()
                        > maximumResources.maximumOutputRowsPerInput()) {
            throw PortablePlanText.failure("FUNCTION_CONTRACT_MISMATCH",
                    "function.reference",
                    "function reference exceeds or differs from its static contract");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableFunctionProviderContract)) return false;
        PortableFunctionProviderContract that =
                (PortableFunctionProviderContract) other;
        return contractVersion == that.contractVersion
                && maximumConfigurationBytes == that.maximumConfigurationBytes
                && functionClassName.equals(that.functionClassName)
                && functionAbi == that.functionAbi
                && configurationCodecId.equals(that.configurationCodecId)
                && maximumResources.equals(that.maximumResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contractVersion, functionClassName, functionAbi,
                configurationCodecId, maximumConfigurationBytes,
                maximumResources);
    }
}
