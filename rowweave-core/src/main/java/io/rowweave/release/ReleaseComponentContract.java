package io.rowweave.release;

import java.util.Objects;

/** One explicitly classified format or SPI in a release contract. */
public final class ReleaseComponentContract implements Comparable<ReleaseComponentContract> {
    public enum Scope {
        PUBLIC_FROZEN(1), INTERNAL_SAME_BUILD(2), LEGACY_EPHEMERAL(3),
        REQUIRED_ABSENT(4);

        private final int wireId;
        Scope(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Scope fromWireId(int wireId) {
            for (Scope scope : values()) {
                if (scope.wireId == wireId) return scope;
            }
            throw ReleaseContractText.failure("UNKNOWN_SCOPE", "components.scope",
                    "unknown release component scope");
        }
    }

    private final String componentId;
    private final int formatVersion;
    private final int minimumReaderRevision;
    private final int maximumReaderRevision;
    private final int minimumWriterRevision;
    private final int maximumWriterRevision;
    private final Scope scope;
    private final String contractDigest;
    private final boolean required;

    public ReleaseComponentContract(
            String componentId, int formatVersion,
            int minimumReaderRevision, int maximumReaderRevision,
            int minimumWriterRevision, int maximumWriterRevision,
            Scope scope, String contractDigest, boolean required) {
        requireComponentId(componentId);
        if (formatVersion <= 0 || minimumReaderRevision < 0
                || maximumReaderRevision < minimumReaderRevision
                || minimumWriterRevision < 0
                || maximumWriterRevision < minimumWriterRevision) {
            throw ReleaseContractText.failure("INVALID_VERSION", "components.version",
                    "release component version range is invalid");
        }
        if (scope == null) {
            throw ReleaseContractText.failure("UNKNOWN_SCOPE", "components.scope",
                    "release component scope is required");
        }
        ReleaseContractText.requireDigest(contractDigest, "components.contractDigest");
        this.componentId = componentId;
        this.formatVersion = formatVersion;
        this.minimumReaderRevision = minimumReaderRevision;
        this.maximumReaderRevision = maximumReaderRevision;
        this.minimumWriterRevision = minimumWriterRevision;
        this.maximumWriterRevision = maximumWriterRevision;
        this.scope = scope;
        this.contractDigest = contractDigest;
        this.required = required;
    }

    private static void requireComponentId(String value) {
        ReleaseContractText.encode(value, "components.componentId");
        if (!value.matches("rowweave\\.[a-z0-9][a-z0-9.-]*")) {
            throw ReleaseContractText.failure("INVALID_COMPONENT_ID",
                    "components.componentId", "release component ID is invalid");
        }
    }

    public String componentId() { return componentId; }
    public int formatVersion() { return formatVersion; }
    public int minimumReaderRevision() { return minimumReaderRevision; }
    public int maximumReaderRevision() { return maximumReaderRevision; }
    public int minimumWriterRevision() { return minimumWriterRevision; }
    public int maximumWriterRevision() { return maximumWriterRevision; }
    public Scope scope() { return scope; }
    public String contractDigest() { return contractDigest; }
    public boolean required() { return required; }

    @Override
    public int compareTo(ReleaseComponentContract other) {
        return componentId.compareTo(other.componentId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ReleaseComponentContract)) return false;
        ReleaseComponentContract that = (ReleaseComponentContract) other;
        return formatVersion == that.formatVersion
                && minimumReaderRevision == that.minimumReaderRevision
                && maximumReaderRevision == that.maximumReaderRevision
                && minimumWriterRevision == that.minimumWriterRevision
                && maximumWriterRevision == that.maximumWriterRevision
                && required == that.required
                && componentId.equals(that.componentId)
                && scope == that.scope
                && contractDigest.equals(that.contractDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentId, formatVersion, minimumReaderRevision,
                maximumReaderRevision, minimumWriterRevision,
                maximumWriterRevision, scope, contractDigest, required);
    }
}
