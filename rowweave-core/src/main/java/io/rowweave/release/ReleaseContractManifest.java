package io.rowweave.release;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Canonical release identity and exact component compatibility inventory. */
public final class ReleaseContractManifest {
    public static final int SCHEMA_VERSION = 1;

    private final String releaseId;
    private final int protocolEpoch;
    private final int releaseRevision;
    private final int minimumReadableRevision;
    private final int minimumWritableRevision;
    private final List<ReleaseComponentContract> componentContracts;
    private final List<String> runtimeCapabilities;
    private final String publicApiBaselineDigest;
    private final String decimalSemanticsId;
    private final String manifestDigest;

    public ReleaseContractManifest(
            String releaseId, int protocolEpoch, int releaseRevision,
            int minimumReadableRevision, int minimumWritableRevision,
            Collection<ReleaseComponentContract> componentContracts,
            Collection<String> runtimeCapabilities,
            String publicApiBaselineDigest, String decimalSemanticsId) {
        ReleaseContractText.encode(releaseId, "releaseId");
        ReleaseContractText.encode(decimalSemanticsId, "decimalSemanticsId");
        if (protocolEpoch <= 0 || releaseRevision < 0
                || minimumReadableRevision < 0
                || minimumReadableRevision > releaseRevision
                || minimumWritableRevision < 0
                || minimumWritableRevision > releaseRevision) {
            throw ReleaseContractText.failure("INVALID_VERSION", "release.version",
                    "release version range is invalid");
        }
        ReleaseContractText.requireDigest(
                publicApiBaselineDigest, "publicApiBaselineDigest");
        if (componentContracts == null || componentContracts.isEmpty()
                || componentContracts.size() > ReleaseContractManifestCodec.MAX_COMPONENTS) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "components",
                    "release component count is invalid");
        }
        List<ReleaseComponentContract> components =
                new ArrayList<>(componentContracts.size());
        Set<String> ids = new HashSet<>();
        for (ReleaseComponentContract component : componentContracts) {
            if (component == null) {
                throw ReleaseContractText.failure("INVALID_COMPONENT", "components",
                        "release component is required");
            }
            if (!ids.add(component.componentId())) {
                throw ReleaseContractText.failure("DUPLICATE_COMPONENT", "components",
                        "release component ID is duplicated");
            }
            components.add(component);
        }
        Collections.sort(components);
        this.runtimeCapabilities = canonicalCapabilities(runtimeCapabilities);
        this.releaseId = releaseId;
        this.protocolEpoch = protocolEpoch;
        this.releaseRevision = releaseRevision;
        this.minimumReadableRevision = minimumReadableRevision;
        this.minimumWritableRevision = minimumWritableRevision;
        this.componentContracts = Collections.unmodifiableList(components);
        this.publicApiBaselineDigest = publicApiBaselineDigest;
        this.decimalSemanticsId = decimalSemanticsId;
        this.manifestDigest = ReleaseContractManifestCodec.hex(
                ReleaseContractManifestCodec.sha256(
                        ReleaseContractManifestCodec.encodeBody(this)));
    }

    private static List<String> canonicalCapabilities(Collection<String> values) {
        if (values == null || values.isEmpty()
                || values.size() > ReleaseContractManifestCodec.MAX_CAPABILITIES) {
            throw ReleaseContractText.failure("LIMIT_EXCEEDED", "runtimeCapabilities",
                    "runtime capability count is invalid");
        }
        TreeSet<String> sorted = new TreeSet<>();
        for (String value : values) {
            ReleaseContractText.encode(value, "runtimeCapabilities");
            if (!sorted.add(value)) {
                throw ReleaseContractText.failure("DUPLICATE_CAPABILITY",
                        "runtimeCapabilities", "runtime capability is duplicated");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    public String releaseId() { return releaseId; }
    public int protocolEpoch() { return protocolEpoch; }
    public int releaseRevision() { return releaseRevision; }
    public int minimumReadableRevision() { return minimumReadableRevision; }
    public int minimumWritableRevision() { return minimumWritableRevision; }
    public List<ReleaseComponentContract> componentContracts() { return componentContracts; }
    public List<String> runtimeCapabilities() { return runtimeCapabilities; }
    public String publicApiBaselineDigest() { return publicApiBaselineDigest; }
    public String decimalSemanticsId() { return decimalSemanticsId; }
    public String manifestDigest() { return manifestDigest; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ReleaseContractManifest)) return false;
        ReleaseContractManifest that = (ReleaseContractManifest) other;
        return protocolEpoch == that.protocolEpoch
                && releaseRevision == that.releaseRevision
                && minimumReadableRevision == that.minimumReadableRevision
                && minimumWritableRevision == that.minimumWritableRevision
                && releaseId.equals(that.releaseId)
                && componentContracts.equals(that.componentContracts)
                && runtimeCapabilities.equals(that.runtimeCapabilities)
                && publicApiBaselineDigest.equals(that.publicApiBaselineDigest)
                && decimalSemanticsId.equals(that.decimalSemanticsId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(releaseId, protocolEpoch, releaseRevision,
                minimumReadableRevision, minimumWritableRevision,
                componentContracts, runtimeCapabilities,
                publicApiBaselineDigest, decimalSemanticsId);
    }
}
