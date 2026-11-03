package io.rowweave.monitor;

import io.rowweave.commit.BatchSinkOutcome;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded, redacted monitor projection for one Plan's batch Sinks. */
public final class BatchSinkView {
    public static final String DOMAIN_NONE = "NONE";
    public static final String DOMAIN_SAME = "SAME_DOMAIN";
    public static final String DOMAIN_CROSS = "CROSS_DOMAIN";
    private static final int MAX_SINKS = 4096;

    private final int sinkCount;
    private final List<String> capabilityProfiles;
    private final String commitDomainDisposition;
    private final List<String> descriptorDigestPrefixes;
    private final int locatorClosureCount;
    private final String outcomeState;

    private BatchSinkView(
            int sinkCount,
            List<String> capabilityProfiles,
            String commitDomainDisposition,
            List<String> descriptorDigestPrefixes,
            int locatorClosureCount,
            String outcomeState) {
        if (sinkCount < 0 || sinkCount > MAX_SINKS) {
            throw new IllegalArgumentException("sinkCount is out of bounds");
        }
        if (capabilityProfiles == null
                || capabilityProfiles.size() != sinkCount) {
            throw new IllegalArgumentException(
                    "capabilityProfiles must align with sinkCount");
        }
        if (descriptorDigestPrefixes == null
                || descriptorDigestPrefixes.size() != sinkCount) {
            throw new IllegalArgumentException(
                    "descriptorDigestPrefixes must align with sinkCount");
        }
        if (!DOMAIN_NONE.equals(commitDomainDisposition)
                && !DOMAIN_SAME.equals(commitDomainDisposition)
                && !DOMAIN_CROSS.equals(commitDomainDisposition)) {
            throw new IllegalArgumentException(
                    "unknown commitDomainDisposition");
        }
        if ((sinkCount == 0) != DOMAIN_NONE.equals(commitDomainDisposition)) {
            throw new IllegalArgumentException(
                    "empty Sink summary must use NONE domain disposition");
        }
        if (locatorClosureCount < 0 || locatorClosureCount > sinkCount) {
            throw new IllegalArgumentException(
                    "locatorClosureCount is out of bounds");
        }
        List<String> profiles = new ArrayList<>(sinkCount);
        List<String> prefixes = new ArrayList<>(sinkCount);
        for (int index = 0; index < sinkCount; index++) {
            String profile = capabilityProfiles.get(index);
            if (profile == null || profile.isEmpty() || profile.length() > 64) {
                throw new IllegalArgumentException("invalid Sink capability profile");
            }
            String prefix = descriptorDigestPrefixes.get(index);
            if (prefix == null || !prefix.matches("[0-9a-f]{16}")) {
                throw new IllegalArgumentException(
                        "descriptor digest prefix must be 16 lowercase hex chars");
            }
            profiles.add(profile);
            prefixes.add(prefix);
        }
        if (outcomeState != null) {
            try {
                BatchSinkOutcome.State.valueOf(outcomeState);
            } catch (IllegalArgumentException invalid) {
                throw new IllegalArgumentException("unknown batch Sink outcome state");
            }
        }
        this.sinkCount = sinkCount;
        this.capabilityProfiles = Collections.unmodifiableList(profiles);
        this.commitDomainDisposition = commitDomainDisposition;
        this.descriptorDigestPrefixes = Collections.unmodifiableList(prefixes);
        this.locatorClosureCount = locatorClosureCount;
        this.outcomeState = outcomeState;
    }

    public static BatchSinkView of(
            int sinkCount,
            List<String> capabilityProfiles,
            String commitDomainDisposition,
            List<String> descriptorDigestPrefixes,
            int locatorClosureCount,
            String outcomeState) {
        return new BatchSinkView(sinkCount, capabilityProfiles,
                commitDomainDisposition, descriptorDigestPrefixes,
                locatorClosureCount, outcomeState);
    }

    public int sinkCount() { return sinkCount; }
    public List<String> capabilityProfiles() { return capabilityProfiles; }
    public String commitDomainDisposition() { return commitDomainDisposition; }
    public List<String> descriptorDigestPrefixes() {
        return descriptorDigestPrefixes;
    }
    public int locatorClosureCount() { return locatorClosureCount; }
    public String outcomeState() { return outcomeState; }
}
