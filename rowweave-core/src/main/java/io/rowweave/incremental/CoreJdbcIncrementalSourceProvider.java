package io.rowweave.incremental;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.SourceExecutor;
import io.rowweave.JdbcSnapshotReadRequest;

import java.util.ServiceLoader;

/** Shared exact-profile adapter; source rows still stream on the source stage. */
abstract class CoreJdbcIncrementalSourceProvider
        implements IncrementalSourceProvider {
    private static final long serialVersionUID = 1L;
    private final IncrementalFrontier.Kind frontierKind;
    private final transient JdbcIncrementalSourceAccess directAccess;

    CoreJdbcIncrementalSourceProvider(
            IncrementalFrontier.Kind frontierKind,
            JdbcIncrementalSourceAccess directAccess) {
        this.frontierKind = frontierKind;
        this.directAccess = directAccess;
    }

    @Override
    public final IncrementalSourceProviderContract contract() {
        return frontierKind == IncrementalFrontier.Kind.INT64_SEQUENCE_V1
                ? JdbcIncrementalSourceProfile.sequenceContract()
                : JdbcIncrementalSourceProfile.timestampKeyContract();
    }

    @Override
    public final byte[] canonicalizeConfiguration(
            IncrementalSourceConfig configuration) {
        return profile(configuration).canonicalizeConfiguration(configuration);
    }

    @Override
    public final IncrementalCaptureResponse captureTarget(
            IncrementalSourceConfig configuration,
            IncrementalCaptureRequest request) {
        JdbcIncrementalSourceProfile profile = profile(configuration);
        JdbcIncrementalSourceAccess.Capture captured =
                access(profile).capture(profile, request);
        if (captured == null) {
            throw new IllegalArgumentException("JDBC_PROFILE_CAPTURE_INVALID");
        }
        return profile.capture(
                request, captured.target(), captured.snapshot());
    }

    @Override
    public final IncrementalMaterializationResponse materializeInterval(
            IncrementalSourceConfig configuration,
            CapturedTargetReceipt capturedTarget) {
        JdbcIncrementalSourceProfile profile = profile(configuration);
        JdbcIncrementalSourceProfile.Snapshot snapshot =
                JdbcIncrementalSourceProfile.decodeSnapshotIdentity(
                        capturedTarget.snapshotIdentity());
        return profile.materialize(capturedTarget, snapshot,
                access(profile).materialize(
                        profile, capturedTarget, snapshot));
    }

    @Override
    public final SourceExecutor<IncrementalSourceSplit> openSplit(
            IncrementalSourceSplit input) {
        JdbcIncrementalSourceProfile profile =
                profile(input.configuration());
        JdbcSnapshotReadRequest request =
                JdbcIncrementalSourceProfile.decodeOpenRequest(
                        input.openRequest());
        profile.verifyOpenRequest(input, request);
        SourceExecutor<IncrementalSourceSplit> opened =
                access(profile).open(profile, input, request);
        if (opened == null) {
            throw new IllegalArgumentException("JDBC_PROFILE_READER_MISSING");
        }
        return opened;
    }

    private JdbcIncrementalSourceProfile profile(
            IncrementalSourceConfig configuration) {
        return JdbcIncrementalSourceProfile.bind(
                configuration, frontierKind);
    }

    private JdbcIncrementalSourceAccess access(
            JdbcIncrementalSourceProfile profile) {
        if (directAccess != null) {
            if (!directAccess.supports(profile)) {
                throw new IllegalArgumentException(
                        "JDBC_PROFILE_ACCESS_UNAVAILABLE");
            }
            return directAccess;
        }
        JdbcIncrementalSourceAccess found = null;
        int candidates = 0;
        for (JdbcIncrementalSourceAccess candidate
                : ServiceLoader.load(JdbcIncrementalSourceAccess.class)) {
            if (++candidates > 32) {
                throw new IllegalArgumentException(
                        "JDBC_PROFILE_ACCESS_AMBIGUOUS");
            }
            if (!candidate.supports(profile)) continue;
            if (found != null) {
                throw new IllegalArgumentException(
                        "JDBC_PROFILE_ACCESS_AMBIGUOUS");
            }
            found = candidate;
        }
        if (found == null) {
            throw new IllegalArgumentException(
                    "JDBC_PROFILE_ACCESS_UNAVAILABLE");
        }
        return found;
    }
}
