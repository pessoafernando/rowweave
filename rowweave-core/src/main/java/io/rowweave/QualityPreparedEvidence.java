package io.rowweave;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounded immutable control-plane projection of sealed quality members. */
public final class QualityPreparedEvidence {
    private final long acceptedTotal;
    private final long rejectedTotal;
    private final Map<QualityReasonKey, Long> reasonObservations;
    private final List<QualityExemplar> exemplars;
    private final List<QualityPreparedMemberEvidence> members;

    QualityPreparedEvidence(long acceptedTotal, long rejectedTotal,
                            Map<QualityReasonKey, Long> reasonObservations,
                            List<QualityExemplar> exemplars,
                            List<QualityPreparedMemberEvidence> members) {
        if (acceptedTotal < 0L || rejectedTotal < 0L) {
            throw new IllegalArgumentException("quality totals are negative");
        }
        if (reasonObservations == null
                || reasonObservations.size() > QualityGatePolicy.MAX_REASON_THRESHOLDS) {
            throw new IllegalArgumentException("quality observations exceed their bound");
        }
        if (exemplars == null || exemplars.size() > QualityGatePolicy.MAX_EXEMPLARS) {
            throw new IllegalArgumentException("quality exemplars exceed their bound");
        }
        if (members == null || members.size() != 3) {
            throw new IllegalArgumentException("quality evidence requires three members");
        }
        this.acceptedTotal = acceptedTotal;
        this.rejectedTotal = rejectedTotal;
        this.reasonObservations = Collections.unmodifiableMap(
                new LinkedHashMap<>(reasonObservations));
        this.exemplars = Collections.unmodifiableList(new ArrayList<>(exemplars));
        this.members = Collections.unmodifiableList(new ArrayList<>(members));
    }

    public long acceptedTotal() { return acceptedTotal; }
    public long rejectedTotal() { return rejectedTotal; }
    public long observedReason(QualityReasonKey reason) {
        Long observed = reasonObservations.get(reason);
        return observed == null ? 0L : observed;
    }
    public Map<QualityReasonKey, Long> reasonObservations() {
        return reasonObservations;
    }
    public List<QualityExemplar> exemplars() { return exemplars; }
    public List<QualityPreparedMemberEvidence> members() { return members; }
}
