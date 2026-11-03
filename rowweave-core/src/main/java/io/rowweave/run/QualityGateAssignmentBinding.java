package io.rowweave.run;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Master-authored immutable quality identity frozen into one assignment root. */
public final class QualityGateAssignmentBinding {
    public static final int MEMBER_COUNT = 3;
    private static final int DIGEST_BYTES = 32;
    private static final List<String> CANONICAL_ROLES = Collections.unmodifiableList(
            Arrays.asList("typed-quality.valid", "typed-quality.rejected",
                    "typed-quality.summary"));

    private final byte[] policyDigest;
    private final List<MemberBinding> members;

    public QualityGateAssignmentBinding(
            byte[] policyDigest, List<MemberBinding> members) {
        requireDigest(policyDigest, "policyDigest");
        if (members == null || members.size() != MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "quality assignment requires exactly three member bindings");
        }
        List<MemberBinding> copy = new ArrayList<>(members.size());
        Set<Integer> ordinals = new HashSet<>();
        Set<String> roles = new HashSet<>();
        for (int i = 0; i < members.size(); i++) {
            MemberBinding member = members.get(i);
            if (member == null || !ordinals.add(member.physicalOrdinal())
                    || !roles.add(member.role())) {
                throw new IllegalArgumentException(
                        "quality assignment member bindings must be unique");
            }
            if (!CANONICAL_ROLES.get(i).equals(member.role())) {
                throw new IllegalArgumentException(
                        "quality assignment members must use canonical role order");
            }
            copy.add(member);
        }
        if (!roles.equals(new HashSet<>(CANONICAL_ROLES))) {
            throw new IllegalArgumentException(
                    "quality assignment roles must be the reserved closure");
        }
        this.policyDigest = policyDigest.clone();
        this.members = Collections.unmodifiableList(copy);
    }

    public byte[] policyDigest() { return policyDigest.clone(); }
    public List<MemberBinding> members() { return members; }
    public static List<String> canonicalRoles() { return CANONICAL_ROLES; }

    public MemberBinding memberForOrdinal(int physicalOrdinal) {
        for (MemberBinding member : members) {
            if (member.physicalOrdinal() == physicalOrdinal) return member;
        }
        return null;
    }

    public boolean matches(int physicalOrdinal, SinkAttemptIdentity identity) {
        MemberBinding expected = memberForOrdinal(physicalOrdinal);
        return expected != null && identity != null
                && expected.role().equals(identity.sinkOperatorId())
                && MessageDigest.isEqual(
                expected.descriptorDigest(), identity.descriptorDigest());
    }

    private static void requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityGateAssignmentBinding)) return false;
        QualityGateAssignmentBinding that = (QualityGateAssignmentBinding) other;
        return MessageDigest.isEqual(policyDigest, that.policyDigest)
                && members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(policyDigest) + members.hashCode();
    }

    /** One trusted physical assignment to reserved-role/descriptor binding. */
    public static final class MemberBinding {
        private final int physicalOrdinal;
        private final String role;
        private final byte[] descriptorDigest;

        public MemberBinding(
                int physicalOrdinal, String role, byte[] descriptorDigest) {
            if (physicalOrdinal < 0) {
                throw new IllegalArgumentException(
                        "quality member physicalOrdinal must be >= 0");
            }
            if (!CANONICAL_ROLES.contains(role)) {
                throw new IllegalArgumentException(
                        "quality member role must be reserved");
            }
            requireDigest(descriptorDigest, "descriptorDigest");
            this.physicalOrdinal = physicalOrdinal;
            this.role = role;
            this.descriptorDigest = descriptorDigest.clone();
        }

        public int physicalOrdinal() { return physicalOrdinal; }
        public String role() { return role; }
        public byte[] descriptorDigest() { return descriptorDigest.clone(); }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof MemberBinding)) return false;
            MemberBinding that = (MemberBinding) other;
            return physicalOrdinal == that.physicalOrdinal
                    && role.equals(that.role)
                    && MessageDigest.isEqual(descriptorDigest, that.descriptorDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(physicalOrdinal, role,
                    Arrays.hashCode(descriptorDigest));
        }
    }
}
