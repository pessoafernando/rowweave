package io.rowweave.run;

import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.internal.BatchSourceSplitBindings;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical ownership of the ordered exact source splits assigned to one physical stage. */
public final class SourceSplitAssignment {
    private static final String DOMAIN = "rowweave-source-split-assignment-v1";

    /** One SOURCE operator and its ordered physical bucket. */
    public static final class SourceGroup {
        private final String sourceOperatorId;
        private final List<BatchSourceSplitBinding> bindings;

        private SourceGroup(String sourceOperatorId, List<BatchSourceSplitBinding> bindings) {
            this.sourceOperatorId = RunText.required(
                    sourceOperatorId, "sourceOperatorId", RunText.MAX_ID_BYTES);
            if (bindings == null || bindings.isEmpty()
                    || bindings.size() > StageAssignmentManifest.MAX_ASSIGNMENTS) {
                throw new IllegalArgumentException(
                        "source assignment binding count must be in [1,"
                                + StageAssignmentManifest.MAX_ASSIGNMENTS + "]");
            }
            List<BatchSourceSplitBinding> copy = new ArrayList<>(bindings.size());
            Set<String> splitIds = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                BatchSourceSplitBinding binding = bindings.get(i);
                if (binding == null) {
                    throw new IllegalArgumentException(
                            "source assignment binding is required at " + i);
                }
                BatchSourceSplitDescriptor descriptor = binding.descriptor();
                if (descriptor.replayGuarantee()
                        != BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START) {
                    throw new IllegalArgumentException(
                            "source assignment requires exact replay at binding " + i);
                }
                if (!splitIds.add(descriptor.splitId())) {
                    throw new IllegalArgumentException(
                            "duplicate source splitId in operator " + sourceOperatorId);
                }
                copy.add(binding);
            }
            BatchSourceSplitBindings.validateCurrent(copy);
            this.bindings = Collections.unmodifiableList(copy);
        }

        public static SourceGroup create(
                String sourceOperatorId, List<BatchSourceSplitBinding> bindings) {
            return new SourceGroup(sourceOperatorId, bindings);
        }

        public String sourceOperatorId() { return sourceOperatorId; }
        public List<BatchSourceSplitBinding> bindings() { return bindings; }
    }

    /** Bounded routing-only projection; it never retains Source or descriptor objects. */
    public static final class Context implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final class SourceGroupContext implements Serializable {
            private static final long serialVersionUID = 1L;
            private final String sourceOperatorId;
            private final int sourceSplitCount;

            private SourceGroupContext(String sourceOperatorId, int sourceSplitCount) {
                this.sourceOperatorId = RunText.required(
                        sourceOperatorId, "sourceOperatorId", RunText.MAX_ID_BYTES);
                if (sourceSplitCount <= 0
                        || sourceSplitCount > StageAssignmentManifest.MAX_ASSIGNMENTS) {
                    throw new IllegalArgumentException(
                            "source group split count is out of bounds");
                }
                this.sourceSplitCount = sourceSplitCount;
            }

            public static SourceGroupContext create(
                    String sourceOperatorId, int sourceSplitCount) {
                return new SourceGroupContext(sourceOperatorId, sourceSplitCount);
            }

            public String sourceOperatorId() { return sourceOperatorId; }
            public int sourceSplitCount() { return sourceSplitCount; }

            private Object readResolve() throws ObjectStreamException {
                return new SourceGroupContext(sourceOperatorId, sourceSplitCount);
            }
        }

        private final String artifactId;
        private final int logicalStage;
        private final int instanceIndex;
        private final List<SourceGroupContext> sourceGroups;
        private final int sourceSplitCount;
        private final byte[] ownershipDigest;

        private Context(String artifactId, int logicalStage, int instanceIndex,
                        List<SourceGroupContext> sourceGroups,
                        int sourceSplitCount, byte[] ownershipDigest) {
            this.artifactId = RunText.required(
                    artifactId, "artifactId", RunText.MAX_ID_BYTES);
            if (logicalStage < 0 || instanceIndex < 0) {
                throw new IllegalArgumentException(
                        "source context physical identity is invalid");
            }
            if (sourceGroups == null || sourceGroups.isEmpty()
                    || sourceGroups.size() > StageAssignmentManifest.MAX_ASSIGNMENTS) {
                throw new IllegalArgumentException("source context group count is out of bounds");
            }
            List<SourceGroupContext> copy = new ArrayList<>(sourceGroups.size());
            Set<String> ids = new HashSet<>();
            int counted = 0;
            for (SourceGroupContext group : sourceGroups) {
                if (group == null || !ids.add(group.sourceOperatorId())) {
                    throw new IllegalArgumentException(
                            "source context groups require unique operator ids");
                }
                if (counted > StageAssignmentManifest.MAX_ASSIGNMENTS
                        - group.sourceSplitCount()) {
                    throw new IllegalArgumentException("source context split count exceeds limit");
                }
                counted += group.sourceSplitCount();
                copy.add(group);
            }
            if (counted != sourceSplitCount) {
                throw new IllegalArgumentException("source context split count mismatch");
            }
            if (ownershipDigest == null || ownershipDigest.length != 32) {
                throw new IllegalArgumentException("source context ownershipDigest must be SHA-256");
            }
            this.logicalStage = logicalStage;
            this.instanceIndex = instanceIndex;
            this.sourceGroups = Collections.unmodifiableList(copy);
            this.sourceSplitCount = sourceSplitCount;
            this.ownershipDigest = ownershipDigest.clone();
        }

        public static Context create(
                String artifactId, int logicalStage, int instanceIndex,
                List<SourceGroupContext> sourceGroups, int sourceSplitCount,
                byte[] ownershipDigest) {
            return new Context(artifactId, logicalStage, instanceIndex,
                    sourceGroups, sourceSplitCount, ownershipDigest);
        }

        public boolean matches(SourceSplitAssignment assignment) {
            if (assignment == null
                    || !artifactId.equals(assignment.artifactId())
                    || logicalStage != assignment.logicalStage()
                    || instanceIndex != assignment.instanceIndex()
                    || sourceSplitCount != assignment.sourceSplitCount()
                    || !MessageDigest.isEqual(
                    ownershipDigest, assignment.ownershipDigest())
                    || sourceGroups.size() != assignment.sourceGroups().size()) {
                return false;
            }
            for (int i = 0; i < sourceGroups.size(); i++) {
                SourceGroupContext context = sourceGroups.get(i);
                SourceGroup group = assignment.sourceGroups().get(i);
                if (!context.sourceOperatorId().equals(group.sourceOperatorId())
                        || context.sourceSplitCount() != group.bindings().size()) {
                    return false;
                }
            }
            return true;
        }

        public SourceSplitAssignment bind(
                List<List<BatchSourceSplitBinding>> bindingGroups) {
            if (bindingGroups == null || bindingGroups.size() != sourceGroups.size()) {
                throw new IllegalArgumentException("source context binding group count mismatch");
            }
            List<SourceGroup> groups = new ArrayList<>(bindingGroups.size());
            for (int i = 0; i < bindingGroups.size(); i++) {
                List<BatchSourceSplitBinding> bindings = bindingGroups.get(i);
                if (bindings == null
                        || bindings.size() != sourceGroups.get(i).sourceSplitCount()) {
                    throw new IllegalArgumentException(
                            "source context binding count mismatch at group " + i);
                }
                groups.add(SourceGroup.create(
                        sourceGroups.get(i).sourceOperatorId(), bindings));
            }
            SourceSplitAssignment assignment = SourceSplitAssignment.create(
                    artifactId, logicalStage, instanceIndex, groups);
            if (!matches(assignment)) {
                throw new IllegalArgumentException("source ownership digest mismatch");
            }
            return assignment;
        }

        public String artifactId() { return artifactId; }
        public int logicalStage() { return logicalStage; }
        public int instanceIndex() { return instanceIndex; }
        public List<SourceGroupContext> sourceGroups() { return sourceGroups; }
        public int sourceSplitCount() { return sourceSplitCount; }
        public byte[] ownershipDigest() { return ownershipDigest.clone(); }

        private Object readResolve() throws ObjectStreamException {
            return new Context(artifactId, logicalStage, instanceIndex,
                    sourceGroups, sourceSplitCount, ownershipDigest);
        }
    }

    private final String artifactId;
    private final int logicalStage;
    private final int instanceIndex;
    private final List<SourceGroup> sourceGroups;
    private final int sourceSplitCount;
    private final byte[] ownershipDigest;

    private SourceSplitAssignment(String artifactId, int logicalStage, int instanceIndex,
                                  List<SourceGroup> sourceGroups) {
        this.artifactId = RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (logicalStage < 0) throw new IllegalArgumentException("logicalStage must be >= 0");
        if (instanceIndex < 0) throw new IllegalArgumentException("instanceIndex must be >= 0");
        if (sourceGroups == null || sourceGroups.isEmpty()
                || sourceGroups.size() > StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException(
                    "source group count must be in [1,"
                            + StageAssignmentManifest.MAX_ASSIGNMENTS + "]");
        }
        List<SourceGroup> copy = new ArrayList<>(sourceGroups.size());
        Set<String> operatorIds = new HashSet<>();
        int splits = 0;
        for (int i = 0; i < sourceGroups.size(); i++) {
            SourceGroup group = sourceGroups.get(i);
            if (group == null) {
                throw new IllegalArgumentException("source group is required at " + i);
            }
            if (!operatorIds.add(group.sourceOperatorId())) {
                throw new IllegalArgumentException(
                        "duplicate sourceOperatorId: " + group.sourceOperatorId());
            }
            if (splits > StageAssignmentManifest.MAX_ASSIGNMENTS - group.bindings().size()) {
                throw new IllegalArgumentException("source split count exceeds assignment limit");
            }
            splits += group.bindings().size();
            copy.add(group);
        }
        this.logicalStage = logicalStage;
        this.instanceIndex = instanceIndex;
        this.sourceGroups = Collections.unmodifiableList(copy);
        this.sourceSplitCount = splits;
        this.ownershipDigest = digestCanonical();
    }

    public static SourceSplitAssignment create(
            String artifactId, int logicalStage, int instanceIndex,
            List<SourceGroup> sourceGroups) {
        return new SourceSplitAssignment(
                artifactId, logicalStage, instanceIndex, sourceGroups);
    }

    private byte[] digestCanonical() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, DOMAIN);
            RunWire.writeText(out, artifactId);
            out.writeInt(logicalStage);
            out.writeInt(instanceIndex);
            out.writeInt(sourceGroups.size());
            for (SourceGroup group : sourceGroups) {
                RunWire.writeText(out, group.sourceOperatorId());
                out.writeInt(group.bindings().size());
                for (BatchSourceSplitBinding binding : group.bindings()) {
                    RunWire.writeText(out, binding.descriptor().splitId());
                    out.write(BatchSourceSplitDescriptorCodec.digest(binding.descriptor()));
                }
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public String artifactId() { return artifactId; }
    public int logicalStage() { return logicalStage; }
    public int instanceIndex() { return instanceIndex; }
    public List<SourceGroup> sourceGroups() { return sourceGroups; }
    public int sourceSplitCount() { return sourceSplitCount; }
    public byte[] ownershipDigest() { return ownershipDigest.clone(); }
    public Context context() {
        List<Context.SourceGroupContext> groups = new ArrayList<>(sourceGroups.size());
        for (SourceGroup group : sourceGroups) {
            groups.add(Context.SourceGroupContext.create(
                    group.sourceOperatorId(), group.bindings().size()));
        }
        return Context.create(artifactId, logicalStage, instanceIndex,
                groups, sourceSplitCount, ownershipDigest);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SourceSplitAssignment)) return false;
        SourceSplitAssignment that = (SourceSplitAssignment) other;
        return logicalStage == that.logicalStage
                && instanceIndex == that.instanceIndex
                && sourceSplitCount == that.sourceSplitCount
                && artifactId.equals(that.artifactId)
                && MessageDigest.isEqual(ownershipDigest, that.ownershipDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(artifactId, logicalStage, instanceIndex, sourceSplitCount)
                + java.util.Arrays.hashCode(ownershipDigest);
    }
}
