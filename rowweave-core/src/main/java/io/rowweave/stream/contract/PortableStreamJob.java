package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCheckpointMode;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import java.util.Objects;

/** Product Stream envelope: frozen logical plan plus operational policy. */
public final class PortableStreamJob {
    public static final int CURRENT_VERSION = 1;

    private final PortableStreamPlan plan;
    private final StreamCheckpointConfig checkpointConfig;
    private final StreamRestartPolicy restartPolicy;
    private final StreamResourceProfile resourceProfile;

    public PortableStreamJob(PortableStreamPlan plan,
            StreamCheckpointConfig checkpointConfig,
            StreamRestartPolicy restartPolicy,
            StreamResourceProfile resourceProfile) {
        if (plan == null || checkpointConfig == null || restartPolicy == null
                || resourceProfile == null) {
            throw new IllegalArgumentException("Stream product job fields are required");
        }
        if (plan.deliveryGuarantee()
                == PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE
                && !checkpointConfig.periodic()) {
            throw StreamContractSupport.failure("CHECKPOINT_POLICY_MISMATCH",
                    "job.checkpoint", "exactly-once requires periodic checkpoints");
        }
        if (plan.deliveryGuarantee()
                == PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE
                && resourceProfile.checkpointBytes() <= 0L) {
            throw StreamContractSupport.failure(
                    "INVALID_JOB_RESOURCE_CEILING",
                    "job.resources.checkpointBytes",
                    "exactly-once requires positive durable checkpoint storage");
        }
        if (checkpointConfig.mode() == StreamCheckpointMode.UNALIGNED
                && !plan.requiredCapabilities().contains(
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "job.plan.capabilities",
                    "unaligned checkpoint capability is required");
        }
        if (checkpointConfig.mode() == StreamCheckpointMode.UNALIGNED
                && resourceProfile.checkpointBytes() <= 0L) {
            throw StreamContractSupport.failure(
                    "INVALID_JOB_RESOURCE_CEILING",
                    "job.resources.checkpointBytes",
                    "unaligned checkpoint requires positive channel-state storage");
        }
        if (plan.timeMode() == PortableStreamPlan.TimeMode.EVENT_TIME
                && !plan.requiredCapabilities().contains(
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY)
                && !plan.requiredCapabilities().contains(
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_SOURCE_TIME_POLICY",
                    "job.plan.capabilities",
                    "product event-time jobs require explicit progress authority");
        }
        for (PortableStreamNode node : plan.nodes()) {
            StreamNodeExecutionDescriptor execution =
                    node.executionDescriptor();
            if (resourceProfile.checkpointBytes() > 0L
                    && execution != null
                    && PortableStreamPlan.requiresCheckpointPayload(
                    node.opcode())
                    && execution.maximumCheckpointBytes() <= 0L) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "job.plan.nodes.execution.maximumCheckpointBytes",
                        "checkpoint participant requires a positive node-local payload lease");
            }
            if (execution != null
                    && execution.maximumCheckpointBytes()
                    > resourceProfile.checkpointBytes()) {
                throw StreamContractSupport.failure(
                        "INVALID_JOB_RESOURCE_CEILING",
                        "job.resources.checkpointBytes",
                        "job checkpoint ceiling must cover every node payload lease");
            }
            if (node.opcode() == StreamOperatorCode.EVENT_TIME_WINDOW
                    && !plan.requiredCapabilities().contains(
                    PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY)) {
                throw StreamContractSupport.failure(
                        "MISSING_WINDOW_FIELD_BINDING",
                        "job.plan.capabilities",
                        "product windows require explicit stable field bindings");
            }
        }
        this.plan = plan;
        this.checkpointConfig = checkpointConfig;
        this.restartPolicy = restartPolicy;
        this.resourceProfile = resourceProfile;
    }

    public PortableStreamPlan plan() { return plan; }
    public StreamCheckpointConfig checkpointConfig() { return checkpointConfig; }
    public StreamRestartPolicy restartPolicy() { return restartPolicy; }
    public StreamResourceProfile resourceProfile() { return resourceProfile; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof PortableStreamJob)) return false;
        PortableStreamJob that = (PortableStreamJob) other;
        return plan.equals(that.plan)
                && checkpointConfig.equals(that.checkpointConfig)
                && restartPolicy.equals(that.restartPolicy)
                && resourceProfile.equals(that.resourceProfile);
    }

    @Override public int hashCode() {
        return Objects.hash(plan, checkpointConfig, restartPolicy, resourceProfile);
    }
}
