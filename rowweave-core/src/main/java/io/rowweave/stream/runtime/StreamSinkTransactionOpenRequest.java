package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamSinkBinding;

/** Immutable fenced open request for one physical transactional Sink subtask. */
public final class StreamSinkTransactionOpenRequest {
    private final String operatorUid;
    private final int subtaskIndex;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final StreamSinkBinding binding;

    StreamSinkTransactionOpenRequest(
            String operatorUid, int subtaskIndex,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, StreamSinkBinding binding) {
        this.operatorUid = operatorUid;
        this.subtaskIndex = subtaskIndex;
        this.runId = runId;
        this.attemptId = attemptId;
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.binding = binding;
    }

    public String operatorUid() { return operatorUid; }
    public int subtaskIndex() { return subtaskIndex; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public StreamSinkBinding binding() { return binding; }
}
