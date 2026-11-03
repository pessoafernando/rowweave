package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Local FORWARD router from one producer lane to parallel task mailboxes. */
final class StreamParallelForwardOutput implements StreamChannelOutput {
    private final List<StreamChannelOutput> partitions;
    private final long[] sequences;
    private int nextRecordTarget;

    StreamParallelForwardOutput(
            List<? extends StreamChannelOutput> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "forward route requires at least one partition");
        }
        List<StreamChannelOutput> copied = new ArrayList<>(
                partitions.size());
        for (StreamChannelOutput partition : partitions) {
            if (partition == null) {
                throw new IllegalArgumentException(
                        "forward route contains a null partition");
            }
            copied.add(partition);
        }
        this.partitions = Collections.unmodifiableList(copied);
        this.sequences = new long[copied.size()];
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return partitions.get(0).channelIdentity();
    }

    @Override public synchronized void send(StreamEnvelope envelope) {
        if (envelope == null || !channelIdentity().equals(
                envelope.channelIdentity())) {
            throw new IllegalArgumentException(
                    "forward envelope belongs to another attempt");
        }
        if (envelope.kind() == StreamEnvelope.Kind.RECORD) {
            int target = nextRecordTarget;
            nextRecordTarget = (nextRecordTarget + 1) % partitions.size();
            send(target, envelope);
            return;
        }
        for (int target = 0; target < partitions.size(); target++) {
            send(target, envelope);
        }
    }

    private void send(int target, StreamEnvelope envelope) {
        long sequence = sequences[target] = Math.addExact(
                sequences[target], 1L);
        StreamChannelOutput partition = partitions.get(target);
        partition.send(StreamParallelKeyedOutput.rebind(
                envelope, partition.channelIdentity(), sequence));
    }

    @Override public void close() {
        for (StreamChannelOutput partition : partitions) partition.close();
    }

    @Override public void cancel() {
        for (StreamChannelOutput partition : partitions) partition.cancel();
    }

    @Override public void fail(Throwable cause) {
        for (StreamChannelOutput partition : partitions) {
            partition.fail(cause);
        }
    }

    @Override public StreamChannelMetrics metrics() {
        return StreamChannelMetrics.aggregateOutputs(partitions);
    }
}
