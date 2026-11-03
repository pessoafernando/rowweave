package io.rowweave.cluster.rpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/** Computes the only capability set that either peer may use on a v1 connection. */
public final class ProtocolNegotiator {
    private ProtocolNegotiator() { }

    public static NegotiatedProtocol negotiate(
            ProtocolHelloV1 local, ProtocolHelloV1 remote) {
        if (local == null || remote == null) reject("HELLO_REQUIRED", "both hellos are required");
        if (!local.manifestDigest().equals(remote.manifestDigest())) {
            reject("MANIFEST_DIGEST_MISMATCH", "release manifest digests differ");
        }
        if (local.protocolEpoch() != remote.protocolEpoch()) {
            reject("PROTOCOL_EPOCH_MISMATCH", "protocol epochs differ");
        }
        ProtocolRevisionRange readers = local.readerRevisions().intersect(
                remote.readerRevisions());
        ProtocolRevisionRange writers = local.writerRevisions().intersect(
                remote.writerRevisions());
        if (readers == null) reject("READER_REVISION_MISMATCH", "reader revisions do not overlap");
        if (writers == null) reject("WRITER_REVISION_MISMATCH", "writer revisions do not overlap");

        List<Integer> messages = requiredIntersection(
                local.messageIds(), remote.messageIds(), "MESSAGE_INTERSECTION_EMPTY");
        List<Integer> artifacts = requiredIntersection(
                local.artifactFormats(), remote.artifactFormats(), "ARTIFACT_INTERSECTION_EMPTY");
        List<Integer> stages = requiredIntersection(local.stageCommandVersions(),
                remote.stageCommandVersions(), "STAGE_COMMAND_INTERSECTION_EMPTY");
        List<Integer> data = requiredIntersection(local.dataProtocolVersions(),
                remote.dataProtocolVersions(), "DATA_PROTOCOL_INTERSECTION_EMPTY");
        List<Integer> deployments = requiredIntersection(local.deploymentAbis(),
                remote.deploymentAbis(), "DEPLOYMENT_ABI_INTERSECTION_EMPTY");
        List<String> capabilities = requiredIntersection(local.runtimeCapabilities(),
                remote.runtimeCapabilities(), "RUNTIME_CAPABILITY_INTERSECTION_EMPTY");

        TreeSet<Integer> required = new TreeSet<>(local.requiredExtensionIds());
        required.addAll(remote.requiredExtensionIds());
        for (Integer id : required) {
            if (!local.supportsExtension(id) || !remote.supportsExtension(id)) {
                reject("REQUIRED_EXTENSION_UNAVAILABLE",
                        "required protocol extension is unsupported by one peer");
            }
        }
        List<Integer> optionals = intersection(
                local.extensionValues().keySet(), remote.extensionValues().keySet());
        return new NegotiatedProtocol(local.protocolEpoch(), readers, writers,
                messages, artifacts, stages, data, deployments, capabilities,
                optionals, Math.min(local.maximumFrameBytes(), remote.maximumFrameBytes()),
                Math.min(local.maximumResidentBytes(), remote.maximumResidentBytes()));
    }

    private static <T extends Comparable<? super T>> List<T> requiredIntersection(
            Collection<T> left, Collection<T> right, String code) {
        List<T> result = intersection(left, right);
        if (result.isEmpty()) reject(code, "required protocol capability intersection is empty");
        return result;
    }

    private static <T extends Comparable<? super T>> List<T> intersection(
            Collection<T> left, Collection<T> right) {
        TreeSet<T> result = new TreeSet<>(left);
        result.retainAll(right);
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    private static void reject(String code, String message) {
        throw new ProtocolNegotiationException(code, message);
    }
}
