package io.rowweave.deployment;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamDynamicSourceProvider;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Signed static Stream connector index; only Worker role may initialize code. */
public final class StreamConnectorProviderRegistry {
    public static final String SOURCE_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.stream.spi.StreamSourceProvider";
    public static final String SINK_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.stream.spi.StreamSinkProvider";
    public static final String TRANSACTIONAL_SINK_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.stream.spi.StreamTransactionalSinkProvider";
    private static final String CONTRACT_PREFIX =
            "META-INF/rowweave/stream-connector/contracts/";
    private static final int MAX_SERVICE_BYTES = 32 * 1024;
    private static final int MAX_PROVIDERS_PER_IMAGE = 64;
    private static final int MAX_PROVIDERS = 256;
    private static final Pattern BINARY_CLASS = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    public enum Role { STATIC_ONLY, WORKER }

    private final VerifiedDeploymentSet deployments;
    private final Role role;
    private final Map<Key, Candidate> candidates;

    private StreamConnectorProviderRegistry(
            VerifiedDeploymentSet deployments, Role role,
            Map<Key, Candidate> candidates) {
        this.deployments = deployments;
        this.role = role;
        this.candidates = Collections.unmodifiableMap(candidates);
    }

    public static StreamConnectorProviderRegistry scan(
            VerifiedDeploymentSet deployments, Role role) {
        if (deployments == null || role == null) {
            throw new IllegalArgumentException("deployment set and role are required");
        }
        deployments.requireAdmissionOpen();
        Map<Key, Candidate> candidates = new LinkedHashMap<>();
        for (VerifiedDeploymentImage image : deployments.imagesInCanonicalOrder()) {
            Set<String> classes = new HashSet<>();
            Set<String> resources = new HashSet<>();
            int count = 0;
            count += discover(image, SOURCE_SERVICE_RESOURCE,
                    ProviderType.SOURCE, classes, resources, candidates);
            count += discover(image, SINK_SERVICE_RESOURCE,
                    ProviderType.SINK, classes, resources, candidates);
            count += discover(image, TRANSACTIONAL_SINK_SERVICE_RESOURCE,
                    ProviderType.TRANSACTIONAL_SINK, classes, resources,
                    candidates);
            if (count > MAX_PROVIDERS_PER_IMAGE) {
                throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
            }
        }
        return new StreamConnectorProviderRegistry(deployments, role, candidates);
    }

    public StreamConnectorProviderContract requireContract(
            StreamConnectorProviderContract.Kind kind, String providerKey) {
        deployments.requireAdmissionOpen();
        Candidate candidate = candidates.get(new Key(kind, providerKey));
        if (candidate == null) throw failure("STREAM_CONNECTOR_UNKNOWN");
        return candidate.contract;
    }

    /** Canonical signed metadata; no provider class is loaded. */
    public List<StreamConnectorProviderContract> contracts() {
        deployments.requireAdmissionOpen();
        List<StreamConnectorProviderContract> result =
                new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates.values()) {
            result.add(candidate.contract);
        }
        return Collections.unmodifiableList(result);
    }

    public String deploymentSetId() {
        deployments.requireAdmissionOpen();
        return deployments.deploymentSetId();
    }

    public void requireWorkerRole() {
        if (role != Role.WORKER) {
            throw new IllegalArgumentException(
                    "signed Stream Worker requires a Worker registry");
        }
        deployments.requireAdmissionOpen();
    }

    /** Metadata-only whole-graph admission; never initializes a provider class. */
    public void verifyArtifact(PlanArtifact artifact) {
        deployments.requireAdmissionOpen();
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        if (!artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)
                || !deployments.deploymentSetId().equals(
                PlanArtifactDeploymentBinding.deploymentSetId(artifact))) {
            throw failure("STREAM_CONNECTOR_DEPLOYMENT_MISMATCH");
        }
        for (PortableStreamNode node : PortableStreamJobArtifact
                .decodeValidated(artifact).plan().nodes()) {
            if (node.opcode() == StreamOperatorCode.SOURCE) {
                verifySource(node);
            } else if (node.opcode() == StreamOperatorCode.SINK) {
                verifySink(node);
            }
        }
    }

    private void verifySource(PortableStreamNode node) {
        StreamSourceBinding binding = (StreamSourceBinding) node.binding();
        Candidate candidate = candidate(
                StreamConnectorProviderContract.Kind.SOURCE,
                binding.providerKey());
        StreamConnectorProviderContract contract = candidate.contract;
        requireDigest(contract, binding.providerContractDigest());
        if (binding.configurationFrame().length
                > contract.maximumConfigurationBytes()) {
            throw failure("STREAM_CONNECTOR_CONFIGURATION_LIMIT");
        }
        requireConfiguration(binding.configurationFrame());
        if (binding.replayGuarantee()
                != StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED
                || (node.scalarParameters().containsKey("source.termination")
                && !contract.boundedEndOfInput())
                || (binding.dynamicDiscovery() && !contract.dynamicDiscovery())
                || (node.scalarParameters().containsKey("source.mode")
                && !contract.residentCdc())
                || ("STREAM_CDC_ENUMERATOR_V2".equals(
                node.scalarParameters().get("source.enumeratorStateFormat"))
                && !contract.externalCheckpointAcknowledgement())) {
            throw failure("STREAM_CONNECTOR_CAPABILITY_MISMATCH");
        }
    }

    private void verifySink(PortableStreamNode node) {
        StreamSinkBinding binding = (StreamSinkBinding) node.binding();
        Candidate candidate = candidate(
                StreamConnectorProviderContract.Kind.SINK,
                binding.providerKey());
        StreamConnectorProviderContract contract = candidate.contract;
        requireDigest(contract, binding.providerContractDigest());
        if (binding.configurationFrame().length
                > contract.maximumConfigurationBytes()) {
            throw failure("STREAM_CONNECTOR_CONFIGURATION_LIMIT");
        }
        requireConfiguration(binding.configurationFrame());
        boolean transactional = binding.visibilityProfile()
                == StreamSinkBinding.VisibilityProfile
                .CHECKPOINT_TRANSACTIONAL;
        if (transactional != (contract.sinkVisibility()
                == StreamConnectorProviderContract.SinkVisibility
                .CHECKPOINT_TRANSACTIONAL)) {
            throw failure("STREAM_CONNECTOR_CAPABILITY_MISMATCH");
        }
    }

    private static void requireDigest(
            StreamConnectorProviderContract contract, String actual) {
        if (actual == null || !MessageDigest.isEqual(
                StreamConnectorProviderContractCodec.digestHex(contract)
                        .getBytes(StandardCharsets.US_ASCII),
                actual.getBytes(StandardCharsets.US_ASCII))) {
            throw failure("STREAM_CONNECTOR_CONTRACT_DRIFT");
        }
    }

    private static void requireConfiguration(byte[] frame) {
        try {
            StreamConnectorConfiguration.decode(frame);
        } catch (IllegalArgumentException invalid) {
            throw failure("STREAM_CONNECTOR_CONFIGURATION_INVALID", invalid);
        }
    }

    public StreamSourceProvider loadSource(
            String providerKey, ProviderActionSupervisor supervisor,
            long deadlineNanos) {
        return (StreamSourceProvider) load(
                StreamConnectorProviderContract.Kind.SOURCE,
                providerKey, supervisor, deadlineNanos);
    }

    public StreamSinkProvider loadSink(
            String providerKey, ProviderActionSupervisor supervisor,
            long deadlineNanos) {
        Candidate candidate = candidate(
                StreamConnectorProviderContract.Kind.SINK, providerKey);
        if (candidate.providerType != ProviderType.SINK) {
            throw failure("STREAM_CONNECTOR_KIND_MISMATCH");
        }
        return (StreamSinkProvider) load(candidate, supervisor, deadlineNanos);
    }

    public StreamTransactionalSinkProvider loadTransactionalSink(
            String providerKey, ProviderActionSupervisor supervisor,
            long deadlineNanos) {
        Candidate candidate = candidate(
                StreamConnectorProviderContract.Kind.SINK, providerKey);
        if (candidate.providerType != ProviderType.TRANSACTIONAL_SINK) {
            throw failure("STREAM_CONNECTOR_KIND_MISMATCH");
        }
        return (StreamTransactionalSinkProvider) load(
                candidate, supervisor, deadlineNanos);
    }

    private Object load(StreamConnectorProviderContract.Kind kind,
            String providerKey, ProviderActionSupervisor supervisor,
            long deadlineNanos) {
        return load(candidate(kind, providerKey), supervisor, deadlineNanos);
    }

    private Candidate candidate(StreamConnectorProviderContract.Kind kind,
            String providerKey) {
        deployments.requireAdmissionOpen();
        Candidate result = candidates.get(new Key(kind, providerKey));
        if (result == null) throw failure("STREAM_CONNECTOR_UNKNOWN");
        return result;
    }

    private Object load(Candidate candidate,
            ProviderActionSupervisor supervisor, long deadlineNanos) {
        if (role != Role.WORKER) {
            throw new IllegalStateException("STREAM_CONNECTOR_RUNTIME_REQUIRED");
        }
        if (supervisor == null) {
            throw new IllegalArgumentException("provider supervisor is required");
        }
        return supervisor.call("stream-connector-load", deadlineNanos,
                () -> candidate.load(deployments));
    }

    private static int discover(VerifiedDeploymentImage image,
            String serviceResource, ProviderType providerType,
            Set<String> classes, Set<String> resources,
            Map<Key, Candidate> candidates) {
        int size = image.entryExpandedSize(serviceResource);
        if (size < 0) return 0;
        if (size == 0 || size > MAX_SERVICE_BYTES) {
            throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
        }
        List<ServiceEntry> entries = parseService(read(image, serviceResource,
                MAX_SERVICE_BYTES, "STREAM_CONNECTOR_SERVICE_INVALID"));
        for (ServiceEntry entry : entries) {
            if (!classes.add(entry.providerClassName)
                    || !resources.add(entry.contractResource)
                    || !image.hasClass(entry.providerClassName)) {
                throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
            }
            byte[] bytes = read(image, entry.contractResource,
                    StreamConnectorProviderContractCodec.MAX_FRAME_BYTES,
                    "STREAM_CONNECTOR_CONTRACT_INVALID");
            final StreamConnectorProviderContract contract;
            try {
                contract = StreamConnectorProviderContractCodec.decode(bytes);
            } catch (RuntimeException invalid) {
                throw failure("STREAM_CONNECTOR_CONTRACT_INVALID", invalid);
            }
            if (!contract.deploymentId().equals(
                    image.manifest().deploymentId())
                    || !StreamConnectorConfiguration.CODEC_ID.equals(
                    contract.configurationCodecId())
                    || contract.kind() != providerType.kind
                    || (providerType == ProviderType.TRANSACTIONAL_SINK
                    && contract.sinkVisibility() !=
                    StreamConnectorProviderContract.SinkVisibility
                            .CHECKPOINT_TRANSACTIONAL)
                    || (providerType == ProviderType.SINK
                    && contract.sinkVisibility() !=
                    StreamConnectorProviderContract.SinkVisibility
                            .EAGER_AT_LEAST_ONCE)) {
                throw failure("STREAM_CONNECTOR_CONTRACT_INVALID");
            }
            if (candidates.size() >= MAX_PROVIDERS
                    || candidates.put(new Key(contract.kind(),
                    contract.providerKey()), new Candidate(
                    image.manifest().deploymentId(),
                    entry.providerClassName, providerType, contract)) != null) {
                throw failure("STREAM_CONNECTOR_DUPLICATE");
            }
        }
        return entries.size();
    }

    private static byte[] read(VerifiedDeploymentImage image,
            String resource, int maximum, String code) {
        int size = image.entryExpandedSize(resource);
        if (size < 1 || size > maximum) throw failure(code);
        try (VerifiedDeploymentImage.BorrowedEntry entry =
                     image.borrowResource(resource)) {
            if (entry == null || entry.bytes().length != size) throw failure(code);
            return entry.bytes().clone();
        }
    }

    private static List<ServiceEntry> parseService(byte[] bytes) {
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw failure("STREAM_CONNECTOR_SERVICE_INVALID", malformed);
        }
        if (text.isEmpty() || text.charAt(text.length() - 1) != '\n'
                || text.indexOf('\r') >= 0 || text.indexOf('\0') >= 0) {
            throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
        }
        String[] lines = text.substring(0, text.length() - 1)
                .split("\\n", -1);
        if (lines.length < 1 || lines.length > MAX_PROVIDERS_PER_IMAGE) {
            throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
        }
        List<ServiceEntry> entries = new ArrayList<>(lines.length);
        for (String line : lines) {
            int equals = line.indexOf('=');
            if (equals <= 0 || equals != line.lastIndexOf('=')
                    || equals == line.length() - 1) {
                throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
            }
            String className = line.substring(0, equals);
            String resource = line.substring(equals + 1);
            if (className.length() > 256
                    || !BINARY_CLASS.matcher(className).matches()
                    || resource.length() > 256
                    || !resource.startsWith(CONTRACT_PREFIX)
                    || resource.contains("//") || resource.contains("/../")
                    || resource.contains("/./") || resource.indexOf('\\') >= 0
                    || resource.endsWith("/..") || resource.endsWith("/.")) {
                throw failure("STREAM_CONNECTOR_SERVICE_INVALID");
            }
            entries.add(new ServiceEntry(className, resource));
        }
        return entries;
    }

    private static DeploymentValidationException failure(String code) {
        return new DeploymentValidationException(
                code, "Stream connector deployment admission failed");
    }

    private static DeploymentValidationException failure(
            String code, Throwable cause) {
        return new DeploymentValidationException(
                code, "Stream connector deployment admission failed", cause);
    }

    private enum ProviderType {
        SOURCE(StreamConnectorProviderContract.Kind.SOURCE,
                StreamSourceProvider.class),
        SINK(StreamConnectorProviderContract.Kind.SINK,
                StreamSinkProvider.class),
        TRANSACTIONAL_SINK(StreamConnectorProviderContract.Kind.SINK,
                StreamTransactionalSinkProvider.class);

        private final StreamConnectorProviderContract.Kind kind;
        private final Class<?> providerInterface;

        ProviderType(StreamConnectorProviderContract.Kind kind,
                Class<?> providerInterface) {
            this.kind = kind;
            this.providerInterface = providerInterface;
        }
    }

    private static final class Key {
        private final StreamConnectorProviderContract.Kind kind;
        private final String providerKey;

        private Key(StreamConnectorProviderContract.Kind kind,
                String providerKey) {
            this.kind = kind;
            this.providerKey = providerKey;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Key)) return false;
            Key that = (Key) other;
            return kind == that.kind && providerKey != null
                    && providerKey.equals(that.providerKey);
        }

        @Override public int hashCode() {
            return 31 * (kind == null ? 0 : kind.hashCode())
                    + (providerKey == null ? 0 : providerKey.hashCode());
        }
    }

    private static final class ServiceEntry {
        private final String providerClassName;
        private final String contractResource;

        private ServiceEntry(String providerClassName,
                String contractResource) {
            this.providerClassName = providerClassName;
            this.contractResource = contractResource;
        }
    }

    private static final class Candidate {
        private final String deploymentId;
        private final String providerClassName;
        private final ProviderType providerType;
        private final StreamConnectorProviderContract contract;
        private volatile Object loaded;

        private Candidate(String deploymentId, String providerClassName,
                ProviderType providerType,
                StreamConnectorProviderContract contract) {
            this.deploymentId = deploymentId;
            this.providerClassName = providerClassName;
            this.providerType = providerType;
            this.contract = contract;
        }

        private Object load(VerifiedDeploymentSet deployments) {
            Object result = loaded;
            if (result != null) return result;
            synchronized (this) {
                result = loaded;
                if (result != null) return result;
                try {
                    DeploymentClassLoader loader =
                            deployments.classLoader(deploymentId);
                    Class<?> type = Class.forName(providerClassName,
                            false, loader);
                    if (type.getClassLoader() != loader
                            || !deployments.trustsClass(type)
                            || !providerType.providerInterface
                            .isAssignableFrom(type)
                            || !Modifier.isPublic(type.getModifiers())) {
                        throw failure("STREAM_CONNECTOR_NOT_ADMITTED");
                    }
                    Constructor<?> constructor = type.getConstructor();
                    if (!Modifier.isPublic(constructor.getModifiers())) {
                        throw failure("STREAM_CONNECTOR_NOT_ADMITTED");
                    }
                    result = constructor.newInstance();
                    String key = providerType == ProviderType.SOURCE
                            ? ((StreamSourceProvider) result).providerKey()
                            : providerType == ProviderType.SINK
                            ? ((StreamSinkProvider) result).providerKey()
                            : ((StreamTransactionalSinkProvider) result).providerKey();
                    String digest = providerType == ProviderType.SOURCE
                            ? ((StreamSourceProvider) result).contractDigest()
                            : providerType == ProviderType.SINK
                            ? ((StreamSinkProvider) result).contractDigest()
                            : ((StreamTransactionalSinkProvider) result).contractDigest();
                    if (!contract.providerKey().equals(key)
                            || !MessageDigest.isEqual(
                            StreamConnectorProviderContractCodec.digestHex(contract)
                                    .getBytes(StandardCharsets.US_ASCII),
                            digest == null ? new byte[0]
                                    : digest.getBytes(StandardCharsets.US_ASCII))
                            || (providerType == ProviderType.SOURCE
                            && ((contract.dynamicDiscovery()
                            && !(result instanceof StreamDynamicSourceProvider))
                            || (contract.residentCdc()
                            && !(result instanceof StreamCdcSourceProvider))))) {
                        throw failure("STREAM_CONNECTOR_CONTRACT_DRIFT");
                    }
                    loaded = result;
                    return result;
                } catch (DeploymentValidationException invalid) {
                    throw invalid;
                } catch (ReflectiveOperationException | LinkageError
                         | RuntimeException invalid) {
                    throw failure("STREAM_CONNECTOR_NOT_ADMITTED", invalid);
                }
            }
        }
    }
}
