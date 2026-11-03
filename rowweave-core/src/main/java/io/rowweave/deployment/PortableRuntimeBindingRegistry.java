package io.rowweave.deployment;

import io.rowweave.BatchSink;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchExecutableSource;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.Source;
import io.rowweave.SourceExecutor;
import io.rowweave.SourceExecutorFactory;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.PortableBatchSinkProvider;
import io.rowweave.portable.PortableBatchSourceProvider;
import io.rowweave.portable.PortableBindingVerifier;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableConnectorProviderContractCodec;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanException;
import io.rowweave.portable.PortableRuntimeResolver;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Complete static-verifier/Worker-resolver pair for portable runtime bindings. */
public final class PortableRuntimeBindingRegistry
        implements PortableBindingVerifier, PortableRuntimeResolver {
    public static final String SOURCE_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.portable.PortableBatchSourceProvider";
    public static final String SINK_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.portable.PortableBatchSinkProvider";
    public static final int MAX_SERVICE_BYTES = 32 * 1024;
    public static final int MAX_CANDIDATES = 256;
    public static final long MAX_ACTION_TIMEOUT_MILLIS = 60_000L;

    public enum Role { STATIC_ONLY, WORKER }

    private static final Pattern CLASS_NAME = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private final VerifiedDeploymentSet deployments;
    private final Role role;
    private final ProviderActionSupervisor supervisor;
    private final long actionTimeoutMillis;
    private final Map<String, Candidate> sources;
    private final Map<String, Candidate> sinks;
    private final PortableFunctionProviderRegistry functions;

    private PortableRuntimeBindingRegistry(
            VerifiedDeploymentSet deployments, Role role,
            ProviderActionSupervisor supervisor, long actionTimeoutMillis,
            Map<String, Candidate> sources, Map<String, Candidate> sinks,
            PortableFunctionProviderRegistry functions) {
        this.deployments = deployments;
        this.role = role;
        this.supervisor = supervisor;
        this.actionTimeoutMillis = actionTimeoutMillis;
        this.sources = Collections.unmodifiableMap(sources);
        this.sinks = Collections.unmodifiableMap(sinks);
        this.functions = functions;
    }

    public static PortableRuntimeBindingRegistry scan(
            VerifiedDeploymentSet deployments, Role role,
            ProviderActionSupervisor supervisor, long actionTimeoutMillis) {
        if (deployments == null || role == null) {
            throw new IllegalArgumentException(
                    "deployments and portable registry role are required");
        }
        if (role == Role.WORKER
                && (supervisor == null || actionTimeoutMillis <= 0
                || actionTimeoutMillis > MAX_ACTION_TIMEOUT_MILLIS)) {
            throw new IllegalArgumentException(
                    "Worker portable registry requires bounded supervision");
        }
        if (role == Role.STATIC_ONLY
                && (supervisor != null || actionTimeoutMillis != 0L)) {
            throw new IllegalArgumentException(
                    "static portable registry cannot own runtime supervision");
        }
        Map<String, Candidate> sources = new HashMap<>();
        Map<String, Candidate> sinks = new HashMap<>();
        for (VerifiedDeploymentImage image
                : deployments.imagesInCanonicalOrder()) {
            discover(image, SOURCE_SERVICE_RESOURCE,
                    PortableOperatorCode.BindingKind.SOURCE, sources);
            discover(image, SINK_SERVICE_RESOURCE,
                    PortableOperatorCode.BindingKind.SINK, sinks);
            if (sources.size() + sinks.size() > MAX_CANDIDATES) {
                fail("CONNECTOR_PROVIDER_LIMIT_EXCEEDED");
            }
        }
        PortableFunctionProviderRegistry functions =
                PortableFunctionProviderRegistry.scan(
                        deployments, role == Role.WORKER
                                ? PortableFunctionProviderRegistry.Role.WORKER
                                : PortableFunctionProviderRegistry.Role.STATIC_ONLY);
        return new PortableRuntimeBindingRegistry(
                deployments, role, supervisor, actionTimeoutMillis,
                sources, sinks, functions);
    }

    @Override
    public String deploymentSetId() {
        return deployments.deploymentSetId();
    }

    @Override
    public void verifySource(PortableSourceBinding binding) {
        deployments.requireAdmissionOpen();
        if (binding == null) fail("SOURCE_BINDING_INVALID");
        Candidate candidate = require(sources, binding.providerKey());
        requireDigest(candidate, binding.providerContractDigest());
        PortableConnectorProviderContract contract = candidate.contract;
        byte[] configuration = binding.configurationFrame();
        List<byte[]> splits = binding.splitFrames();
        if (configuration.length > contract.maximumConfigurationBytes()
                || splits.size() > contract.maximumInstances()) {
            fail("SOURCE_BINDING_LIMIT_EXCEEDED");
        }
        for (byte[] split : splits) {
            if (split.length > contract.maximumPayloadBytes()) {
                fail("SOURCE_BINDING_LIMIT_EXCEEDED");
            }
            decodeSourceDescriptor(split);
        }
        requireCodec(contract);
    }

    @Override
    public void verifySink(PortableSinkBinding binding) {
        deployments.requireAdmissionOpen();
        if (binding == null) fail("SINK_BINDING_INVALID");
        Candidate candidate = require(sinks, binding.providerKey());
        requireDigest(candidate, binding.providerContractDigest());
        PortableConnectorProviderContract contract = candidate.contract;
        if (binding.configurationFrame().length
                > contract.maximumConfigurationBytes()
                || binding.descriptorFrame().length
                > contract.maximumPayloadBytes()) {
            fail("SINK_BINDING_LIMIT_EXCEEDED");
        }
        decodeSinkDescriptor(binding.descriptorFrame());
        requireCodec(contract);
    }

    @Override
    public void verifyFunction(FunctionDeploymentReference reference) {
        functions.verify(reference);
    }

    @Override
    public List<Source> resolveSource(PortableSourceBinding binding) {
        requireWorker();
        verifySource(binding);
        Candidate candidate = require(sources, binding.providerKey());
        long deadline = deadline();
        byte[] configuration = binding.configurationFrame();
        List<byte[]> splits = binding.splitFrames();
        return supervisor.call("portable-source-resolve", deadline, () -> {
            PortableBatchSourceProvider provider =
                    candidate.loadSource(deployments);
            requireRuntimeContract(candidate, provider.contract());
            List<Source> inputs = new ArrayList<>(splits.size());
            for (byte[] split : splits) {
                BatchSourceSplitDescriptor expected =
                        decodeSourceDescriptor(split);
                Source input = provider.create(
                        configuration.clone(), split.clone());
                if (!(input instanceof BatchSource)) {
                    fail("SOURCE_RUNTIME_MISMATCH");
                }
                DataSchema dataSchema = input.getSchema();
                BatchSourceSplitDescriptor descriptor =
                        ((BatchSource) input).batchSourceSplit();
                if (dataSchema == null || !expected.equals(descriptor)) {
                    fail("SOURCE_RUNTIME_MISMATCH");
                }
                SourceExecutor<?> executor = SourceExecutorFactory.create(input);
                inputs.add(new ResolvedSourceInput(
                        input, dataSchema, descriptor, executor));
            }
            return Collections.unmodifiableList(inputs);
        });
    }

    @Override
    public Sink resolveSink(
            PortableSinkBinding binding, DataSchema inputSchema) {
        requireWorker();
        verifySink(binding);
        if (inputSchema == null) fail("SINK_RUNTIME_MISMATCH");
        Candidate candidate = require(sinks, binding.providerKey());
        long deadline = deadline();
        BatchSinkDescriptor expected = decodeSinkDescriptor(
                binding.descriptorFrame());
        return supervisor.call("portable-sink-resolve", deadline, () -> {
            PortableBatchSinkProvider provider =
                    candidate.loadSink(deployments);
            requireRuntimeContract(candidate, provider.contract());
            Sink output = provider.create(
                    binding.descriptorFrame(),
                    binding.configurationFrame(), inputSchema);
            if (!(output instanceof BatchSink)) {
                fail("SINK_RUNTIME_MISMATCH");
            }
            DataSchema dataSchema = output.getSchema();
            BatchSinkDescriptor descriptor =
                    ((BatchSink) output).batchSinkDescriptor();
            if (dataSchema == null || !expected.equals(descriptor)) {
                fail("SINK_RUNTIME_MISMATCH");
            }
            return new ResolvedSinkOutput(
                    output, dataSchema, descriptor,
                    output.isWorkerWritable(),
                    output.exclusiveDestinationId(),
                    output.supportsCoordinatedCommit());
        });
    }

    @Override
    public Serializable resolveFunction(
            FunctionDeploymentReference reference) {
        requireWorker();
        return functions.resolve(reference, supervisor, deadline());
    }

    private void requireWorker() {
        if (role != Role.WORKER) {
            throw new IllegalStateException("PORTABLE_RUNTIME_REQUIRED");
        }
    }

    private long deadline() {
        return System.nanoTime() + actionTimeoutMillis * 1_000_000L;
    }

    private static Candidate require(
            Map<String, Candidate> candidates, String providerKey) {
        Candidate candidate = candidates.get(providerKey);
        if (candidate == null) fail("CONNECTOR_PROVIDER_UNKNOWN");
        return candidate;
    }

    private static void requireDigest(
            Candidate candidate, String expected) {
        if (!MessageDigest.isEqual(
                candidate.contractDigest.getBytes(StandardCharsets.US_ASCII),
                expected.getBytes(StandardCharsets.US_ASCII))) {
            fail("CONNECTOR_CONTRACT_MISMATCH");
        }
    }

    private static void requireCodec(
            PortableConnectorProviderContract contract) {
        if (!PortableFunctionProviderRegistry.OPAQUE_CONFIGURATION_CODEC
                .equals(contract.configurationCodecId())) {
            fail("CONNECTOR_CONFIGURATION_CODEC_UNSUPPORTED");
        }
    }

    private static BatchSourceSplitDescriptor decodeSourceDescriptor(
            byte[] frame) {
        try {
            return BatchSourceSplitDescriptorCodec.decode(frame);
        } catch (RuntimeException malformed) {
            throw new PortablePlanException(
                    "SOURCE_DESCRIPTOR_INVALID", "connector.sourceDescriptor",
                    "portable source descriptor is invalid", malformed);
        }
    }

    private static BatchSinkDescriptor decodeSinkDescriptor(byte[] frame) {
        try {
            return BatchSinkDescriptorCodec.decode(frame);
        } catch (RuntimeException malformed) {
            throw new PortablePlanException(
                    "SINK_DESCRIPTOR_INVALID", "connector.sinkDescriptor",
                    "portable sink descriptor is invalid", malformed);
        }
    }

    private static void requireRuntimeContract(
            Candidate candidate,
            PortableConnectorProviderContract runtimeContract) {
        byte[] encoded = PortableConnectorProviderContractCodec.encode(
                runtimeContract);
        if (!MessageDigest.isEqual(candidate.contractBytes, encoded)) {
            fail("CONNECTOR_CONTRACT_DRIFT");
        }
    }

    private static void discover(
            VerifiedDeploymentImage image, String serviceResource,
            PortableOperatorCode.BindingKind expectedKind,
            Map<String, Candidate> candidates) {
        int size = image.entryExpandedSize(serviceResource);
        if (size < 0) return;
        if (size == 0 || size > MAX_SERVICE_BYTES) {
            fail("CONNECTOR_SERVICE_INVALID");
        }
        for (ServiceEntry entry : parseService(
                read(image, serviceResource, MAX_SERVICE_BYTES))) {
            if (!image.hasClass(entry.providerClassName)) {
                fail("CONNECTOR_SERVICE_INVALID");
            }
            byte[] contractBytes = read(image, entry.contractResource,
                    PortableConnectorProviderContractCodec.MAX_FRAME_BYTES);
            PortableConnectorProviderContract contract;
            try {
                contract = PortableConnectorProviderContractCodec.decode(
                        contractBytes);
            } catch (RuntimeException invalid) {
                throw new PortablePlanException(
                        "CONNECTOR_CONTRACT_INVALID",
                        "connector.contract",
                        "signed connector contract is invalid", invalid);
            }
            if (contract.bindingKind() != expectedKind
                    || !MessageDigest.isEqual(contractBytes,
                    PortableConnectorProviderContractCodec.encode(contract))) {
                fail("CONNECTOR_CONTRACT_INVALID");
            }
            Candidate candidate = new Candidate(image,
                    image.manifest().deploymentId(), entry.providerClassName,
                    contractBytes, contract);
            if (candidates.put(contract.providerKey(), candidate) != null) {
                fail("CONNECTOR_PROVIDER_DUPLICATE");
            }
        }
    }

    private static byte[] read(
            VerifiedDeploymentImage image, String resource, int maximum) {
        int size = image.entryExpandedSize(resource);
        if (size <= 0 || size > maximum) fail("CONNECTOR_RESOURCE_INVALID");
        try (VerifiedDeploymentImage.BorrowedEntry entry =
                     image.borrowResource(resource)) {
            if (entry == null || entry.bytes().length != size) {
                fail("CONNECTOR_RESOURCE_INVALID");
            }
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
        } catch (CharacterCodingException invalid) {
            fail("CONNECTOR_SERVICE_INVALID");
            return Collections.emptyList();
        }
        if (text.isEmpty() || text.indexOf('\r') >= 0
                || text.indexOf('\0') >= 0
                || text.charAt(text.length() - 1) != '\n') {
            fail("CONNECTOR_SERVICE_INVALID");
        }
        String[] lines = text.substring(0, text.length() - 1)
                .split("\\n", -1);
        if (lines.length > MAX_CANDIDATES) {
            fail("CONNECTOR_PROVIDER_LIMIT_EXCEEDED");
        }
        Set<String> providers = new HashSet<>();
        Set<String> resources = new HashSet<>();
        List<ServiceEntry> entries = new ArrayList<>(lines.length);
        for (String line : lines) {
            int separator = line.indexOf('=');
            if (separator <= 0 || separator != line.lastIndexOf('=')
                    || separator == line.length() - 1) {
                fail("CONNECTOR_SERVICE_INVALID");
            }
            String provider = line.substring(0, separator);
            String resource = line.substring(separator + 1);
            if (!CLASS_NAME.matcher(provider).matches()
                    || !providers.add(provider) || !resources.add(resource)
                    || resource.startsWith("/")
                    || resource.indexOf('\\') >= 0
                    || resource.contains("//")
                    || resource.contains("/../")
                    || resource.startsWith("../")
                    || resource.length() > 512) {
                fail("CONNECTOR_SERVICE_INVALID");
            }
            entries.add(new ServiceEntry(provider, resource));
        }
        return entries;
    }

    private static void requireProviderShape(Class<?> type) {
        int modifiers = type.getModifiers();
        if (!Modifier.isPublic(modifiers) || !Modifier.isFinal(modifiers)
                || type.isInterface() || Modifier.isAbstract(modifiers)) {
            fail("CONNECTOR_PROVIDER_CLASS_INVALID");
        }
        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                fail("CONNECTOR_PROVIDER_CLASS_INVALID");
            }
        }
        Constructor<?>[] constructors = type.getDeclaredConstructors();
        if (constructors.length != 1
                || constructors[0].getParameterTypes().length != 0
                || !Modifier.isPublic(constructors[0].getModifiers())) {
            fail("CONNECTOR_PROVIDER_CLASS_INVALID");
        }
    }

    private static void fail(String code) {
        throw new PortablePlanException(code, "connector.registry",
                "portable connector provider admission failed");
    }

    private static final class Candidate {
        private final VerifiedDeploymentImage image;
        private final String deploymentId;
        private final String providerClassName;
        private final byte[] contractBytes;
        private final String contractDigest;
        private final PortableConnectorProviderContract contract;

        private Candidate(
                VerifiedDeploymentImage image, String deploymentId,
                String providerClassName, byte[] contractBytes,
                PortableConnectorProviderContract contract) {
            this.image = image;
            this.deploymentId = deploymentId;
            this.providerClassName = providerClassName;
            this.contractBytes = contractBytes.clone();
            this.contractDigest =
                    PortableConnectorProviderContractCodec.digest(contract);
            this.contract = contract;
        }

        private Object load(VerifiedDeploymentSet deployments) {
            try {
                Class<?> type = deployments.classLoader(deploymentId)
                        .loadClass(providerClassName);
                requireProviderShape(type);
                Object provider = type.getConstructor().newInstance();
                if (!image.ownsClass(type)) {
                    fail("CONNECTOR_PROVIDER_CLASS_INVALID");
                }
                return provider;
            } catch (PortablePlanException failure) {
                throw failure;
            } catch (ReflectiveOperationException | LinkageError failure) {
                throw new PortablePlanException(
                        "CONNECTOR_PROVIDER_CLASS_INVALID",
                        "connector.provider",
                        "portable connector provider class is invalid", failure);
            }
        }

        private PortableBatchSourceProvider loadSource(
                VerifiedDeploymentSet deployments) {
            Object provider = load(deployments);
            if (!(provider instanceof PortableBatchSourceProvider)) {
                fail("CONNECTOR_PROVIDER_CLASS_INVALID");
            }
            return (PortableBatchSourceProvider) provider;
        }

        private PortableBatchSinkProvider loadSink(
                VerifiedDeploymentSet deployments) {
            Object provider = load(deployments);
            if (!(provider instanceof PortableBatchSinkProvider)) {
                fail("CONNECTOR_PROVIDER_CLASS_INVALID");
            }
            return (PortableBatchSinkProvider) provider;
        }
    }

    /** Core-owned metadata facade; no provider callback is reachable from Plan validation. */
    private static final class ResolvedSourceInput
            implements BatchExecutableSource {
        private static final long serialVersionUID = 1L;

        private final Source delegate;
        private final DataSchema dataSchema;
        private final BatchSourceSplitDescriptor descriptor;
        private transient SourceExecutor<?> preparedExecutor;

        private ResolvedSourceInput(
                Source delegate, DataSchema dataSchema,
                BatchSourceSplitDescriptor descriptor,
                SourceExecutor<?> preparedExecutor) {
            this.delegate = delegate;
            this.dataSchema = new DataSchema(dataSchema.getFields());
            this.descriptor = descriptor;
            this.preparedExecutor = preparedExecutor;
        }

        @Override public DataSchema getSchema() {
            return dataSchema;
        }

        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return descriptor;
        }

        @Override public synchronized SourceExecutor<?> createSourceExecutor() {
            SourceExecutor<?> executor = preparedExecutor;
            if (executor == null) {
                throw new IllegalStateException(
                        "portable source executor is unavailable or already opened");
            }
            preparedExecutor = null;
            return new DelegatingInputExecutor(this, executor);
        }

        @Override public String toString() {
            return "ResolvedSourceInput{" + delegate.getClass().getName() + '}';
        }
    }

    private static final class DelegatingInputExecutor
            extends SourceExecutor<ResolvedSourceInput> {
        private final SourceExecutor<?> delegate;

        private DelegatingInputExecutor(
                ResolvedSourceInput input, SourceExecutor<?> delegate) {
            super(input, input.dataSchema);
            this.delegate = delegate;
        }

        @Override public void open() { delegate.open(); }
        @Override public boolean hasNext() { return delegate.hasNext(); }
        @Override public DataRow next() { return delegate.next(); }
        @Override public void close() { delegate.close(); }
    }

    /** Core-owned metadata facade; row delivery/lifecycle remain the data-plane delegate. */
    private static final class ResolvedSinkOutput implements BatchSink {
        private static final long serialVersionUID = 1L;

        private final Sink delegate;
        private final DataSchema dataSchema;
        private final BatchSinkDescriptor descriptor;
        private final boolean workerWritable;
        private final String exclusiveDestinationId;
        private final boolean coordinatedCommit;

        private ResolvedSinkOutput(
                Sink delegate, DataSchema dataSchema,
                BatchSinkDescriptor descriptor, boolean workerWritable,
                String exclusiveDestinationId, boolean coordinatedCommit) {
            this.delegate = delegate;
            this.dataSchema = new DataSchema(dataSchema.getFields());
            this.descriptor = descriptor;
            this.workerWritable = workerWritable;
            this.exclusiveDestinationId = exclusiveDestinationId;
            this.coordinatedCommit = coordinatedCommit;
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public BatchSinkDescriptor batchSinkDescriptor() {
            return descriptor;
        }
        @Override public boolean isWorkerWritable() { return workerWritable; }
        @Override public String exclusiveDestinationId() {
            return exclusiveDestinationId;
        }
        @Override public boolean supportsCoordinatedCommit() {
            return coordinatedCommit;
        }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public void close() { delegate.close(); }
        @Override public void abort() { delegate.abort(); }
        @Override public void prepareCommit() { delegate.prepareCommit(); }
        @Override public void commitPrepared() { delegate.commitPrepared(); }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
    }

    private static final class ServiceEntry {
        private final String providerClassName;
        private final String contractResource;

        private ServiceEntry(String providerClassName, String contractResource) {
            this.providerClassName = providerClassName;
            this.contractResource = contractResource;
        }
    }
}
