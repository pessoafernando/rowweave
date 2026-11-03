package io.rowweave.deployment;

import io.rowweave.CoGroupFunction;
import io.rowweave.FilterFunction;
import io.rowweave.FlatMapFunction;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.MapFunction;
import io.rowweave.MapPartitionFunction;
import io.rowweave.StreamAggregateFunction;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.PortableFunctionProvider;
import io.rowweave.portable.PortableFunctionProviderContract;
import io.rowweave.portable.PortableFunctionProviderContractCodec;
import io.rowweave.portable.PortablePlanException;
import io.rowweave.internal.ProviderActionSupervisor;

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

/** Signed static contract registry; only Worker role may initialize provider code. */
public final class PortableFunctionProviderRegistry {
    public static final String SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.portable.PortableFunctionProvider";
    public static final int MAX_SERVICE_BYTES = 32 * 1024;
    public static final int MAX_PROVIDERS_PER_DEPLOYMENT = 64;
    public static final int MAX_CANDIDATES = 256;
    public static final String OPAQUE_CONFIGURATION_CODEC =
            "rowweave.opaque-bytes.v1";

    public enum Role { STATIC_ONLY, WORKER }

    private static final Pattern CLASS_NAME = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private final VerifiedDeploymentSet deployments;
    private final Role role;
    private final Map<Key, Candidate> candidates;

    private PortableFunctionProviderRegistry(
            VerifiedDeploymentSet deployments, Role role,
            Map<Key, Candidate> candidates) {
        this.deployments = deployments;
        this.role = role;
        this.candidates = Collections.unmodifiableMap(candidates);
    }

    public static PortableFunctionProviderRegistry scan(
            VerifiedDeploymentSet deployments, Role role) {
        if (deployments == null || role == null) {
            throw new IllegalArgumentException(
                    "deployments and function registry role are required");
        }
        Map<Key, Candidate> candidates = new HashMap<>();
        for (VerifiedDeploymentImage image
                : deployments.imagesInCanonicalOrder()) {
            discover(image, candidates);
        }
        return new PortableFunctionProviderRegistry(
                deployments, role, candidates);
    }

    /** Static-only verification: no class loader or provider instance is touched. */
    public void verify(FunctionDeploymentReference reference) {
        deployments.requireAdmissionOpen();
        if (reference == null
                || !deployments.deploymentSetId().equals(
                        reference.deploymentSetId())) {
            fail("FUNCTION_DEPLOYMENT_MISMATCH");
        }
        Candidate candidate = candidates.get(new Key(
                reference.deploymentId(), reference.className()));
        if (candidate == null
                || !digestEquals(candidate.jarDigest,
                        reference.deploymentJarDigest())) {
            fail("FUNCTION_DEPLOYMENT_MISMATCH");
        }
        candidate.contract.validateReference(reference);
        if (!OPAQUE_CONFIGURATION_CODEC.equals(
                reference.configurationCodecId())) {
            fail("FUNCTION_CONFIGURATION_CODEC_UNSUPPORTED");
        }
    }

    /** Resolves only after the caller has completed whole-plan metadata verification. */
    public Serializable resolve(
            FunctionDeploymentReference reference,
            ProviderActionSupervisor supervisor, long deadlineNanos) {
        if (role != Role.WORKER) {
            throw new IllegalStateException("FUNCTION_RUNTIME_REQUIRED");
        }
        if (supervisor == null) {
            throw new IllegalArgumentException(
                    "function provider action supervisor is required");
        }
        verify(reference);
        Candidate candidate = candidates.get(new Key(
                reference.deploymentId(), reference.className()));
        return supervisor.call("portable-function-create", deadlineNanos,
                () -> candidate.resolve(deployments, reference));
    }

    private static void discover(
            VerifiedDeploymentImage image, Map<Key, Candidate> candidates) {
        int serviceBytes = image.entryExpandedSize(SERVICE_RESOURCE);
        if (serviceBytes < 0) return;
        if (serviceBytes == 0 || serviceBytes > MAX_SERVICE_BYTES) {
            fail("FUNCTION_SERVICE_INVALID");
        }
        byte[] service = read(image, SERVICE_RESOURCE,
                MAX_SERVICE_BYTES, "FUNCTION_SERVICE_INVALID");
        List<ServiceEntry> entries = parseService(service);
        if (entries.size() > MAX_PROVIDERS_PER_DEPLOYMENT) {
            fail("FUNCTION_SERVICE_INVALID");
        }
        Set<String> providerClasses = new HashSet<>();
        Set<String> contractResources = new HashSet<>();
        for (ServiceEntry entry : entries) {
            if (!providerClasses.add(entry.providerClassName)
                    || !contractResources.add(entry.contractResource)
                    || !image.hasClass(entry.providerClassName)) {
                fail("FUNCTION_SERVICE_INVALID");
            }
            byte[] contractBytes = read(image, entry.contractResource,
                    PortableFunctionProviderContractCodec.MAX_FRAME_BYTES,
                    "FUNCTION_CONTRACT_INVALID");
            PortableFunctionProviderContract contract;
            try {
                contract = PortableFunctionProviderContractCodec.decode(
                        contractBytes);
            } catch (RuntimeException invalid) {
                throw new PortablePlanException("FUNCTION_CONTRACT_INVALID",
                        "function.contract",
                        "signed function contract is invalid", invalid);
            }
            if (!MessageDigest.isEqual(contractBytes,
                    PortableFunctionProviderContractCodec.encode(contract))
                    || !image.hasClass(contract.functionClassName())
                    || !OPAQUE_CONFIGURATION_CODEC.equals(
                            contract.configurationCodecId())) {
                fail("FUNCTION_CONTRACT_INVALID");
            }
            String deploymentId = image.manifest().deploymentId();
            Candidate candidate = new Candidate(
                    image, deploymentId, entry.providerClassName,
                    hex(image.manifest().sha256()), contractBytes, contract);
            if (candidates.size() >= MAX_CANDIDATES) {
                fail("FUNCTION_PROVIDER_LIMIT_EXCEEDED");
            }
            if (candidates.put(new Key(deploymentId,
                    contract.functionClassName()), candidate) != null) {
                fail("FUNCTION_PROVIDER_DUPLICATE");
            }
        }
    }

    private static byte[] read(
            VerifiedDeploymentImage image, String resource, int maximum,
            String failureCode) {
        int size = image.entryExpandedSize(resource);
        if (size <= 0 || size > maximum) fail(failureCode);
        try (VerifiedDeploymentImage.BorrowedEntry entry =
                     image.borrowResource(resource)) {
            if (entry == null || entry.bytes().length != size) {
                fail(failureCode);
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
            fail("FUNCTION_SERVICE_INVALID");
            return Collections.emptyList();
        }
        if (text.isEmpty() || text.indexOf('\r') >= 0
                || text.indexOf('\0') >= 0
                || text.charAt(text.length() - 1) != '\n') {
            fail("FUNCTION_SERVICE_INVALID");
        }
        String[] lines = text.substring(0, text.length() - 1)
                .split("\\n", -1);
        List<ServiceEntry> entries = new ArrayList<>(lines.length);
        for (String line : lines) {
            int separator = line.indexOf('=');
            if (separator <= 0 || separator != line.lastIndexOf('=')
                    || separator == line.length() - 1) {
                fail("FUNCTION_SERVICE_INVALID");
            }
            String provider = line.substring(0, separator);
            String resource = line.substring(separator + 1);
            if (!CLASS_NAME.matcher(provider).matches()
                    || resource.startsWith("/")
                    || resource.indexOf('\\') >= 0
                    || resource.contains("//")
                    || resource.contains("/../")
                    || resource.startsWith("../")
                    || resource.length() > 512) {
                fail("FUNCTION_SERVICE_INVALID");
            }
            entries.add(new ServiceEntry(provider, resource));
        }
        return entries;
    }

    private static boolean matches(FunctionAbi abi, Serializable function) {
        switch (abi) {
            case FILTER_V1: return function instanceof FilterFunction;
            case MAP_V1: return function instanceof MapFunction;
            case FLAT_MAP_V1: return function instanceof FlatMapFunction;
            case REDUCE_GROUP_V1: return function instanceof GroupReduceFunction;
            case COMBINE_GROUP_V1:
                return function instanceof GroupCombineFunction;
            case COMBINE_REDUCE_GROUP_V1:
                return function instanceof GroupCombineReduceFunction;
            case CO_GROUP_V1: return function instanceof CoGroupFunction;
            case MAP_PARTITION_V1:
                return function instanceof MapPartitionFunction;
            case STREAM_AGGREGATE_V1:
                return function instanceof StreamAggregateFunction;
            case STREAM_RETRACT_AGGREGATE_V2:
                return function instanceof
                        io.rowweave.RetractableStreamAggregateFunction;
            case STREAM_RETRACT_WINDOW_AGGREGATE_V2:
                return function instanceof
                        io.rowweave.RetractableStreamWindowAggregateFunction;
            default: return false;
        }
    }

    private static boolean digestEquals(String left, String right) {
        return MessageDigest.isEqual(
                left.getBytes(StandardCharsets.US_ASCII),
                right.getBytes(StandardCharsets.US_ASCII));
    }

    private static String hex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            result[index * 2] = digits[(bytes[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[bytes[index] & 0xf];
        }
        return new String(result);
    }

    private static void fail(String code) {
        throw new PortablePlanException(code, "function.registry",
                "portable function provider admission failed");
    }

    private static final class Candidate {
        private final VerifiedDeploymentImage image;
        private final String deploymentId;
        private final String providerClassName;
        private final String jarDigest;
        private final byte[] contractBytes;
        private final PortableFunctionProviderContract contract;

        private Candidate(
                VerifiedDeploymentImage image, String deploymentId,
                String providerClassName, String jarDigest,
                byte[] contractBytes,
                PortableFunctionProviderContract contract) {
            this.image = image;
            this.deploymentId = deploymentId;
            this.providerClassName = providerClassName;
            this.jarDigest = jarDigest;
            this.contractBytes = contractBytes.clone();
            this.contract = contract;
        }

        private PortableFunctionProvider load(
                VerifiedDeploymentSet deployments) {
            try {
                Class<?> type = deployments.classLoader(deploymentId)
                        .loadClass(providerClassName);
                requireProviderShape(type);
                Object provider = type.getConstructor().newInstance();
                if (!(provider instanceof PortableFunctionProvider)
                        || !image.ownsClass(type)) {
                    fail("FUNCTION_PROVIDER_CLASS_INVALID");
                }
                return (PortableFunctionProvider) provider;
            } catch (PortablePlanException failure) {
                throw failure;
            } catch (ReflectiveOperationException | LinkageError failure) {
                throw new PortablePlanException(
                        "FUNCTION_PROVIDER_CLASS_INVALID",
                        "function.provider",
                        "portable function provider class is invalid", failure);
            }
        }

        private Serializable resolve(
                VerifiedDeploymentSet deployments,
                FunctionDeploymentReference reference) {
            PortableFunctionProvider provider = load(deployments);
            byte[] runtimeContract = PortableFunctionProviderContractCodec.encode(
                    provider.contract());
            if (!MessageDigest.isEqual(contractBytes, runtimeContract)) {
                fail("FUNCTION_CONTRACT_DRIFT");
            }
            final Serializable function;
            try {
                function = provider.create(reference.configurationFrame());
            } catch (RuntimeException | Error failure) {
                fail("FUNCTION_CREATE_FAILED");
                return null;
            }
            if (function == null
                    || !contract.functionClassName().equals(
                            function.getClass().getName())
                    || !image.ownsClass(function.getClass())
                    || !matches(contract.functionAbi(), function)) {
                fail("FUNCTION_RUNTIME_MISMATCH");
            }
            return function;
        }
    }

    private static void requireProviderShape(Class<?> type) {
        int modifiers = type.getModifiers();
        if (!Modifier.isPublic(modifiers) || !Modifier.isFinal(modifiers)
                || type.isInterface() || Modifier.isAbstract(modifiers)) {
            fail("FUNCTION_PROVIDER_CLASS_INVALID");
        }
        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                fail("FUNCTION_PROVIDER_CLASS_INVALID");
            }
        }
        Constructor<?>[] constructors = type.getDeclaredConstructors();
        if (constructors.length != 1
                || constructors[0].getParameterTypes().length != 0
                || !Modifier.isPublic(constructors[0].getModifiers())) {
            fail("FUNCTION_PROVIDER_CLASS_INVALID");
        }
    }

    private static final class Key {
        private final String deploymentId;
        private final String functionClassName;

        private Key(String deploymentId, String functionClassName) {
            this.deploymentId = deploymentId;
            this.functionClassName = functionClassName;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Key)) return false;
            Key that = (Key) other;
            return deploymentId.equals(that.deploymentId)
                    && functionClassName.equals(that.functionClassName);
        }

        @Override public int hashCode() {
            return 31 * deploymentId.hashCode()
                    + functionClassName.hashCode();
        }
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
