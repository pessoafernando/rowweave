package io.rowweave.release;

import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableBatchPlanCodec;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableConnectorProviderContractCodec;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableFunctionProviderContract;
import io.rowweave.portable.PortableFunctionProviderContractCodec;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.StoreFormatHeaderCodec;
import io.rowweave.upgrade.UpgradeAuthorityRecord;
import io.rowweave.upgrade.UpgradeAuthorityRecordCodec;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReleaseContractCompatibilityRunnerTest {

    @Test
    void frozenV10RunnerProducesCheckedInGoldenAcceptedByCurrentReader()
            throws Exception {
        String emitted = run("emit");
        String checkedIn = resource("/compat/v1_0/release-manifest.hex").trim();
        assertEquals(checkedIn, emitted);

        ReleaseContractManifest decoded = ReleaseContractManifestCodec.decode(
                fromHex(emitted));
        assertEquals("rowweave-1.0.0", decoded.releaseId());
        assertEquals(1, decoded.protocolEpoch());
        assertEquals(0, decoded.releaseRevision());
        assertEquals("rowweave-decimal-semantics-v1",
                decoded.decimalSemanticsId());
    }

    @Test
    void currentRevisionZeroWriterIsAcceptedByFrozenV10Reader()
            throws Exception {
        ReleaseComponentContract component = new ReleaseComponentContract(
                "rowweave.plan-artifact-envelope", 1, 0, 0, 0, 0,
                ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('1', 64), true);
        ReleaseContractManifest manifest = new ReleaseContractManifest(
                "rowweave-1.0.0", 1, 0, 0, 0,
                Collections.singletonList(component),
                Collections.singletonList("rowweave.runtime.batch.v1"),
                repeat('a', 64), "rowweave-decimal-semantics-v1");

        String projection = run("project", hex(
                ReleaseContractManifestCodec.encode(manifest)));
        assertEquals("rowweave-1.0.0|1|0|rowweave.plan-artifact-envelope|"
                + "rowweave.runtime.batch.v1|rowweave-decimal-semantics-v1",
                projection);
    }

    @Test
    void frozenV10ReaderRejectsRevisionOneBeforeSemanticUse()
            throws Exception {
        ReleaseComponentContract component = new ReleaseComponentContract(
                "rowweave.plan-artifact-envelope", 1, 0, 1, 0, 1,
                ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                repeat('1', 64), true);
        ReleaseContractManifest revisionOne = new ReleaseContractManifest(
                "rowweave-1.1.0", 1, 1, 0, 0,
                Collections.singletonList(component),
                Arrays.asList("rowweave.runtime.batch.v1", "rowweave.required.n-only.v1"),
                repeat('a', 64), "rowweave-decimal-semantics-v1");

        assertEquals("READER_TOO_OLD", run("project",
                hex(ReleaseContractManifestCodec.encode(revisionOne))));
    }

    @Test
    void frozenV11RunnerReadsV10AndProducesRequiredV11Golden()
            throws Exception {
        String v10 = resource("/compat/v1_0/release-manifest.hex").trim();
        String projection = runWith("compat.v1_1.ReleaseContractV11Runner",
                "project", v10);
        assertEquals("rowweave-1.0.0|1|0|rowweave.plan-artifact-envelope|"
                + "rowweave.runtime.batch.v1|rowweave-decimal-semantics-v1",
                projection);

        String v11 = runWith("compat.v1_1.ReleaseContractV11Runner", "emit");
        assertEquals(resource("/compat/v1_1/release-manifest.hex").trim(), v11);
        ReleaseContractManifest decoded = ReleaseContractManifestCodec.decode(
                fromHex(v11));
        assertEquals(1, decoded.releaseRevision());
        assertTrue(decoded.runtimeCapabilities().contains(
                "rowweave.required.n-only.v1"));
        assertEquals("READER_TOO_OLD", run("project", v11));
    }

    @Test
    void frozenRunnerHasNoProductionCodecDependency() throws Exception {
        assertStandaloneRunner(
                "src/test/java/compat/v1_0/ReleaseContractV10Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_1/ReleaseContractV11Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_2/PortableContractsV12Wire.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_2/FunctionProviderContractV12Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_2/PortableBatchPlanV12Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_2/ConnectorProviderContractV12Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_3/StoreContractsV13Wire.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_3/StoreFormatHeaderV13Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_3/UpgradeAuthorityV13Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/v1_4/ControlProtocolV14Runner.java");
        assertStandaloneRunner(
                "src/test/java/compat/stream_v1/PortableStreamPlanV1Runner.java");
    }

    @Test
    void frozenV12RunnersProduceCheckedInPortablePublicFrames()
            throws Exception {
        assertPortableFrame(
                "compat.v1_2.FunctionProviderContractV12Runner",
                "/compat/v1_2/function-provider.hex", "function");
        assertPortableFrame(
                "compat.v1_2.PortableBatchPlanV12Runner",
                "/compat/v1_2/portable-batch-plan.hex", "plan");
        assertPortableFrame(
                "compat.v1_2.ConnectorProviderContractV12Runner",
                "/compat/v1_2/portable-connector-provider.hex", "connector");
    }

    @Test
    void frozenV13RunnersProduceStoreFramesAcceptedByCurrentReaders()
            throws Exception {
        assertStoreFrame("compat.v1_3.StoreFormatHeaderV13Runner",
                "/compat/v1_3/store-format-header.hex", "header");
        assertStoreFrame("compat.v1_3.UpgradeAuthorityV13Runner",
                "/compat/v1_3/upgrade-authority.hex", "authority");
    }

    @Test
    void frozenV14RunnerProducesHelloAcceptedByCurrentReader()
            throws Exception {
        String checkedIn = resource(
                "/compat/v1_4/control-protocol-hello.hex").trim();
        assertEquals(checkedIn, runWith(
                "compat.v1_4.ControlProtocolV14Runner", "emit"));
        io.rowweave.cluster.rpc.ProtocolHelloV1 hello =
                io.rowweave.cluster.rpc.ProtocolHelloV1Codec.decode(
                        fromHex(checkedIn));
        assertEquals(runWith("compat.v1_4.ControlProtocolV14Runner",
                        "project", checkedIn),
                hello.protocolEpoch() + "|" + hello.readerRevisions().minimum()
                        + "-" + hello.readerRevisions().maximum() + "|"
                        + hello.writerRevisions().minimum() + "-"
                        + hello.writerRevisions().maximum() + "|"
                        + hello.messageIds().size() + "|"
                        + String.join(",", hello.runtimeCapabilities()) + "|"
                        + hello.maximumFrameBytes() + "|"
                        + hello.maximumResidentBytes());
    }

    @Test
    void frozenPortablePlanRunnerProducesGoldenAcceptedByCurrentReader()
            throws Exception {
        String checkedIn = resource(
                "/compat/stream_v1/portable-stream-plan.hex").trim();
        assertEquals(checkedIn, runWith(
                "compat.stream_v1.PortableStreamPlanV1Runner", "emit"));
        String frozenProjection = runWith(
                "compat.stream_v1.PortableStreamPlanV1Runner",
                "project", checkedIn);
        PortableStreamPlan current = PortableStreamPlanCodec.decode(
                fromHex(checkedIn));
        assertEquals(frozenProjection, currentStreamProjection(current));
    }

    @Test
    void currentPortablePlanWriterProducesFrameAcceptedByFrozenReader()
            throws Exception {
        String golden = resource(
                "/compat/stream_v1/portable-stream-plan.hex").trim();
        PortableStreamPlan current = PortableStreamPlanCodec.decode(
                fromHex(golden));
        String currentFrame = hex(PortableStreamPlanCodec.encode(current));
        assertEquals(currentStreamProjection(current), runWith(
                "compat.stream_v1.PortableStreamPlanV1Runner",
                "project", currentFrame));
    }

    private static String currentStreamProjection(PortableStreamPlan plan) {
        List<String> nodes = new ArrayList<>();
        for (PortableStreamNode node : plan.nodes()) {
            nodes.add(node.operatorUid() + ":" + node.opcode().name());
        }
        return plan.planVersion() + "|" + plan.deliveryGuarantee().name()
                + "|" + plan.parallelism() + "|" + plan.maxParallelism()
                + "|" + plan.changelogMode().name() + "|"
                + plan.timeMode().name() + "|"
                + join(plan.requiredCapabilities()) + "|"
                + join(plan.rootSinkOperatorUids()) + "|" + join(nodes);
    }

    private static void assertStoreFrame(
            String runner, String resource, String component) throws Exception {
        String checkedIn = resource(resource).trim();
        assertEquals(checkedIn, runWith(runner, "emit"));
        String frozenProjection = runWith(runner, "project", checkedIn);
        String currentProjection;
        if ("header".equals(component)) {
            StoreFormatHeader header = StoreFormatHeaderCodec.decode(
                    fromHex(checkedIn));
            currentProjection = header.formatVersion() + "|" + header.storeId()
                    + "|" + header.protocolEpoch() + "|"
                    + header.activeRevision() + "|" + header.manifestDigest();
        } else {
            UpgradeAuthorityRecord authority =
                    UpgradeAuthorityRecordCodec.decode(fromHex(checkedIn));
            currentProjection = authority.recordVersion() + "|"
                    + authority.activeHeader().storeId() + "|"
                    + authority.generation() + "|" + authority.phase().wireId()
                    + "|" + authority.sourceNamespace() + "|"
                    + authority.shadowNamespace() + "|"
                    + authority.owner().term() + "|"
                    + authority.activePointerVersion() + "|"
                    + authority.writerEpoch() + "|"
                    + authority.verifiedObjectCount() + "|"
                    + authority.verifiedShardCount() + "|"
                    + authority.writesAllowed() + "|"
                    + authority.newSemanticsObserved();
        }
        assertEquals(frozenProjection, currentProjection);
    }

    private static void assertPortableFrame(
            String runner, String resource, String component) throws Exception {
        String checkedIn = resource(resource).trim();
        assertEquals(checkedIn, runWith(runner, "emit"));
        String frozenProjection = runWith(runner, "project", checkedIn);
        assertEquals(frozenProjection, currentProjection(component,
                fromHex(checkedIn)));
    }

    private static String currentProjection(String component, byte[] frame) {
        if ("function".equals(component)) {
            PortableFunctionProviderContract contract =
                    PortableFunctionProviderContractCodec.decode(frame);
            return contract.contractVersion() + "|" + contract.functionClassName()
                    + "|" + contract.functionAbi().wireId() + "|"
                    + contract.configurationCodecId() + "|"
                    + contract.maximumConfigurationBytes() + "|"
                    + contract.maximumResources().maximumHeapBytes() + "|"
                    + contract.maximumResources().maximumDirectBytes() + "|"
                    + contract.maximumResources().maximumOutputRowsPerInput();
        }
        if ("connector".equals(component)) {
            PortableConnectorProviderContract contract =
                    PortableConnectorProviderContractCodec.decode(frame);
            int kind = contract.bindingKind()
                    == io.rowweave.portable.PortableOperatorCode.BindingKind.SOURCE
                    ? 1 : 2;
            return contract.contractVersion() + "|" + kind + "|"
                    + contract.providerKey() + "|"
                    + contract.configurationCodecId() + "|"
                    + contract.maximumConfigurationBytes() + "|"
                    + contract.maximumPayloadBytes() + "|"
                    + contract.maximumInstances();
        }
        if ("plan".equals(component)) {
            return currentPlanProjection(PortableBatchPlanCodec.decode(frame));
        }
        throw new IllegalArgumentException("unknown component");
    }

    private static String currentPlanProjection(PortableBatchPlan plan) {
        List<String> nodes = new ArrayList<>();
        for (PortablePlanNode node : plan.nodes()) {
            List<String> fields = new ArrayList<>();
            for (PortableField field : node.outputSchema().fields()) {
                fields.add(field.fieldId() + ":" + field.dataType().wireId()
                        + ":" + field.nullable());
            }
            List<String> scalar = new ArrayList<>();
            for (Map.Entry<String, String> entry
                    : node.scalarParameters().entrySet()) {
                scalar.add(entry.getKey() + ":" + entry.getValue());
            }
            List<String> lists = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry
                    : node.listParameters().entrySet()) {
                lists.add(entry.getKey() + ":" + join(entry.getValue()));
            }
            String binding = "none";
            if (node.binding() instanceof PortableSourceBinding) {
                binding = "source:"
                        + ((PortableSourceBinding) node.binding()).providerKey();
            } else if (node.binding() instanceof PortableSinkBinding) {
                binding = "sink:"
                        + ((PortableSinkBinding) node.binding()).providerKey();
            }
            nodes.add(node.operatorId() + "@" + node.opcode().wireId()
                    + "(inputs=" + join(node.inputOperatorIds())
                    + ";fields=" + join(fields) + ";binding=" + binding
                    + ";scalar=" + join(scalar) + ";lists=" + join(lists)
                    + ")");
        }
        return plan.planVersion() + "|" + plan.desiredParallelism() + "|"
                + join(plan.requiredCapabilities()) + "|"
                + join(plan.rootSinkOperatorIds()) + "|" + join(nodes, ";");
    }

    private static String run(String... arguments) throws Exception {
        return runWith("compat.v1_0.ReleaseContractV10Runner", arguments);
    }

    private static String runWith(String mainClass, String... arguments)
            throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String[] command = new String[arguments.length + 4];
        command[0] = java;
        command[1] = "-cp";
        command[2] = System.getProperty("java.class.path");
        command[3] = mainClass;
        System.arraycopy(arguments, 0, command, 4, arguments.length);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output;
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining("\n")).trim();
        }
        assertEquals(0, process.waitFor(), output);
        return output;
    }

    private static String resource(String name) throws Exception {
        try (InputStream input = ReleaseContractCompatibilityRunnerTest.class
                .getResourceAsStream(name)) {
            if (input == null) throw new AssertionError("missing resource " + name);
            return new BufferedReader(new InputStreamReader(input, StandardCharsets.US_ASCII))
                    .lines().collect(Collectors.joining("\n"));
        }
    }

    private static void assertStandaloneRunner(String sourcePath)
            throws Exception {
        Path source = Paths.get(sourcePath);
        String text = new String(Files.readAllBytes(source),
                StandardCharsets.UTF_8);
        assertFalse(text.contains("io.rowweave.release"));
        assertFalse(text.contains("io.rowweave.portable"));
        assertFalse(text.contains("ReleaseContractManifestCodec"));
        assertFalse(text.contains("io.rowweave.upgrade"));
        assertFalse(text.contains("StoreFormatHeaderCodec"));
        assertFalse(text.contains("UpgradeAuthorityRecordCodec"));
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }

    private static byte[] fromHex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String join(List<String> values) {
        return join(values, ",");
    }

    private static String join(List<String> values, String separator) {
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) result.append(separator);
            result.append(values.get(index));
        }
        return result.toString();
    }
}
