package io.rowweave.stream.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** shape dispatch cannot hide behind a renamed predicate. */
class StreamShapeGateArchitectureGuardTest {
    private static final List<String> OPCODE_BLIND_ORCHESTRATORS =
            Arrays.asList(
                    "StreamGenericTaskDeployment.java",
                    "StreamNodeTaskPlacementPlanner.java",
                    "StreamPhysicalExecutionPlan.java",
                    "StreamProductJobMasterServer.java",
                    "StreamProductRunFactory.java",
                    "StreamWorkerCapabilityPool.java",
                    "StreamWorkerPlacement.java");

    @Test
    void scannerRejectsRenamedPredicatesCardinalityAndPredecessorOpcodes() {
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "boolean acceptsSilverTopology(Graph graph) { return true; }")
                .contains("topology-classifier"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "boolean classifySpecialGraph(Graph graph) { return true; }")
                .contains("topology-classifier"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "if (portablePlan.nodes().size() == 3) dispatchSpecial();")
                .contains("fixed-graph-cardinality"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "if (3 == graph.nodes().size()) dispatchSpecial();")
                .contains("fixed-graph-cardinality"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "if (graph.edges().size() == 2) dispatchSpecial();")
                .contains("fixed-graph-cardinality"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "if (context.node().producer().opcode() != "
                        + "StreamOperatorCode.KEY_BY) reject();")
                .contains("predecessor-opcode-classifier"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "boolean eligiblePlan(StreamExecutionGraph graph) { "
                        + "return graph != null; }")
                .contains("graph-boolean-predicate"));
        assertFalse(StreamShapeGateArchitectureGuard.findings(
                "boolean accepts(PortableStreamPlan.ChangelogMode mode) { "
                        + "return mode != null; }")
                .contains("graph-boolean-predicate"));
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                "if (parent.opcode() == StreamOperatorCode.MAP) reject();")
                .contains("predecessor-opcode-classifier"));
    }

    @Test
    void productionHasNoGeneralizedTopologyClassifier() throws Exception {
        assertNoFindings(streamSourceRoot());
        Path compiler = streamSourceRoot().getParent().resolve(
                "StreamPlanCompiler.java");
        assertTrue(StreamShapeGateArchitectureGuard.findings(
                        read(compiler)).isEmpty(),
                compiler + " -> " + StreamShapeGateArchitectureGuard
                        .findings(read(compiler)));
    }

    @Test
    void admissionPlacementAndDeploymentAreOpcodeBlind() throws Exception {
        Path cluster = streamSourceRoot().resolve("cluster");
        for (String file : OPCODE_BLIND_ORCHESTRATORS) {
            String source = read(cluster.resolve(file));
            assertFalse(StreamShapeGateArchitectureGuard
                            .inspectsOpcode(source),
                    file + " must operate only on node/edge contracts");
        }
    }

    @Test
    void workerAdmissionAndTaskDispatchAreOpcodeBlind() throws Exception {
        String worker = read(streamSourceRoot().resolve("cluster").resolve(
                "StreamRemoteWorkerServer.java"));
        String dispatch = StreamShapeGateArchitectureGuard.methodBody(
                worker, "dispatchOperatorTask(");
        assertFalse(StreamShapeGateArchitectureGuard.inspectsOpcode(dispatch),
                "generic task admission/dispatch cannot select by opcode");
    }

    @Test
    void masterLaunchIsOpcodeBlind() throws Exception {
        String masterRun = read(streamSourceRoot().resolve("cluster").resolve(
                "RemoteStreamSourceSinkRunFactory.java"));
        String launch = StreamShapeGateArchitectureGuard.methodBody(
                masterRun, "public void start()");
        assertFalse(StreamShapeGateArchitectureGuard.inspectsOpcode(launch),
                "whole-graph launch cannot select an executor by opcode");
        assertTrue(launch.contains(
                        "StreamGenericTaskDeployment.deployWithTaskProof("),
                "whole-graph launch must enter the generic deployment");
    }

    @Test
    void genericMailboxTaskUsesTheTransportNeutralInputContract()
            throws Exception {
        String task = read(streamSourceRoot().resolve("runtime").resolve(
                "StreamTask.java"));
        assertFalse(task.contains("LocalStreamChannel"),
                "generic mailbox task cannot bind the local lane class");
        assertTrue(task.contains("StreamChannelInput"));
    }

    private static String read(Path path) throws java.io.IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static void assertNoFindings(Path root) throws Exception {
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            String source = read(path);
                            assertTrue(StreamShapeGateArchitectureGuard
                                            .findings(source).isEmpty(),
                                    path + " -> "
                                            + StreamShapeGateArchitectureGuard
                                            .findings(source));
                        } catch (java.io.IOException failure) {
                            throw new IllegalStateException(failure);
                        }
                    });
        }
    }

    private static Path streamSourceRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        Path local = cwd.resolve("src/main/java/io/rowweave/stream");
        return Files.isDirectory(local) ? local : cwd.resolve(
                "rowweave-core/src/main/java/io/rowweave/stream");
    }
}
