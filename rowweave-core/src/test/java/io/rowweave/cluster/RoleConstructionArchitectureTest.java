package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** role construction ownership and activation protocol must not drift silently. */
class RoleConstructionArchitectureTest {

    private static final Set<String> MASTER_LIFECYCLE_FIELDS = names(
            "assignmentDispatcher", "assignmentLanes", "artifactJanitor",
            "cancellationCoordinator", "deploymentRuntime", "heartbeatMonitorThread",
            "heartbeatMonitorTimer", "leadership", "recoveryInventory", "rpcCache",
            "rpcEndpoint", "terminalRunJanitor");

    private static final Set<String> WORKER_LIFECYCLE_FIELDS = names(
            "dataAuthorizations", "dataInbound", "dataPlaneHandshakeAdmission",
            "dataServer", "deploymentRuntime", "heartbeatCache", "heartbeatTimer",
            "leaderSubscription", "localShuffle", "maintenanceTimer", "muxConnections",
            "preparedOutputs", "registrationCache", "reportCache", "rpcEndpoint", "stagePool");

    private static final Set<String> MASTER_TRANSACTIONAL_FIELDS = names(
            "assignmentDispatcher", "assignmentLanes", "artifactJanitor",
            "cancellationCoordinator", "deploymentRuntime", "recoveryInventory",
            "rpcCache", "rpcEndpoint", "terminalRunJanitor");

    private static final Set<String> WORKER_TRANSACTIONAL_FIELDS = names(
            "dataAuthorizations", "dataInbound", "deploymentRuntime", "heartbeatCache",
            "localShuffle", "muxConnections", "preparedOutputs", "registrationCache", "reportCache",
            "rpcEndpoint", "stagePool");

    @Test
    void lifecycleResourceFieldInventoryCannotDriftSilently() {
        assertEquals(MASTER_LIFECYCLE_FIELDS, lifecycleFields(MasterServer.class),
                "new Master lifecycle resources require explicit ownership/transaction review");
        assertEquals(WORKER_LIFECYCLE_FIELDS, lifecycleFields(WorkerServer.class),
                "new Worker lifecycle resources require explicit ownership/transaction review");
    }

    @Test
    void lifecycleResourcesCannotReturnToFieldInitializers() throws Exception {
        assertConstructorAssigned(source("MasterServer.java"), MASTER_LIFECYCLE_FIELDS);
        assertConstructorAssigned(source("WorkerServer.java"), WORKER_LIFECYCLE_FIELDS);
    }

    @Test
    void masterConstructionKeepsEveryOwnedResourceInOneTransaction() throws Exception {
        String source = source("MasterServer.java");
        assertEquals(9, occurrences(source, "transaction.acquire("),
                "Master acquire-site drift requires an ownership audit");
        assertTransactionalAssignments(source, MASTER_TRANSACTIONAL_FIELDS,
                "deploymentRuntime");
        assertConstructionProtocol(source, "startHeartbeatMonitor(heartbeatCheckMs)");
        assertFalse(constructionBeforeCommit(source).contains("startHeartbeatMonitor("));
    }

    @Test
    void workerConstructionKeepsEveryOwnedResourceInOneTransaction() throws Exception {
        String source = source("WorkerServer.java");
        assertEquals(12, occurrences(source, "transaction.acquire("),
                "Worker acquire-site drift requires an ownership audit, including LIVE claim");
        assertTransactionalAssignments(source, WORKER_TRANSACTIONAL_FIELDS,
                "deploymentRuntime", "dataAuthorizations", "dataInbound", "muxConnections");
        String beforeCommit = constructionBeforeCommit(source);
        assertFalse(beforeCommit.contains("subscribeLeaderChanges("));
        assertFalse(beforeCommit.contains("registerWorkerLease("));
        assertFalse(beforeCommit.contains("registerWithLeader("));
        assertFalse(beforeCommit.contains("startHeartbeat("));
        assertConstructionProtocol(source,
                "subscribeLeaderChanges(this::onLeaderChanged)",
                "registerWorkerLease()", "registerWithLeader()",
                "startHeartbeat(heartbeatIntervalMs)");
    }

    private static void assertTransactionalAssignments(String source, Set<String> fields,
                                                        String... indirectFields) {
        Set<String> indirect = names(indirectFields);
        for (String field : fields) {
            if (indirect.contains(field)) {
                continue;
            }
            assertTrue(source.contains("this." + field + " = transaction.acquire("),
                    field + " must be acquired and registered atomically");
        }
        assertTrue(source.contains("? transaction.acquire(() -> runtimeSource.ownedFactory.create(),"),
                "owned DeploymentRuntime must be created only inside the role transaction");
        for (String conditional : indirect) {
            if (!"deploymentRuntime".equals(conditional)) {
                int assignmentStart = source.indexOf("this." + conditional + " =");
                assertTrue(assignmentStart >= 0,
                        conditional + " assignment disappeared and requires guard review");
                int assignmentEnd = source.indexOf(';', assignmentStart);
                assertTrue(assignmentEnd > assignmentStart);
                assertTrue(source.substring(assignmentStart, assignmentEnd)
                                .contains("transaction.acquire("),
                        conditional + " must retain its conditional transaction acquire");
            }
        }
    }

    private static void assertConstructionProtocol(String source, String... postCommitActions) {
        assertEquals(1, occurrences(source, "transaction.commit();"));
        assertEquals(1, occurrences(source, "rpcEndpoint.activate();"));
        assertEquals(1, occurrences(source, "RpcServer.bindDormant("));
        assertEquals(0, occurrences(source, "new RpcServer("),
                "roles must bind dormant RPC and activate only after commit");
        assertTrue(source.contains("catch (Throwable constructionFailure)"));
        assertTrue(source.contains("catch (Throwable postInitFailure)"));

        int cursor = source.indexOf("transaction.commit();");
        assertTrue(cursor >= 0);
        cursor = orderedAfter(source, cursor, "rpcEndpoint.activate();");
        for (String action : postCommitActions) {
            cursor = orderedAfter(source, cursor, action);
        }

        String beforeCommit = constructionBeforeCommit(source);
        assertFalse(beforeCommit.contains("rpcEndpoint.activate()"));
        assertFalse(beforeCommit.contains(".start();"),
                "role constructors cannot start externally visible threads before commit");
    }

    private static int orderedAfter(String source, int cursor, String token) {
        int next = source.indexOf(token, cursor + 1);
        assertTrue(next > cursor, token + " must remain after the preceding construction step");
        return next;
    }

    private static String constructionBeforeCommit(String source) {
        int begin = source.indexOf(
                "RoleConstructionTransaction transaction = new RoleConstructionTransaction();");
        int end = source.indexOf("transaction.commit();", begin);
        assertTrue(begin >= 0 && end > begin, "deep constructor transaction boundary missing");
        return source.substring(begin, end);
    }

    private static void assertConstructorAssigned(String source, Set<String> fields) {
        for (String field : fields) {
            String declaration = null;
            for (String line : source.split("\\R")) {
                String candidate = line.trim();
                if (candidate.startsWith("private ")
                        && candidate.contains(" " + field + ";")) {
                    declaration = candidate;
                    break;
                }
            }
            assertTrue(declaration != null, field + " lifecycle field declaration missing");
            assertFalse(declaration.contains("="),
                    field + " must not allocate/start a lifecycle resource in a field initializer");
        }
    }

    private static Set<String> lifecycleFields(Class<?> role) {
        Set<String> fields = new TreeSet<>();
        for (Field field : role.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            Class<?> type = field.getType();
            if (AutoCloseable.class.isAssignableFrom(type)
                    || ExecutorService.class.isAssignableFrom(type)
                    || Thread.class.isAssignableFrom(type)) {
                fields.add(field.getName());
            }
        }
        return fields;
    }

    private static String source(String fileName) throws Exception {
        Path source = sourceRoot().resolve("io/rowweave/cluster").resolve(fileName);
        return new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    }

    private static Path sourceRoot() {
        Path current = Paths.get("").toAbsolutePath();
        Path direct = current.resolve("src/main/java");
        return Files.isDirectory(direct) ? direct
                : current.resolve("rowweave-core/src/main/java");
    }

    private static Set<String> names(String... names) {
        return new TreeSet<>(Arrays.asList(names));
    }

    private static int occurrences(String source, String token) {
        int count = 0;
        for (int offset = 0; (offset = source.indexOf(token, offset)) >= 0;
             offset += token.length()) {
            count++;
        }
        return count;
    }
}
