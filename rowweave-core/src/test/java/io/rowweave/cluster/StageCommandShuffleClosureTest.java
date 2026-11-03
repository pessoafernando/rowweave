package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** final StageCommand fields must consume the exact route/grant closure. */
class StageCommandShuffleClosureTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("closure-run", "closure-attempt", 1L);

    @Test
    void exactProducerAndConsumerCommandsCloseTheOnlyRemoteRoute() {
        Fixture fixture = fixture();
        StageCommandShuffleClosure closure = fixture.closure();

        closure.accept(1, 0, "w1", fixture.consumer());
        closure.accept(0, 0, "w0", fixture.producer(fixture.endpoint()));
        closure.assertComplete();
    }

    @Test
    void completionRejectsAPlannerRouteMissingFromFinalProducerCommand() {
        Fixture fixture = fixture();
        StageCommandShuffleClosure closure = fixture.closure();

        closure.accept(1, 0, "w1", fixture.consumer());
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, closure::assertComplete);

        assertTrue(failure.getMessage().contains("unconsumed"), failure.getMessage());
    }

    @Test
    void producerCommandCannotSubstituteAnotherBearerForThePlannedRoute() {
        Fixture fixture = fixture();
        ShuffleEndpoint expected = fixture.endpoint();
        byte[] wrong = new byte[32];
        Arrays.fill(wrong, (byte) 0x5a);
        ShuffleEndpoint substituted = ShuffleEndpoint.muxAuthorized(
                expected.host(), expected.dataPort(), expected.targetId(),
                expected.destinationIdentity(),
                io.rowweave.sched.net.ShuffleProducerGrant.fromBytes(wrong),
                expected.producerOrdinal(), expected.cohortId(),
                expected.expectedOrdinalCount());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> fixture.closure().accept(
                        0, 0, "w0", fixture.producer(substituted)));

        assertTrue(failure.getMessage().contains("producer target"),
                failure.getMessage());
    }

    private static Fixture fixture() {
        ExecutionGraph graph = new ExecutionGraph(Arrays.asList(
                new Stage(0, Collections.singletonList(0),
                        Collections.emptyList(), false, "source", 1),
                new Stage(1, Collections.singletonList(1),
                        Collections.singletonList(0), true, "sink")));
        PhysicalPlan physical = new PhysicalPlan(1, Arrays.asList(
                new PhysicalPlan.Instance(0, 0, "w0", false),
                new PhysicalPlan.Instance(1, 0, "w1", true)),
                Collections.singletonList(new int[]{0, 1}), false);
        Map<String, RegistryWorker> workers = new LinkedHashMap<>();
        workers.put("w0", worker("w0", 7600, (byte) 1));
        workers.put("w1", worker("w1", 7601, (byte) 2));
        ShuffleTransportPlan transport = ShuffleTransportPlan.create(
                graph, physical, workers, FENCE, "closure-job", true,
                seededRandom(11L));
        ShuffleGrantPlan grants = ShuffleGrantPlan.create(
                transport, seededRandom(12L));
        return new Fixture(graph, physical, transport, grants);
    }

    private static RegistryWorker worker(String id, int dataPort, byte fill) {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, fill);
        return new RegistryWorker(id, "127.0.0.1", dataPort - 100, 8,
                dataPort, "inc-" + id,
                RpcSecurityMetadata.of(hex(fill), hex((byte) (fill + 8))),
                DataPlaneRegistration.shuffleMtlsV1(fingerprint));
    }

    private static String hex(byte fill) {
        char digit = "0123456789abcdef".charAt(fill & 0x0f);
        char[] value = new char[64];
        Arrays.fill(value, digit);
        return new String(value);
    }

    private static SecureRandom seededRandom(long seed) {
        try {
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(seed);
            return random;
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class Fixture {
        final ExecutionGraph graph;
        final PhysicalPlan physical;
        final ShuffleTransportPlan transport;
        final ShuffleGrantPlan grants;

        Fixture(ExecutionGraph graph, PhysicalPlan physical,
                ShuffleTransportPlan transport, ShuffleGrantPlan grants) {
            this.graph = graph;
            this.physical = physical;
            this.transport = transport;
            this.grants = grants;
        }

        StageCommandShuffleClosure closure() {
            return StageCommandShuffleClosure.create(
                    graph, physical, transport, grants);
        }

        ShuffleEndpoint endpoint() {
            ShuffleTransportPlan.Route route = transport.routes().get(0);
            return grants.producerEndpoint(route.key());
        }

        StageCommand producer(ShuffleEndpoint endpoint) {
            PlanGraph fragment = new PlanGraph(Collections.singletonList(
                    PlanNode.placeholderSource(META)));
            return StageCommand.sourceStageFanout(
                    META, META, 1,
                    new CollectionSource(META, "closure-source", Collections.emptyList()),
                    fragment, Collections.emptyList(), null, false)
                    .withDownTargets(Collections.singletonList(
                            ShuffleTarget.socket(endpoint)))
                    .withObjectReuse(true);
        }

        StageCommand consumer() {
            PlanGraph fragment = new PlanGraph(Collections.singletonList(
                    PlanNode.placeholderSource(META)));
            io.rowweave.sched.net.ShuffleInboundAuthorization authorization =
                    grants.consumerAuthorization(1, 0, 0);
            return StageCommand.sink(META, 1, 1, fragment,
                            new DiscardSink(META))
                    .withInboundTargetId(authorization.targetId())
                    .withInboundAuthorizations(Collections.singletonList(authorization));
        }
    }
}
