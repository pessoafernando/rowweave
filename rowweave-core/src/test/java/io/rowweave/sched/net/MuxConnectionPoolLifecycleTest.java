package io.rowweave.sched.net;

import io.netty.channel.nio.NioEventLoopGroup;
import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MuxConnectionPoolLifecycleTest {

    @Test
    void defaultGroupIsNotCreatedUntilPureDependencyValidationPasses() {
        AtomicInteger created = new AtomicInteger();

        assertThrows(NullPointerException.class, () ->
                MuxConnectionPool.createDefaultForTesting(null, null, null, null,
                        ControlResidentBudget.forTesting(1024 * 1024), () -> {
                            created.incrementAndGet();
                            return new NioEventLoopGroup(1);
                        }, () -> { }));

        assertEquals(0, created.get());
    }

    @Test
    void failureAfterDefaultGroupCreationShutsTheGroupAndPreservesPrimary() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1,
                new NamedDaemonThreadFactory("rowweave-mux-construction-test"));
        RuntimeException primary = new RuntimeException("injected post-group failure");
        DataPlaneConfig config = DataPlaneConfig.fromProperties(new Properties(),
                ControlSecurityConfig.fromProperties(new Properties()), 1024L * 1024L * 1024L);

        RuntimeException actual = assertThrows(RuntimeException.class, () ->
                MuxConnectionPool.createDefaultForTesting(config, null, null, null,
                        ControlResidentBudget.forTesting(1024 * 1024), () -> group,
                        () -> { throw primary; }));

        assertSame(primary, actual);
        assertTrue(group.terminationFuture().await(5, TimeUnit.SECONDS));
    }

    @Test
    void cohortCloseFailureDoesNotSkipEventLoopShutdown() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1,
                new NamedDaemonThreadFactory("rowweave-mux-strict-close-test"));
        RuntimeException primary = new RuntimeException("injected cohort close failure");
        MuxConnectionPool pool = new MuxConnectionPool(group, () -> { throw primary; });

        RuntimeException actual = assertThrows(RuntimeException.class, pool::close);

        assertSame(primary, actual);
        assertTrue(group.terminationFuture().await(5, TimeUnit.SECONDS));
        pool.close();
    }
}
