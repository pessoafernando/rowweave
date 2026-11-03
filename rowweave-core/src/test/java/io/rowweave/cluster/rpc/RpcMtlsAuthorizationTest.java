package io.rowweave.cluster.rpc;

import io.netty.handler.ssl.SslHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.sched.Channel;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.net.DataInboundRegistry;
import io.rowweave.sched.net.DataInboundAuthorizationRegistry;
import io.rowweave.sched.net.DataPlaneHandshakeAdmission;
import io.rowweave.sched.net.FrameCodec;
import io.rowweave.sched.net.MuxConnectionPool;
import io.rowweave.sched.net.ShuffleEndpoint;
import io.rowweave.sched.net.ShuffleAuthorizationRecord;
import io.rowweave.sched.net.ShuffleInboundAuthorization;
import io.rowweave.sched.net.ShuffleProducerGrant;
import io.rowweave.sched.net.WorkerDataIdentity;
import io.rowweave.sched.net.WorkerDataHello;
import io.rowweave.sched.net.WorkerDataServer;
import io.rowweave.sched.stream.StreamEvent;
import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.Provider;
import java.security.Security;
import java.util.Properties;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RpcMtlsAuthorizationTest {
    private static final String PASSWORD = "changeit";

    private Path temp;
    private Identity master;
    private Identity client;
    private Identity worker;
    private Identity wrongSanMaster;
    private Path commonTrust;
    private Path authz;

    @BeforeAll
    void createMaterial(@TempDir Path directory) throws Exception {
        temp = directory;
        master = identity("master", "SAN=ip:127.0.0.1,dns:localhost");
        client = identity("client", "SAN=ip:127.0.0.1,dns:localhost");
        worker = identity("worker", "SAN=ip:127.0.0.1,dns:localhost");
        wrongSanMaster = identity("wrong-san", "SAN=dns:not-local.invalid");
        commonTrust = temp.resolve("common-trust.p12");
        importCertificate(commonTrust, "master", master.certificate);
        importCertificate(commonTrust, "client", client.certificate);
        importCertificate(commonTrust, "worker", worker.certificate);
        importCertificate(commonTrust, "wrong-san", wrongSanMaster.certificate);
        authz = writeAuthz("roles.txt",
                master.fingerprint + "=MASTER\n"
                        + client.fingerprint + "=CLIENT\n"
                        + worker.fingerprint + "=WORKER\n"
                        + wrongSanMaster.fingerprint + "=MASTER\n");
    }

    @Test
    void authenticatedRequestContextIsServerOwnedNonWireIdentity() throws Exception {
        assertFalse(java.io.Serializable.class.isAssignableFrom(RpcRequestContext.class));
        assertFalse(java.io.Serializable.class.isAssignableFrom(AuthenticatedRpcPeer.class));
        assertTrue(Arrays.stream(RpcRequestContext.class.getDeclaredConstructors())
                .noneMatch(c -> Modifier.isPublic(c.getModifiers())));
        assertTrue(Arrays.stream(AuthenticatedRpcPeer.class.getDeclaredConstructors())
                .noneMatch(c -> Modifier.isPublic(c.getModifiers())));

        AtomicReference<RpcRequestContext> observed = new AtomicReference<>();
        DataPlaneRegistration dataIdentity = DataPlaneRegistration.shuffleMtlsV1(
                hexBytes(worker.fingerprint));
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                8, 8, 5_000L, 1024, 4096, 16_384);
        try (RpcServer server = new RpcServer((request, context) -> {
            observed.set(context);
            return RpcMessage.workerRegistered(request.incarnationId(), dataIdentity);
        }, 0, config(master, authz), admission, RpcRequestPolicy.MASTER,
                ControlResidentBudget.forTesting(64L * 1024L));
             RpcClient rpc = client(worker, authz, server.port())) {
            long before = System.nanoTime();
            RpcMessage response = rpc.call(RpcMessage.registerWorker(
                    "worker-context", "127.0.0.1", 62001, 1, 62002,
                    "inc-context", dataIdentity));
            long after = System.nanoTime();

            assertEquals(RpcMessage.Type.WORKER_REGISTERED, response.type());
            RpcRequestContext context = observed.get();
            assertTrue(context != null, "socket handler must receive a request context");
            assertTrue(context.requestDeadlineNanos() > before,
                    "deadline must be the frame-start absolute deadline");
            assertTrue(context.requestDeadlineNanos() > after,
                    "handler must inherit remaining frame deadline rather than reset it");
            assertArrayEquals(hexBytes(worker.fingerprint),
                    context.authenticatedPeer().certificateFingerprint());
            assertTrue(context.authenticatedPeer().hasRole(ControlRole.WORKER));
            assertFalse(context.authenticatedPeer().hasRole(ControlRole.MASTER));
        }
    }

    @Test
    void mtlsRegisterCapablePoliciesRejectContextInsensitiveHandlerBeforeBind() {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                8, 8, 5_000L, 1024, 4096, 16_384);
        for (RpcRequestPolicy policy : Arrays.asList(
                RpcRequestPolicy.MASTER, RpcRequestPolicy.GENERAL)) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> new RpcServer(request -> RpcMessage.ack(), 0,
                            config(master, authz), admission, policy,
                            ControlResidentBudget.forTesting(64L * 1024L)));
            assertTrue(failure.getMessage().contains("context-aware"),
                    failure.getMessage());
        }
    }

    @Test
    void masterRejectsInProcessOrMismatchedClaimBeforeWorkerMutation() {
        Properties properties = properties(master, authz, commonTrust);
        ControlSecurityConfig masterControl =
                ControlSecurityConfig.fromProperties(properties);
        try (io.rowweave.deployment.DeploymentRuntime runtime =
                     io.rowweave.deployment.DeploymentRuntime.load(
                             properties, masterControl, 1024L * 1024L * 1024L);
             io.rowweave.cluster.MasterServer masterServer =
                     new io.rowweave.cluster.MasterServer(
                             5_000L, 0,
                             new io.rowweave.cluster.registry.InProcessRegistry(),
                             io.rowweave.monitor.JobMonitorRegistry.NOOP,
                             null, runtime)) {
            DataPlaneRegistration workerIdentity =
                    DataPlaneRegistration.shuffleMtlsV1(hexBytes(worker.fingerprint));
            RpcMessage register = RpcMessage.registerWorker(
                    "authenticated-worker", "127.0.0.1", 62011, 2, 62012,
                    "inc-authenticated", workerIdentity);
            RpcMessage inProcess = masterServer.handler().apply(
                    RpcSecurityFence.outbound(register,
                            masterControl.securityMetadata(), true));
            assertEquals(RpcMessage.Type.JOB_RESULT, inProcess.type());
            assertFalse(inProcess.success());
            assertTrue(masterServer.worker("authenticated-worker") == null);

            try (RpcClient rpc = client(worker, authz, masterServer.port())) {
                RpcMessage mismatch = rpc.call(RpcMessage.registerWorker(
                        "authenticated-worker", "127.0.0.1", 62011, 2, 62012,
                        "inc-authenticated", DataPlaneRegistration.shuffleMtlsV1(
                                hexBytes(master.fingerprint))));
                assertEquals(RpcMessage.Type.JOB_RESULT, mismatch.type());
                assertFalse(mismatch.success());
                assertTrue(masterServer.worker("authenticated-worker") == null);

                RpcMessage accepted = rpc.call(register);
                assertEquals(RpcMessage.Type.WORKER_REGISTERED, accepted.type());
                assertTrue(masterServer.worker("authenticated-worker") != null);
            }
        }
    }

    @Test
    void registryCandidateNeedsLeaderLocalProofAndReplacementNeedsReregistration()
            throws Exception {
        Properties properties = properties(master, authz, commonTrust);
        ControlSecurityConfig masterControl =
                ControlSecurityConfig.fromProperties(properties);
        io.rowweave.cluster.registry.InProcessRegistry registry =
                new io.rowweave.cluster.registry.InProcessRegistry();
        DataPlaneRegistration workerIdentity =
                DataPlaneRegistration.shuffleMtlsV1(hexBytes(worker.fingerprint));
        io.rowweave.cluster.registry.RegistryWorker candidate =
                new io.rowweave.cluster.registry.RegistryWorker(
                        "leader-proof-worker", "127.0.0.1", 62021, 2, 62022,
                        "inc-proof", masterControl.securityMetadata(), workerIdentity);
        registry.register(candidate);

        try (io.rowweave.deployment.DeploymentRuntime runtime =
                     io.rowweave.deployment.DeploymentRuntime.load(
                             properties, masterControl, 1024L * 1024L * 1024L);
             io.rowweave.cluster.MasterServer masterServer =
                     new io.rowweave.cluster.MasterServer(
                             5_000L, 0, registry,
                             io.rowweave.monitor.JobMonitorRegistry.NOOP,
                             null, runtime)) {
            assertEquals(0, schedulableWorkerCount(masterServer),
                    "registry identity is only a candidate before this leader authenticates REGISTER");

            try (RpcClient rpc = client(worker, authz, masterServer.port())) {
                RpcMessage accepted = rpc.call(RpcMessage.registerWorker(
                        candidate.workerId(), candidate.host(), candidate.port(),
                        candidate.slots(), candidate.dataPort(), candidate.incarnationId(),
                        workerIdentity));
                assertEquals(RpcMessage.Type.WORKER_REGISTERED, accepted.type());
            }
            assertEquals(1, schedulableWorkerCount(masterServer));

            byte[] replacementFingerprint = hexBytes(worker.fingerprint);
            replacementFingerprint[0] ^= 0x01;
            registry.register(new io.rowweave.cluster.registry.RegistryWorker(
                    candidate.workerId(), candidate.host(), candidate.port(),
                    candidate.slots(), candidate.dataPort(), "inc-replacement",
                    masterControl.securityMetadata(),
                    DataPlaneRegistration.shuffleMtlsV1(replacementFingerprint)));
            assertEquals(0, schedulableWorkerCount(masterServer),
                    "replacement candidate cannot inherit the old incarnation proof");
        }
    }

    @Test
    void processTopologyRequiresEveryEnabledMtlsRoleBeforeRoleResources()
            throws Exception {
        ControlSecurityConfig masterOnly = config(master, authz);
        masterOnly.requireLocalRole(ControlRole.MASTER);
        assertThrows(IllegalStateException.class,
                () -> masterOnly.requireLocalRole(ControlRole.WORKER));

        ControlSecurityConfig workerOnly = config(worker, authz);
        workerOnly.requireLocalRole(ControlRole.WORKER);
        assertThrows(IllegalStateException.class,
                () -> workerOnly.requireLocalRole(ControlRole.MASTER));

        ControlSecurityConfig clientOnly = config(client, authz);
        assertThrows(IllegalStateException.class,
                () -> clientOnly.requireLocalRole(ControlRole.MASTER));
        assertThrows(IllegalStateException.class,
                () -> clientOnly.requireLocalRole(ControlRole.WORKER));

        Path combinedRoles = writeAuthz("combined-roles.txt",
                master.fingerprint + "=MASTER,WORKER\n");
        ControlSecurityConfig combined = config(master, combinedRoles);
        combined.requireLocalRole(ControlRole.MASTER);
        combined.requireLocalRole(ControlRole.WORKER);

        ControlSecurityConfig loopback = ControlSecurityConfig.fromProperties(
                new Properties());
        loopback.requireLocalRole(ControlRole.MASTER);
        loopback.requireLocalRole(ControlRole.WORKER);
    }

    @Test
    void controlOnlyMtlsRuntimeIgnoresWorkerDataPlaneConfiguration() {
        for (Identity identity : Arrays.asList(client, master)) {
            Properties properties = properties(identity, authz, commonTrust);
            properties.setProperty("rowweave.shuffle.mux", "false");
            properties.setProperty("rowweave.shuffle.maxConcurrentHandshakes", "999");
            ControlSecurityConfig control =
                    ControlSecurityConfig.fromProperties(properties);
            try (io.rowweave.deployment.DeploymentRuntime runtime =
                         io.rowweave.deployment.DeploymentRuntime.load(
                                 properties, control, 1024L * 1024L * 1024L)) {
                assertThrows(IllegalStateException.class,
                        runtime::requireDataPlaneConfig);
                assertThrows(IllegalStateException.class,
                        runtime::requireDataPlaneSecurityContext);
                assertThrows(IllegalStateException.class,
                        runtime::requireDataPlaneHandshakeAdmission);
            }
        }
    }

    @Test
    void publicRoleConstructorsCannotBypassLocalCertificateTopologyGate() {
        Properties workerProperties = properties(worker, authz, commonTrust);
        ControlSecurityConfig workerControl =
                ControlSecurityConfig.fromProperties(workerProperties);
        try (io.rowweave.deployment.DeploymentRuntime runtime =
                     io.rowweave.deployment.DeploymentRuntime.load(
                             workerProperties, workerControl,
                             1024L * 1024L * 1024L)) {
            IllegalStateException rejected = assertThrows(IllegalStateException.class,
                    () -> new io.rowweave.cluster.MasterServer(
                            5_000L, 0,
                            new io.rowweave.cluster.registry.InProcessRegistry(),
                            io.rowweave.monitor.JobMonitorRegistry.NOOP,
                            null, runtime));
            assertTrue(rejected.getMessage().contains("MASTER"),
                    rejected.getMessage());
        }

        Properties masterProperties = properties(master, authz, commonTrust);
        ControlSecurityConfig masterControl =
                ControlSecurityConfig.fromProperties(masterProperties);
        try (io.rowweave.deployment.DeploymentRuntime runtime =
                     io.rowweave.deployment.DeploymentRuntime.load(
                             masterProperties, masterControl,
                             1024L * 1024L * 1024L)) {
            IllegalStateException rejected = assertThrows(IllegalStateException.class,
                    () -> new io.rowweave.cluster.WorkerServer(
                            "wrong-role", 1,
                            new io.rowweave.cluster.FixedMasterLocator(
                                    new io.rowweave.cluster.Endpoint("127.0.0.1", 1)),
                            temp.resolve("wrong-role-worker").toFile(), runtime));
            assertTrue(rejected.getMessage().contains("WORKER"),
                    rejected.getMessage());
        }
    }

    @Test
    void clientCertificateMayCancelRunButWorkerCertificateMayNot() {
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, authz, request -> {
            handled.incrementAndGet();
            return RpcMessage.runCancellationResult(
                    new io.rowweave.run.RunCancellationResult(
                            request.runId(),
                            io.rowweave.run.RunCancellationResult.Disposition.NOT_FOUND,
                            null, 0L));
        }, RpcRequestPolicy.MASTER);
             RpcClient clientRpc = client(client, authz, server.port())) {
            RpcMessage result = clientRpc.call(
                    RpcMessage.cancelRun("run", "request"));
            assertEquals(RpcMessage.Type.RUN_CANCELLATION_RESULT, result.type());
            assertEquals("NOT_FOUND", result.runCancellationDisposition());
            assertEquals(1, handled.get());

            try (RpcClient workerRpc = client(worker, authz, server.port())) {
                assertThrows(RuntimeException.class, () -> workerRpc.call(
                        RpcMessage.cancelRun("run", "request")));
            }
            assertEquals(1, handled.get(),
                    "worker certificate must be rejected before handler");
        }
    }

    @Test
    void validClientRoleAndMasterRoleCompleteMutualTlsCall() {
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, authz, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.MASTER);
             RpcClient rpc = client(client, authz, server.port())) {
            assertEquals(RpcMessage.Type.ACK,
                    rpc.call(RpcMessage.submitRun(new byte[]{1})).type());
            assertEquals(1, handled.get());
            assertTrue(rpc.negotiatedTlsProtocol().equals("TLSv1.2")
                    || rpc.negotiatedTlsProtocol().equals("TLSv1.3"));
            assertTrue(rpc.negotiatedTlsCipherSuite().contains("GCM")
                    || rpc.negotiatedTlsCipherSuite().contains("CHACHA20"),
                    rpc.negotiatedTlsCipherSuite());
        }
    }

    @Test
    void dataPlaneContextCreatesOnlyConstrainedJdkEnginesAndRequiresWorkerIdentity()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(repeat('d', 64));
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        DataPlaneHandshakeAdmission.Lease inbound;
        DataPlaneHandshakeAdmission.Lease outbound;
        try {
            inbound = admission.acquireInbound(
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            outbound = admission.acquireOutbound(
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
        EmbeddedChannel serverTransport = new EmbeddedChannel();
        EmbeddedChannel clientTransport = new EmbeddedChannel();
        DataPlaneSecurityContext.AdmittedTlsChannel server =
                context.installServerHandler(serverTransport.pipeline(), inbound);
        DataPlaneSecurityContext.AdmittedTlsChannel client =
                context.installClientHandler(clientTransport.pipeline(),
                        "127.0.0.1", 19_000, outbound);
        assertEquals(io.netty.buffer.UnpooledByteBufAllocator.class,
                serverTransport.config().getAllocator().getClass());
        assertEquals(io.netty.buffer.UnpooledByteBufAllocator.class,
                clientTransport.config().getAllocator().getClass());

        assertFalse(server.clientMode());
        assertTrue(server.needClientAuth());
        assertTrue(client.clientMode());
        assertEquals("127.0.0.1", client.peerHost());
        assertEquals(19_000, client.peerPort());
        assertEquals("HTTPS", client.endpointIdentificationAlgorithm());
        assertEquals(10_000L, server.handshakeTimeoutMillis());
        assertEquals(10_000L, client.handshakeTimeoutMillis());
        assertTrue(context.localWorkerRole());
        assertEquals(workerConfig.securityMetadata().deploymentSetId(),
                context.deploymentSetId());
        assertEquals(workerConfig.securityMetadata().authzSetId(),
                context.authzSetId());
        assertOnlyTls13Aead(server);
        assertOnlyTls13Aead(client);
        assertTrue(server.engineImplementationName().startsWith("sun.security.ssl."));
        assertTrue(client.engineImplementationName().startsWith("sun.security.ssl."));
        for (java.lang.reflect.Method method
                : DataPlaneSecurityContext.AdmittedTlsChannel.class.getMethods()) {
            String signature = method.toGenericString();
            assertFalse(signature.contains("SslHandler"), signature);
            assertFalse(signature.contains("SSLEngine"), signature);
            assertFalse(method.getName().startsWith("set"), signature);
            assertFalse(method.getName().contains("renegotiate"), signature);
        }
        assertEquals("SunJSSE",
                workerConfig.tlsContext().sslContext().getProvider().getName());
        assertThrows(IllegalStateException.class, server::releaseAfterAuthentication);
        assertEquals(1, admission.snapshot().activeInbound());
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            channel.config().setAllocator(
                    io.netty.buffer.PooledByteBufAllocator.DEFAULT);
            context.configureChannel(channel.config());
            assertEquals(io.netty.buffer.UnpooledByteBufAllocator.class,
                    channel.config().getAllocator().getClass());
            assertFalse(channel.config().getAllocator().isDirectBufferPooled());
            io.netty.buffer.ByteBuf defaultBuffer = channel.alloc().buffer(1, 1);
            try {
                assertFalse(defaultBuffer.isDirect(),
                        "certified data-plane allocator must prefer heap buffers");
            } finally {
                defaultBuffer.release();
            }
            assertEquals(DataPlaneSecurityContext.TLS_READ_BYTES,
                    channel.config().getRecvByteBufAllocator().newHandle().guess());
            assertEquals(DataPlaneSecurityContext.WRITE_LOW_WATER_MARK_BYTES,
                    channel.config().getWriteBufferWaterMark().low());
            assertEquals(DataPlaneSecurityContext.WRITE_HIGH_WATER_MARK_BYTES,
                    channel.config().getWriteBufferWaterMark().high());
        } finally {
            channel.finishAndReleaseAll();
        }

        assertThrows(IllegalStateException.class, () -> new DataPlaneSecurityContext(
                config(master, authz).tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission));
        for (java.lang.reflect.Method method : DataPlaneSecurityContext.class.getMethods()) {
            if (method.getDeclaringClass() != DataPlaneSecurityContext.class) continue;
            String signature = method.toGenericString();
            assertFalse(signature.contains("SSLContext"), signature);
            assertFalse(signature.contains("KeyManager"), signature);
            assertFalse(signature.contains("TrustManager"), signature);
            assertFalse(signature.contains("PrivateKey"), signature);
            assertFalse(signature.contains("char[]"), signature);
        }
        serverTransport.finishAndReleaseAll();
        clientTransport.finishAndReleaseAll();
        assertEquals(0, admission.snapshot().activeTotal());
        admission.close();
    }

    @Test
    void productionMuxCompletesMutualTlsAndExactWorkerHelloBeforeOpen()
            throws Exception {
        String deploymentSet = repeat('d', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        Properties dataProperties = properties(worker, authz, commonTrust);
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                dataProperties, workerConfig, 4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity serverIdentity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        WorkerDataIdentity clientIdentity = WorkerDataIdentity.authenticated(
                "source-worker", "source-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        DataSchema meta = new DataSchema(new Field("id", DataType.IntegerType));
        java.util.UUID targetId = java.util.UUID.randomUUID();
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(8, 2, 1);
        registry.register(targetId, "hello-job", inbound, meta, 2, 1);
        RunAttemptFence fence = new RunAttemptFence(
                "hello-run", "hello-attempt", 1L);
        byte[] grantBytes = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(grantBytes, (byte) 91);
        ShuffleProducerGrant grant = ShuffleProducerGrant.fromBytes(grantBytes);
        byte[] secondGrantBytes = new byte[ShuffleProducerGrant.BYTES];
        Arrays.fill(secondGrantBytes, (byte) 93);
        ShuffleProducerGrant secondGrant = ShuffleProducerGrant.fromBytes(secondGrantBytes);
        byte[] cohortId = new byte[32];
        Arrays.fill(cohortId, (byte) 92);
        java.util.UUID cancelTargetId = java.util.UUID.randomUUID();
        ShuffleProducerGrant cancelFirstGrant =
                ShuffleProducerGrant.fromBytes(filledBytes((byte) 94));
        ShuffleProducerGrant cancelSecondGrant =
                ShuffleProducerGrant.fromBytes(filledBytes((byte) 95));
        byte[] cancelCohortId = filledBytes((byte) 96);
        MultiProducerChannel cancelInbound = new MultiProducerChannel(8, 2, 1);
        registry.register(cancelTargetId, "cancel-job", cancelInbound, meta, 2, 1);
        DataInboundAuthorizationRegistry authorizations =
                new DataInboundAuthorizationRegistry();

        try (DataInboundAuthorizationRegistry.PreparedHandle prepared = authorizations.prepare(
                     "hello-assignment", fence, java.util.Arrays.asList(
                             ShuffleInboundAuthorization.of(targetId, 1,
                                     java.util.Arrays.asList(
                                             ShuffleAuthorizationRecord.authorize(
                                                     fence, targetId, 1,
                                                     clientIdentity, grant),
                                             ShuffleAuthorizationRecord.authorize(
                                                     fence, targetId, 2,
                                                     clientIdentity, secondGrant))),
                             ShuffleInboundAuthorization.of(cancelTargetId, 1,
                                     java.util.Arrays.asList(
                                             ShuffleAuthorizationRecord.authorize(
                                                     fence, cancelTargetId, 1,
                                                     clientIdentity, cancelFirstGrant),
                                             ShuffleAuthorizationRecord.authorize(
                                                     fence, cancelTargetId, 2,
                                                     clientIdentity, cancelSecondGrant)))));
             WorkerDataServer server = new WorkerDataServer(
                     registry, 5_000L, 0, dataConfig, serverSecurity,
                     serverAdmission, serverIdentity, authorizations);
             MuxConnectionPool pool = new MuxConnectionPool(
                     dataConfig, clientSecurity, clientAdmission, clientIdentity)) {
            ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthorized(
                    "127.0.0.1", server.port(), targetId, serverIdentity,
                    grant, 1, cohortId, 2);
            ShuffleEndpoint secondEndpoint = ShuffleEndpoint.muxAuthorized(
                    "127.0.0.1", server.port(), targetId, serverIdentity,
                    secondGrant, 2, cohortId, 2);
            try (io.rowweave.sched.net.OutboundShuffleCohortRegistry.PreparedHandle ignored =
                         pool.prepareOutboundCohorts("hello-producer-assignment", fence,
                                 java.util.Arrays.asList(endpoint, secondEndpoint))) {
                Channel handle = pool.acquire(meta, endpoint, 4096, 0, 5_000);
                assertThrows(IllegalStateException.class,
                        () -> pool.acquire(meta, endpoint, 4096, 0, 5_000),
                        "one authorized route must not emit a replayed OPEN");
                assertEquals(0, serverAdmission.snapshot().activeTotal(),
                        "server TLS/hello admission must be released before OPEN is usable");
                assertEquals(0, clientAdmission.snapshot().activeTotal(),
                        "client TLS/hello admission must be released before OPEN is usable");

                DataRow row = new DataRow(1);
                row.set(0, 41);
                handle.send(StreamEvent.record(row));
                handle.close();
                assertEquals(41, inbound.poll().row().get(0));
                Channel secondHandle = pool.acquire(
                        meta, secondEndpoint, 1, 0, 5_000);
                DataRow secondRow = new DataRow(1);
                secondRow.set(0, 43);
                secondHandle.send(StreamEvent.record(secondRow));
                secondHandle.close();
                assertEquals(43, inbound.poll().row().get(0));
                assertEquals(null, inbound.poll());
                assertEquals(1, server.acceptedConnectionCount(),
                        "sequential ordinals in one cohort must retain one connection generation");
            }

            ShuffleEndpoint cancelFirst = ShuffleEndpoint.muxAuthorized(
                    "127.0.0.1", server.port(), cancelTargetId, serverIdentity,
                    cancelFirstGrant, 1, cancelCohortId, 2);
            ShuffleEndpoint cancelSecond = ShuffleEndpoint.muxAuthorized(
                    "127.0.0.1", server.port(), cancelTargetId, serverIdentity,
                    cancelSecondGrant, 2, cancelCohortId, 2);
            try (io.rowweave.sched.net.OutboundShuffleCohortRegistry.PreparedHandle ignored =
                         pool.prepareOutboundCohorts("cancel-producer-assignment", fence,
                                 java.util.Arrays.asList(cancelFirst, cancelSecond))) {
                Channel firstHandle = pool.acquire(meta, cancelFirst, 1, 0, 5_000);
                Channel blockedHandle = pool.acquire(meta, cancelSecond, 1, 0, 5_000);
                firstHandle.send(StreamEvent.record(rowWithValue(47)));
                AtomicReference<Throwable> blockedFailure = new AtomicReference<>();
                CountDownLatch sendEntered = new CountDownLatch(1);
                Thread blocked = new Thread(() -> {
                    sendEntered.countDown();
                    try {
                        blockedHandle.send(StreamEvent.record(rowWithValue(49)));
                    } catch (Throwable failure) {
                        blockedFailure.set(failure);
                    }
                }, "cancelled-outbound-credit-waiter");
                blocked.start();
                assertTrue(sendEntered.await(1, TimeUnit.SECONDS));
                Thread.sleep(30L);
                assertTrue(blocked.isAlive(),
                        "second ordinal must be waiting on the shared one-byte pool");

                pool.cancelOutboundAttempt(fence);
                blocked.join(1_000L);
                assertFalse(blocked.isAlive(),
                        "attempt cancellation must wake a blocked credit waiter");
                assertTrue(blockedFailure.get() instanceof ChannelPoisonedException,
                        "blocked sender must fail closed after attempt cancellation");
                firstHandle.fail(new IllegalStateException("test cancellation"));
                blockedHandle.fail(new IllegalStateException("test cancellation"));
            }
        } finally {
            authorizations.close();
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    private static byte[] filledBytes(byte fill) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, fill);
        return bytes;
    }

    private static DataRow rowWithValue(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return row;
    }

    @Test
    void productionMuxRejectsWrongDestinationGenerationBeforePoolOrTargetCreation()
            throws Exception {
        String deploymentSet = repeat('e', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                properties(worker, authz, commonTrust), workerConfig,
                4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity actual = WorkerDataIdentity.authenticated(
                "destination-worker", "incarnation-current",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        WorkerDataIdentity stale = WorkerDataIdentity.authenticated(
                "destination-worker", "incarnation-stale",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        DataInboundRegistry registry = new DataInboundRegistry();
        DataSchema meta = new DataSchema(new Field("id", DataType.IntegerType));

        try (WorkerDataServer server = new WorkerDataServer(
                     registry, 5_000L, 0, dataConfig, serverSecurity,
                     serverAdmission, actual);
             MuxConnectionPool pool = new MuxConnectionPool(
                     dataConfig, clientSecurity, clientAdmission,
                     WorkerDataIdentity.authenticated(
                             "source-worker", "source-incarnation",
                             DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                             deploymentSet, workerConfig.securityMetadata().authzSetId(),
                             fingerprint))) {
            ShuffleEndpoint staleEndpoint = ShuffleEndpoint.muxAuthorized(
                    "127.0.0.1", server.port(), java.util.UUID.randomUUID(), stale,
                    ShuffleProducerGrant.fromBytes(new byte[ShuffleProducerGrant.BYTES]),
                    1, new byte[32], 1);

            try (io.rowweave.sched.net.OutboundShuffleCohortRegistry.PreparedHandle ignored =
                         pool.prepareOutboundCohorts("stale-producer-assignment",
                                 new RunAttemptFence("stale-run", "stale-attempt", 1L),
                                 java.util.Collections.singletonList(staleEndpoint))) {
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> pool.acquire(meta, staleEndpoint, 4096, 0, 5_000));
                assertTrue(failure.getMessage().contains("failed to connect mux shuffle"));
                assertEquals(0, pool.connectionCount(),
                        "rejected hello must never publish a physical connection to the pool");
                awaitNoActiveHandshakes(serverAdmission, clientAdmission);
            }
        } finally {
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    @Test
    void productionMtlsDataPortRejectsPlainFrameBeforeDemuxResourcesExist()
            throws Exception {
        String deploymentSet = repeat('f', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                properties(worker, authz, commonTrust), workerConfig,
                4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext security = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), admission);
        WorkerDataIdentity identity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(),
                hexBytes(worker.fingerprint));
        DataInboundRegistry registry = new DataInboundRegistry();

        try (WorkerDataServer server = new WorkerDataServer(
                     registry, 5_000L, 0, dataConfig, security, admission, identity);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3_000);
            java.io.DataOutputStream output =
                    new java.io.DataOutputStream(socket.getOutputStream());
            output.writeByte(2); // legacy FRAME_RECORD prefix, deliberately not a TLS record
            output.writeLong(0L);
            output.writeLong(7L);
            output.writeInt(0);
            output.flush();
            try {
                assertEquals(-1, socket.getInputStream().read());
            } catch (IOException expectedClose) {
                // TLS alert/reset is also a fail-closed transport outcome.
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while (admission.snapshot().activeTotal() != 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            assertEquals(0, admission.snapshot().activeTotal());
            assertEquals(0, workerDataPeakTargets(server),
                    "plaintext must be rejected before DataDemuxHandler sees a target");
        } finally {
            admission.close();
        }
    }

    @Test
    void malformedAndSilentTlsHelloCloseWithinOneDeadlineAndReleaseBothDirections()
            throws Exception {
        String deploymentSet = repeat('a', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        Properties dataProperties = properties(worker, authz, commonTrust);
        dataProperties.setProperty("rowweave.shuffle.handshakeTimeoutMs", "1500");
        dataProperties.setProperty("rowweave.shuffle.connectTimeoutMs", "1500");
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                dataProperties, workerConfig, 4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity serverIdentity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        EventLoopGroup clientIo = new NioEventLoopGroup(1);

        try (WorkerDataServer server = new WorkerDataServer(
                new DataInboundRegistry(), 5_000L, 0, dataConfig, serverSecurity,
                serverAdmission, serverIdentity)) {
            io.netty.channel.Channel malformed = connectTlsOnly(
                    clientIo, clientSecurity, clientAdmission,
                    server.port(), fingerprint, dataConfig.handshakeTimeoutMillis());
            byte[] invalidHello = new byte[WorkerDataHello.MAX_FRAME_BYTES];
            malformed.writeAndFlush(Unpooled.wrappedBuffer(invalidHello)).sync();
            assertTrue(malformed.closeFuture().await(3, TimeUnit.SECONDS),
                    "malformed hello must close without extending the deadline");

            io.netty.channel.Channel silent = connectTlsOnly(
                    clientIo, clientSecurity, clientAdmission,
                    server.port(), fingerprint, dataConfig.handshakeTimeoutMillis());
            assertTrue(silent.closeFuture().await(3, TimeUnit.SECONDS),
                    "silent hello must close at the original connection deadline");
            awaitNoActiveHandshakes(serverAdmission, clientAdmission);
            assertEquals(0, workerDataPeakTargets(server));
        } finally {
            clientIo.shutdownGracefully().sync();
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    @Test
    @Timeout(20)
    void helloAndMaximumFramePrefixInOneTlsWriteAcquireIngressOwnershipWithoutGap()
            throws Exception {
        String deploymentSet = repeat('b', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        Properties dataProperties = properties(worker, authz, commonTrust);
        long ingressBytes = 4L * 64L * 1024L
                + FrameCodec.V1_HEADER_BYTES + (long) FrameCodec.MAX_FRAME_BYTES + 128L;
        dataProperties.setProperty("rowweave.shuffle.maxConnections", "4");
        dataProperties.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(ingressBytes));
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                dataProperties, workerConfig, 4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity serverIdentity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        WorkerDataIdentity sourceIdentity = WorkerDataIdentity.authenticated(
                "source-worker", "source-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        EventLoopGroup clientIo = new NioEventLoopGroup(1);
        long frameCharge = FrameCodec.V1_HEADER_BYTES
                + (long) FrameCodec.MAX_FRAME_BYTES + 128L;

        try (WorkerDataServer server = new WorkerDataServer(
                     new DataInboundRegistry(), 5_000L, 0, dataConfig, serverSecurity,
                     serverAdmission, serverIdentity);
             RawTlsConnection client = connectRawTls(
                     clientIo, clientSecurity, clientAdmission, server.port(),
                     fingerprint, dataConfig.handshakeTimeoutMillis())) {
            byte[] hello = WorkerDataHello.encode(sourceIdentity);
            java.util.UUID target = java.util.UUID.randomUUID();
            ByteBuf plaintext = Unpooled.buffer(
                            hello.length + FrameCodec.V1_HEADER_BYTES + 17)
                    .writeBytes(hello)
                    .writeByte(FrameCodec.FRAME_RECORD)
                    .writeLong(target.getMostSignificantBits())
                    .writeLong(target.getLeastSignificantBits())
                    .writeInt(1)
                    .writeInt(FrameCodec.MAX_FRAME_BYTES)
                    .writeZero(17);
            client.channel.writeAndFlush(plaintext).sync();
            client.tls.releaseAfterAuthentication();

            awaitWorkerDataState(server, 1, 64L * 1024L + frameCharge);
            assertEquals(0, serverAdmission.snapshot().activeTotal(),
                    "server hello lease must transfer to scratch/frame ownership");
            assertEquals(0, clientAdmission.snapshot().activeTotal());
            assertTrue(workerDataPeakResident(server) <= ingressBytes);
            client.close();
            awaitWorkerDataState(server, 0, 0L);
        } finally {
            clientIo.shutdownGracefully().sync();
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    @Test
    @Timeout(30)
    void authenticatedConnectionsSendingMaximumHeadersAreBoundedByIngressBudget()
            throws Exception {
        String deploymentSet = repeat('c', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        Properties dataProperties = properties(worker, authz, commonTrust);
        long scratch = 64L * 1024L;
        long frameCharge = FrameCodec.V1_HEADER_BYTES
                + (long) FrameCodec.MAX_FRAME_BYTES + 128L;
        long ingressBytes = 4L * scratch + frameCharge;
        dataProperties.setProperty("rowweave.shuffle.maxConnections", "4");
        dataProperties.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(ingressBytes));
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                dataProperties, workerConfig, 4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity serverIdentity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        WorkerDataIdentity sourceIdentity = WorkerDataIdentity.authenticated(
                "source-worker", "source-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        EventLoopGroup clientIo = new NioEventLoopGroup(2);
        java.util.List<RawTlsConnection> clients = new java.util.ArrayList<>();

        try (WorkerDataServer server = new WorkerDataServer(
                new DataInboundRegistry(), 5_000L, 0, dataConfig, serverSecurity,
                serverAdmission, serverIdentity)) {
            for (int index = 0; index < 4; index++) {
                RawTlsConnection connection = connectRawTls(
                        clientIo, clientSecurity, clientAdmission, server.port(),
                        fingerprint, dataConfig.handshakeTimeoutMillis());
                clients.add(connection);
                connection.channel.writeAndFlush(
                        Unpooled.wrappedBuffer(WorkerDataHello.encode(sourceIdentity))).sync();
                connection.tls.releaseAfterAuthentication();
            }
            awaitWorkerDataState(server, 4, 4L * scratch);

            java.util.UUID target = java.util.UUID.randomUUID();
            for (RawTlsConnection connection : clients) {
                ByteBuf header = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                        .writeByte(FrameCodec.FRAME_RECORD)
                        .writeLong(target.getMostSignificantBits())
                        .writeLong(target.getLeastSignificantBits())
                        .writeInt(1)
                        .writeInt(FrameCodec.MAX_FRAME_BYTES);
                connection.channel.writeAndFlush(header);
            }
            awaitWorkerDataState(server, 1, scratch + frameCharge);
            assertEquals(ingressBytes, workerDataPeakResident(server),
                    "four scratch leases plus exactly one maximum frame must be the peak");
            assertTrue(workerDataPeakResident(server) <= ingressBytes);
            assertEquals(0, serverAdmission.snapshot().activeTotal());
            assertEquals(0, clientAdmission.snapshot().activeTotal());
            for (RawTlsConnection connection : clients) connection.close();
            clients.clear();
            awaitWorkerDataState(server, 0, 0L);
        } finally {
            for (RawTlsConnection connection : clients) connection.close();
            clientIo.shutdownGracefully().sync();
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    @Test
    @Timeout(20)
    void authenticatedPartialBodyIdleAndMalformedHeaderReleaseAllIngressOwnership()
            throws Exception {
        String deploymentSet = repeat('9', 64);
        ControlSecurityConfig workerConfig = config(worker, authz)
                .withDeploymentSetId(deploymentSet);
        Properties dataProperties = properties(worker, authz, commonTrust);
        long scratch = 64L * 1024L;
        long frameCharge = FrameCodec.V1_HEADER_BYTES
                + (long) FrameCodec.MAX_FRAME_BYTES + 128L;
        dataProperties.setProperty("rowweave.shuffle.maxConnections", "4");
        dataProperties.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(4L * scratch + frameCharge));
        dataProperties.setProperty("rowweave.shuffle.idleReadTimeoutMs", "1000");
        DataPlaneConfig dataConfig = DataPlaneConfig.fromProperties(
                dataProperties, workerConfig, 4L * 1024L * 1024L * 1024L);
        DataPlaneHandshakeAdmission serverAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission clientAdmission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext serverSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), serverAdmission);
        DataPlaneSecurityContext clientSecurity = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                dataConfig.handshakeTimeoutMillis(), clientAdmission);
        byte[] fingerprint = hexBytes(worker.fingerprint);
        WorkerDataIdentity serverIdentity = WorkerDataIdentity.authenticated(
                "destination-worker", "destination-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        WorkerDataIdentity sourceIdentity = WorkerDataIdentity.authenticated(
                "source-worker", "source-incarnation",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                deploymentSet, workerConfig.securityMetadata().authzSetId(), fingerprint);
        EventLoopGroup clientIo = new NioEventLoopGroup(1);

        try (WorkerDataServer server = new WorkerDataServer(
                new DataInboundRegistry(), 5_000L, 0, dataConfig, serverSecurity,
                serverAdmission, serverIdentity)) {
            try (RawTlsConnection idle = connectRawTls(
                    clientIo, clientSecurity, clientAdmission, server.port(),
                    fingerprint, dataConfig.handshakeTimeoutMillis())) {
                byte[] hello = WorkerDataHello.encode(sourceIdentity);
                java.util.UUID target = java.util.UUID.randomUUID();
                idle.channel.writeAndFlush(Unpooled.buffer(
                                hello.length + FrameCodec.V1_HEADER_BYTES + 17)
                        .writeBytes(hello)
                        .writeByte(FrameCodec.FRAME_RECORD)
                        .writeLong(target.getMostSignificantBits())
                        .writeLong(target.getLeastSignificantBits())
                        .writeInt(1)
                        .writeInt(FrameCodec.MAX_FRAME_BYTES)
                        .writeZero(17)).sync();
                idle.tls.releaseAfterAuthentication();
                awaitWorkerDataState(server, 1, scratch + frameCharge);
                assertTrue(idle.channel.closeFuture().await(4, TimeUnit.SECONDS),
                        "partial V1 body must close at the configured read-idle deadline");
                awaitWorkerDataState(server, 0, 0L);
            }

            try (RawTlsConnection malformed = connectRawTls(
                    clientIo, clientSecurity, clientAdmission, server.port(),
                    fingerprint, dataConfig.handshakeTimeoutMillis())) {
                byte[] hello = WorkerDataHello.encode(sourceIdentity);
                java.util.UUID target = java.util.UUID.randomUUID();
                malformed.channel.writeAndFlush(Unpooled.buffer(
                                hello.length + FrameCodec.V1_HEADER_BYTES)
                        .writeBytes(hello)
                        .writeByte(127)
                        .writeLong(target.getMostSignificantBits())
                        .writeLong(target.getLeastSignificantBits())
                        .writeInt(1)
                        .writeInt(0)).sync();
                malformed.tls.releaseAfterAuthentication();
                assertTrue(malformed.channel.closeFuture().await(3, TimeUnit.SECONDS),
                        "invalid authenticated V1 header must fail closed");
                awaitWorkerDataState(server, 0, 0L);
            }
            assertEquals(0, serverAdmission.snapshot().activeTotal());
            assertEquals(0, clientAdmission.snapshot().activeTotal());
        } finally {
            clientIo.shutdownGracefully().sync();
            serverAdmission.close();
            clientAdmission.close();
        }
    }

    private static void awaitNoActiveHandshakes(
            DataPlaneHandshakeAdmission first,
            DataPlaneHandshakeAdmission second) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while ((first.snapshot().activeTotal() != 0
                || second.snapshot().activeTotal() != 0)
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(0, first.snapshot().activeTotal());
        assertEquals(0, second.snapshot().activeTotal());
    }

    private static int workerDataPeakTargets(WorkerDataServer server) throws Exception {
        java.lang.reflect.Method snapshotMethod = WorkerDataServer.class
                .getDeclaredMethod("admissionSnapshot");
        snapshotMethod.setAccessible(true);
        Object snapshot = snapshotMethod.invoke(server);
        java.lang.reflect.Method peakTargets = snapshot.getClass()
                .getDeclaredMethod("peakTargetsPerConnection");
        peakTargets.setAccessible(true);
        return (Integer) peakTargets.invoke(snapshot);
    }

    private static long workerDataPeakResident(WorkerDataServer server) throws Exception {
        Object snapshot = workerDataSnapshot(server);
        java.lang.reflect.Method peak = snapshot.getClass()
                .getDeclaredMethod("peakResidentBytes");
        peak.setAccessible(true);
        return (Long) peak.invoke(snapshot);
    }

    private static void awaitWorkerDataState(
            WorkerDataServer server, int connections, long residentBytes) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            Object snapshot = workerDataSnapshot(server);
            java.lang.reflect.Method active = snapshot.getClass()
                    .getDeclaredMethod("activeConnections");
            java.lang.reflect.Method resident = snapshot.getClass()
                    .getDeclaredMethod("residentBytes");
            active.setAccessible(true);
            resident.setAccessible(true);
            if ((Integer) active.invoke(snapshot) == connections
                    && (Long) resident.invoke(snapshot) == residentBytes) return;
            Thread.sleep(10L);
        }
        throw new AssertionError("Worker data state did not converge: "
                + workerDataSnapshot(server));
    }

    private static Object workerDataSnapshot(WorkerDataServer server) throws Exception {
        java.lang.reflect.Method snapshot = WorkerDataServer.class
                .getDeclaredMethod("admissionSnapshot");
        snapshot.setAccessible(true);
        return snapshot.invoke(server);
    }

    private static RawTlsConnection connectRawTls(
            EventLoopGroup group, DataPlaneSecurityContext security,
            DataPlaneHandshakeAdmission admission, int port,
            byte[] expectedFingerprint, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireOutbound(deadline);
        AtomicReference<DataPlaneSecurityContext.AdmittedTlsChannel> installed =
                new AtomicReference<>();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                        @Override
                        protected void initChannel(
                                io.netty.channel.socket.SocketChannel channel) {
                            installed.set(security.installClientHandler(
                                    channel.pipeline(), "127.0.0.1", port,
                                    lease, deadline));
                        }
                    });
            ChannelFuture connected = bootstrap.connect("127.0.0.1", port).sync();
            DataPlaneSecurityContext.AdmittedTlsChannel tls = installed.get();
            tls.awaitHandshakeSuccess();
            security.requireWorkerPeer(tls, expectedFingerprint);
            return new RawTlsConnection(connected.channel(), tls);
        } finally {
            lease.close();
        }
    }

    private static io.netty.channel.Channel connectTlsOnly(
            EventLoopGroup group, DataPlaneSecurityContext security,
            DataPlaneHandshakeAdmission admission, int port,
            byte[] expectedFingerprint, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireOutbound(deadline);
        AtomicReference<DataPlaneSecurityContext.AdmittedTlsChannel> installed =
                new AtomicReference<>();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                        @Override
                        protected void initChannel(
                                io.netty.channel.socket.SocketChannel channel) {
                            installed.set(security.installClientHandler(
                                    channel.pipeline(), "127.0.0.1", port,
                                    lease, deadline));
                        }
                    });
            ChannelFuture connected = bootstrap.connect("127.0.0.1", port).sync();
            DataPlaneSecurityContext.AdmittedTlsChannel tls = installed.get();
            tls.awaitHandshakeSuccess();
            security.requireWorkerPeer(tls, expectedFingerprint);
            return connected.channel();
        } finally {
            lease.close();
        }
    }

    @Test
    void dataPlaneHandlerOwnsLeaseAcrossRemovalAndChannelClose() throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        EmbeddedChannel channel = new EmbeddedChannel();
        context.installServerHandler(channel.pipeline(), lease);
        assertEquals(1, admission.snapshot().activeInbound());

        lease.close();
        assertEquals(1, admission.snapshot().activeInbound(),
                "caller lease must not release ownership after handler claim");

        try {
            channel.pipeline().remove(channel.pipeline().get(SslHandler.class));
            channel.runPendingTasks();
            assertFalse(channel.isOpen());
            assertEquals(0, admission.snapshot().activeInbound());
        } finally {
            channel.finishAndReleaseAll();
            admission.close();
        }
    }

    @Test
    @Timeout(10)
    void allocatorDriftKeepsAdmissionUntilTlsPipelineRemovalCompletes()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                2, DataPlaneHandshakeAdmission.requiredResidentBytes(2));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        EmbeddedChannel channel = new EmbeddedChannel();
        context.installServerHandler(channel.pipeline(), lease);
        CountDownLatch removalStarted = new CountDownLatch(1);
        CountDownLatch allowRemoval = new CountDownLatch(1);
        channel.pipeline().addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
            @Override
            public void handlerRemoved(io.netty.channel.ChannelHandlerContext ignored)
                    throws Exception {
                removalStarted.countDown();
                if (!allowRemoval.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test removal barrier timed out");
                }
            }
        });
        channel.config().setAllocator(io.netty.buffer.PooledByteBufAllocator.DEFAULT);
        ExecutorService trigger = Executors.newSingleThreadExecutor();
        try {
            Future<?> rejected = trigger.submit(() -> {
                try {
                    channel.writeInbound(io.netty.buffer.Unpooled.wrappedBuffer(
                            new byte[]{22, 3, 3, 0, 0}));
                } finally {
                    channel.runPendingTasks();
                }
                channel.checkException();
            });
            assertTrue(removalStarted.await(2, TimeUnit.SECONDS));
            assertEquals(1, admission.snapshot().activeInbound(),
                    "allocator drift must not release before TLS handler removal");
            allowRemoval.countDown();
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> rejected.get(2, TimeUnit.SECONDS));
            assertEquals(0, admission.snapshot().activeInbound());
        } finally {
            allowRemoval.countDown();
            trigger.shutdownNow();
            trigger.awaitTermination(2, TimeUnit.SECONDS);
            channel.finishAndReleaseAll();
            admission.close();
        }
    }

    @Test
    @Timeout(15)
    void concurrentTlsRemovalCannotDeliverPlaintextThroughUnlinkWindow()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                2, DataPlaneHandshakeAdmission.requiredResidentBytes(2));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        io.netty.channel.EventLoopGroup boss =
                new io.netty.channel.nio.NioEventLoopGroup(1);
        io.netty.channel.EventLoopGroup workers =
                new io.netty.channel.nio.NioEventLoopGroup(1);
        CountDownLatch installed = new CountDownLatch(1);
        CountDownLatch inboundBlocked = new CountDownLatch(1);
        CountDownLatch releaseInbound = new CountDownLatch(1);
        AtomicReference<io.netty.channel.Channel> accepted = new AtomicReference<>();
        AtomicReference<SslHandler> tlsHandler = new AtomicReference<>();
        AtomicInteger plaintextRows = new AtomicInteger();
        io.netty.channel.Channel listener = null;
        Socket rawClient = null;
        try {
            listener = new io.netty.bootstrap.ServerBootstrap()
                    .group(boss, workers)
                    .channel(io.netty.channel.socket.nio.NioServerSocketChannel.class)
                    .childHandler(new io.netty.channel.ChannelInitializer<
                            io.netty.channel.socket.SocketChannel>() {
                        @Override
                        protected void initChannel(
                                io.netty.channel.socket.SocketChannel channel)
                                throws Exception {
                            accepted.set(channel);
                            channel.pipeline().addLast(
                                    new io.netty.channel.ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(
                                        io.netty.channel.ChannelHandlerContext handlerContext,
                                        Object message) throws Exception {
                                    inboundBlocked.countDown();
                                    if (!releaseInbound.await(5, TimeUnit.SECONDS)) {
                                        io.netty.util.ReferenceCountUtil.release(message);
                                        throw new IllegalStateException(
                                                "TLS unlink test barrier timed out");
                                    }
                                    handlerContext.fireChannelRead(message);
                                }
                            });
                            DataPlaneHandshakeAdmission.Lease lease =
                                    admission.acquireInbound(
                                            System.nanoTime()
                                                    + TimeUnit.SECONDS.toNanos(5));
                            context.installServerHandler(channel.pipeline(), lease);
                            tlsHandler.set(channel.pipeline().get(SslHandler.class));
                            channel.pipeline().addLast(
                                    new io.netty.channel.ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(
                                        io.netty.channel.ChannelHandlerContext handlerContext,
                                        Object message) {
                                    plaintextRows.incrementAndGet();
                                    io.netty.util.ReferenceCountUtil.release(message);
                                }
                            });
                            installed.countDown();
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();
            rawClient = new Socket();
            rawClient.connect(listener.localAddress());
            assertTrue(installed.await(5, TimeUnit.SECONDS));
            rawClient.getOutputStream().write(new byte[]{22, 3, 3, 0, 0});
            rawClient.getOutputStream().flush();
            assertTrue(inboundBlocked.await(5, TimeUnit.SECONDS));

            io.netty.channel.Channel child = accepted.get();
            SslHandler removed = tlsHandler.get();
            child.pipeline().remove(removed);
            assertTrue(child.pipeline().context(removed) == null);
            assertEquals(1, admission.snapshot().activeInbound(),
                    "unlink must not release before TLS handler cleanup");
            releaseInbound.countDown();
            assertTrue(child.closeFuture().await(5, TimeUnit.SECONDS));
            long idleBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (admission.snapshot().activeTotal() != 0
                    && System.nanoTime() < idleBy) {
                Thread.yield();
            }
            assertEquals(0, admission.snapshot().activeTotal());
            assertEquals(0, plaintextRows.get(),
                    "TLS presence guard must discard unlink-window plaintext");
        } finally {
            releaseInbound.countDown();
            if (rawClient != null) rawClient.close();
            io.netty.channel.Channel child = accepted.get();
            if (child != null) child.close().syncUninterruptibly();
            if (listener != null) listener.close().syncUninterruptibly();
            workers.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            boss.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            admission.close();
        }
    }

    @Test
    @Timeout(15)
    void concurrentTlsRemovalCannotWritePlaintextThroughUnlinkWindow()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                2, DataPlaneHandshakeAdmission.requiredResidentBytes(2));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        io.netty.channel.EventLoopGroup boss =
                new io.netty.channel.nio.NioEventLoopGroup(1);
        io.netty.channel.EventLoopGroup workers =
                new io.netty.channel.nio.NioEventLoopGroup(1);
        CountDownLatch installed = new CountDownLatch(1);
        CountDownLatch outboundBlocked = new CountDownLatch(1);
        CountDownLatch releaseOutbound = new CountDownLatch(1);
        AtomicReference<io.netty.channel.Channel> accepted = new AtomicReference<>();
        AtomicReference<SslHandler> tlsHandler = new AtomicReference<>();
        AtomicInteger plaintextWrites = new AtomicInteger();
        io.netty.channel.Channel listener = null;
        Socket rawClient = null;
        try {
            listener = new io.netty.bootstrap.ServerBootstrap()
                    .group(boss, workers)
                    .channel(io.netty.channel.socket.nio.NioServerSocketChannel.class)
                    .childHandler(new io.netty.channel.ChannelInitializer<
                            io.netty.channel.socket.SocketChannel>() {
                        @Override
                        protected void initChannel(
                                io.netty.channel.socket.SocketChannel channel)
                                throws Exception {
                            accepted.set(channel);
                            channel.pipeline().addLast(
                                    new io.netty.channel.ChannelOutboundHandlerAdapter() {
                                @Override
                                public void write(
                                        io.netty.channel.ChannelHandlerContext handlerContext,
                                        Object message,
                                        io.netty.channel.ChannelPromise promise)
                                        throws Exception {
                                    plaintextWrites.incrementAndGet();
                                    handlerContext.write(message, promise);
                                }
                            });
                            DataPlaneHandshakeAdmission.Lease lease =
                                    admission.acquireInbound(
                                            System.nanoTime()
                                                    + TimeUnit.SECONDS.toNanos(5));
                            context.installServerHandler(channel.pipeline(), lease);
                            tlsHandler.set(channel.pipeline().get(SslHandler.class));
                            channel.pipeline().addLast(
                                    new io.netty.channel.ChannelOutboundHandlerAdapter() {
                                @Override
                                public void write(
                                        io.netty.channel.ChannelHandlerContext handlerContext,
                                        Object message,
                                        io.netty.channel.ChannelPromise promise)
                                        throws Exception {
                                    outboundBlocked.countDown();
                                    if (!releaseOutbound.await(5, TimeUnit.SECONDS)) {
                                        io.netty.util.ReferenceCountUtil.release(message);
                                        promise.tryFailure(new IllegalStateException(
                                                "TLS outbound unlink barrier timed out"));
                                        return;
                                    }
                                    handlerContext.write(message, promise);
                                }
                            });
                            installed.countDown();
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();
            rawClient = new Socket();
            rawClient.connect(listener.localAddress());
            assertTrue(installed.await(5, TimeUnit.SECONDS));

            io.netty.channel.Channel child = accepted.get();
            io.netty.channel.ChannelFuture write = child.writeAndFlush(
                    io.netty.buffer.Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
            assertTrue(outboundBlocked.await(5, TimeUnit.SECONDS));
            SslHandler removed = tlsHandler.get();
            child.pipeline().remove(removed);
            assertTrue(child.pipeline().context(removed) == null);
            assertEquals(1, admission.snapshot().activeInbound(),
                    "unlink must retain admission until TLS handler cleanup");
            releaseOutbound.countDown();
            assertTrue(write.await(5, TimeUnit.SECONDS));
            assertFalse(write.isSuccess());
            assertTrue(child.closeFuture().await(5, TimeUnit.SECONDS));
            long idleBy = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (admission.snapshot().activeTotal() != 0
                    && System.nanoTime() < idleBy) {
                Thread.yield();
            }
            assertEquals(0, admission.snapshot().activeTotal());
            assertEquals(0, plaintextWrites.get(),
                    "TLS presence guard must reject unlink-window plaintext writes");
        } finally {
            releaseOutbound.countDown();
            if (rawClient != null) rawClient.close();
            io.netty.channel.Channel child = accepted.get();
            if (child != null) child.close().syncUninterruptibly();
            if (listener != null) listener.close().syncUninterruptibly();
            workers.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            boss.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            admission.close();
        }
    }

    @Test
    void failedAtomicInstallReturnsClaimedLease() throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        assertThrows(IllegalArgumentException.class,
                () -> context.installServerHandler(null, lease));
        assertEquals(0, admission.snapshot().activeInbound());
        admission.close();
    }

    @Test
    void dataPlaneHandlerClosesChannelBeforeExceptionCanReleaseAdmission()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        EmbeddedChannel channel = new EmbeddedChannel();
        context.installServerHandler(channel.pipeline(), lease);
        try {
            IOException propagated = assertThrows(IOException.class, () -> {
                channel.pipeline().fireExceptionCaught(
                        new IOException("recoverable upstream"));
                channel.runPendingTasks();
                channel.checkException();
            });
            assertEquals("recoverable upstream", propagated.getMessage());
            assertFalse(channel.isOpen());
            assertEquals(0, admission.snapshot().activeInbound());
        } finally {
            channel.finishAndReleaseAll();
            admission.close();
        }
    }

    @Test
    void forgedNettyHandshakePromiseCannotReleaseOpaqueAdmission() throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        EmbeddedChannel channel = new EmbeddedChannel();
        DataPlaneSecurityContext.AdmittedTlsChannel admitted =
                context.installServerHandler(channel.pipeline(), lease);
        SslHandler raw = channel.pipeline().get(SslHandler.class);
        assertThrows(ClassCastException.class, () -> {
            io.netty.util.concurrent.Promise<?> ignored =
                    (io.netty.util.concurrent.Promise<?>) raw.handshakeFuture();
        });
        assertFalse(raw.handshakeFuture().cancel(false));
        assertThrows(IllegalStateException.class,
                admitted::releaseAfterAuthentication);
        assertEquals(1, admission.snapshot().activeInbound());
        channel.finishAndReleaseAll();
        assertEquals(0, admission.snapshot().activeInbound());
        admission.close();
    }

    @Test
    void hostileRenegotiationPromiseCannotPreventTerminalClose() throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission.Lease lease = admission.acquireInbound(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        EmbeddedChannel channel = new EmbeddedChannel();
        context.installServerHandler(channel.pipeline(), lease);
        SslHandler raw = channel.pipeline().get(SslHandler.class);
        io.netty.util.concurrent.Promise<io.netty.channel.Channel> hostile =
                new io.netty.util.concurrent.DefaultPromise<io.netty.channel.Channel>(
                        io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE) {
                    @Override
                    public boolean tryFailure(Throwable cause) {
                        throw new AssertionError("hostile promise callback");
                    }
                };
        assertFalse(raw.renegotiate(hostile).isSuccess());
        channel.runPendingTasks();
        assertFalse(channel.isOpen());
        assertEquals(0, admission.snapshot().activeInbound());
        channel.finishAndReleaseAll();
        admission.close();
    }

    @Test
    void controlTlsContextPinsSunJsseDespiteHigherPriorityTlsProvider() throws Exception {
        Provider hijack = new HijackTlsProvider();
        assertEquals(1, Security.insertProviderAt(hijack, 1));
        try {
            assertEquals("RowWeaveHijack",
                    javax.net.ssl.SSLContext.getInstance("TLS").getProvider().getName());
            assertEquals("RowWeaveHijack",
                    javax.net.ssl.KeyManagerFactory.getInstance("SunX509")
                            .getProvider().getName());
            assertEquals("RowWeaveHijack",
                    javax.net.ssl.TrustManagerFactory.getInstance("PKIX")
                            .getProvider().getName());
            ControlSecurityConfig configured = config(worker, authz);
            assertEquals("SunJSSE",
                    configured.tlsContext().sslContext().getProvider().getName());
        } finally {
            Security.removeProvider(hijack.getName());
        }
    }

    @Test
    void dataPlaneHandlerRequiresOneOpenDirectionMatchedLeaseFromItsAdmission()
            throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission owner = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneHandshakeAdmission foreign = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, owner);
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        EmbeddedChannel outboundChannel = new EmbeddedChannel();
        try (DataPlaneHandshakeAdmission.Lease wrongDirection = owner.acquireOutbound(
                     System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
             DataPlaneHandshakeAdmission.Lease foreignInbound = foreign.acquireInbound(
                     System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
             DataPlaneHandshakeAdmission.Lease reusable = owner.acquireInbound(
                     System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
             DataPlaneHandshakeAdmission.Lease alreadyClosed = owner.acquireInbound(
                     System.nanoTime() + TimeUnit.SECONDS.toNanos(1))) {
            assertThrows(IllegalArgumentException.class,
                    () -> context.installServerHandler(
                            inboundChannel.pipeline(), wrongDirection));
            assertThrows(IllegalArgumentException.class,
                    () -> context.installServerHandler(
                            inboundChannel.pipeline(), foreignInbound));
            alreadyClosed.close();
            assertThrows(RejectedExecutionException.class,
                    () -> context.installServerHandler(
                            inboundChannel.pipeline(), alreadyClosed));
            context.installServerHandler(inboundChannel.pipeline(), reusable);
            assertThrows(RejectedExecutionException.class,
                    () -> context.installServerHandler(
                            inboundChannel.pipeline(), reusable));
            context.installClientHandler(outboundChannel.pipeline(),
                    "127.0.0.1", 19_000, wrongDirection);
        } finally {
            inboundChannel.finishAndReleaseAll();
            outboundChannel.finishAndReleaseAll();
            foreign.close();
            owner.close();
        }
        assertEquals(DataPlaneHandshakeAdmission.Lease.class,
                DataPlaneSecurityContext.class.getMethod(
                        "installServerHandler", io.netty.channel.ChannelPipeline.class,
                        DataPlaneHandshakeAdmission.Lease.class)
                        .getParameterTypes()[1]);
    }

    @Test
    void dataPlanePeerMustHaveWorkerRoleAndExactRegisteredFingerprint() throws Exception {
        ControlSecurityConfig workerConfig = config(worker, authz);
        DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                4, handshakeResidentBytes());
        DataPlaneSecurityContext context = new DataPlaneSecurityContext(
                workerConfig.tlsContext(), workerConfig.securityMetadata(),
                10_000L, admission);
        byte[] expected = context.localCertificateFingerprint();
        ExecutorService acceptor = Executors.newSingleThreadExecutor();
        try (javax.net.ssl.SSLServerSocket listener =
                     workerConfig.tlsContext().newServerSocket()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            Future<?> accepted = acceptor.submit(() -> {
                try (javax.net.ssl.SSLSocket socket =
                             (javax.net.ssl.SSLSocket) listener.accept()) {
                    socket.startHandshake();
                    context.requireWorkerPeer(socket.getSession(), expected);
                    return null;
                }
            });
            try (javax.net.ssl.SSLSocket socket = workerConfig.tlsContext().newClientSocket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", listener.getLocalPort()));
                socket.startHandshake();
                context.requireWorkerPeer(socket.getSession(), expected);
                byte[] wrong = expected.clone();
                wrong[0] ^= 1;
                IOException mismatch = assertThrows(IOException.class,
                        () -> context.requireWorkerPeer(socket.getSession(), wrong));
                assertTrue(mismatch.getMessage().contains("fingerprint mismatch"));
            }
            accepted.get(5, TimeUnit.SECONDS);
        } finally {
            acceptor.shutdownNow();
            assertTrue(acceptor.awaitTermination(5, TimeUnit.SECONDS));
        }

        ExecutorService rejectingAcceptor = Executors.newSingleThreadExecutor();
        try (javax.net.ssl.SSLServerSocket listener =
                     workerConfig.tlsContext().newServerSocket()) {
            listener.bind(new InetSocketAddress("127.0.0.1", 0));
            Future<String> rejected = rejectingAcceptor.submit(() -> {
                try (javax.net.ssl.SSLSocket socket =
                             (javax.net.ssl.SSLSocket) listener.accept()) {
                    socket.startHandshake();
                    try {
                        context.requireWorkerPeer(
                                socket.getSession(), hexBytes(client.fingerprint));
                        return "accepted";
                    } catch (IOException expectedFailure) {
                        return expectedFailure.getMessage();
                    }
                }
            });
            ControlSecurityConfig clientConfig = config(client, authz);
            try (javax.net.ssl.SSLSocket socket =
                         clientConfig.tlsContext().newClientSocket()) {
                socket.connect(new InetSocketAddress(
                        "127.0.0.1", listener.getLocalPort()));
                socket.startHandshake();
            }
            assertTrue(rejected.get(5, TimeUnit.SECONDS).contains("WORKER role"));
        } finally {
            rejectingAcceptor.shutdownNow();
            assertTrue(rejectingAcceptor.awaitTermination(5, TimeUnit.SECONDS));
            admission.close();
        }
    }

    @Test
    void deploymentSetIdentityIsVerifiedAfterTlsAndBeforeHandler() {
        String setA = repeat('a', 64);
        String setB = repeat('b', 64);
        AtomicInteger handled = new AtomicInteger();
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                8, 8, 5_000L, 1024, 4096, 16_384);
        ControlSecurityConfig serverConfig = config(master, authz).withDeploymentSetId(setA);
        try (RpcServer server = new RpcServer((request, context) -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, 0, serverConfig, admission, RpcRequestPolicy.MASTER,
                ControlResidentBudget.forTesting(64L * 1024L));
             RpcClient matching = new RpcClient("127.0.0.1", server.port(), 2_000, 2_000,
                     config(client, authz).withDeploymentSetId(setA),
                     ControlResidentBudget.forTesting(64L * 1024L))) {
            assertEquals(RpcMessage.Type.ACK,
                    matching.call(RpcMessage.submitRun(new byte[]{1})).type());
            assertEquals(1, handled.get());

            try (RpcClient mismatched = new RpcClient(
                    "127.0.0.1", server.port(), 2_000, 2_000,
                    config(client, authz).withDeploymentSetId(setB),
                    ControlResidentBudget.forTesting(64L * 1024L))) {
                assertThrows(RuntimeException.class,
                        () -> mismatched.call(RpcMessage.submitRun(new byte[]{1})));
            }
            assertEquals(1, handled.get(), "set mismatch must stop before handler");
        }
    }

    @Test
    void wrongServerRoleIsRejectedByClientBeforeRequestWrite() throws Exception {
        Path wrongRoles = writeAuthz("wrong-server-role.txt",
                master.fingerprint + "=WORKER\n" + client.fingerprint + "=CLIENT\n");
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, wrongRoles, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.MASTER);
             RpcClient rpc = client(client, wrongRoles, server.port())) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> rpc.call(RpcMessage.submitRun(new byte[]{1})));
            assertTrue(hasCauseMessage(failure, "required MASTER role"), failure.toString());
            assertEquals(0, handled.get());
        }
    }

    @Test
    void wrongClientRequestRoleIsRejectedAtHeaderBeforeHandler() throws Exception {
        Path workerRoles = writeAuthz("worker-role.txt",
                master.fingerprint + "=WORKER\n" + client.fingerprint + "=CLIENT\n");
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, workerRoles, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.WORKER);
             RpcClient rpc = client(client, workerRoles, server.port())) {
            assertThrows(RuntimeException.class,
                    () -> rpc.call(RpcMessage.assignStage(
                            "worker", 1L, new byte[]{1}, "incarnation")));
            assertEquals(0, handled.get());
            assertEquals(0L, server.inFlightFrameBytes(),
                    "body frame budget must remain untouched");
        }
    }

    @Test
    void wrongSanFailsHandshakeBeforeHandler() {
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(wrongSanMaster, authz, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.MASTER)) {
            assertThrows(RuntimeException.class,
                    () -> client(client, authz, server.port()));
            assertEquals(0, handled.get());
        }
    }

    @Test
    void wrongTrustFailsHandshakeBeforeHandler() throws Exception {
        Path clientOnlyTrust = temp.resolve("client-only-trust.p12");
        importCertificate(clientOnlyTrust, "client", client.certificate);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, authz, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.MASTER)) {
            assertThrows(RuntimeException.class,
                    () -> client(client, authz, server.port(), clientOnlyTrust));
            assertEquals(0, handled.get());
        }
    }

    @Test
    void masterCertificateWithoutServerAuthEkuFailsAtConfigLoad() throws Exception {
        Identity clientOnly = identity("client-only-master",
                "SAN=ip:127.0.0.1,dns:localhost", "EKU=clientAuth");
        Path roles = writeAuthz("wrong-eku.txt",
                clientOnly.fingerprint + "=MASTER\n" + client.fingerprint + "=CLIENT\n");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> config(clientOnly, roles));
        assertTrue(failure.getMessage().contains("EKU"), failure.getMessage());
    }

    @Test
    void privateKeyAndLeafCertificateMismatchFailsAtConfigLoad() throws Exception {
        java.security.KeyStore masterKeys = java.security.KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(master.keyStore)) {
            masterKeys.load(in, PASSWORD.toCharArray());
        }
        java.security.Key privateKey = masterKeys.getKey("identity", PASSWORD.toCharArray());
        java.security.cert.Certificate clientLeaf;
        try (InputStream in = Files.newInputStream(client.certificate)) {
            clientLeaf = CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
        Path mismatchStore = temp.resolve("mismatched-key.p12");
        java.security.KeyStore mismatch = java.security.KeyStore.getInstance("PKCS12");
        mismatch.load(null, PASSWORD.toCharArray());
        mismatch.setKeyEntry("identity", privateKey, PASSWORD.toCharArray(),
                new java.security.cert.Certificate[]{clientLeaf});
        try (java.io.OutputStream out = Files.newOutputStream(mismatchStore)) {
            mismatch.store(out, PASSWORD.toCharArray());
        }
        Identity mismatched = new Identity(
                mismatchStore, client.certificate, client.fingerprint);
        Path roles = writeAuthz("mismatched-key.txt", client.fingerprint + "=MASTER\n");

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> config(mismatched, roles));
        assertTrue(failure.getMessage().contains("private key"), failure.getMessage());
    }

    @Test
    void missingClientCertificateFailsMutualTlsHandshakeBeforeHandler() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = server(master, authz, request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, RpcRequestPolicy.MASTER)) {
            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                    javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            java.security.KeyStore trust = java.security.KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(commonTrust)) {
                trust.load(in, PASSWORD.toCharArray());
            }
            tmf.init(trust);
            javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            try (javax.net.ssl.SSLSocket socket = (javax.net.ssl.SSLSocket)
                    context.getSocketFactory().createSocket("127.0.0.1", server.port())) {
                javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.setSoTimeout(2_000);
                assertThrows(java.io.IOException.class, () -> {
                    socket.startHandshake();
                    socket.getOutputStream().write(RpcCodec.encode(
                            RpcMessage.submitRun(new byte[]{1})));
                    socket.getOutputStream().flush();
                    if (socket.getInputStream().read() < 0) {
                        throw new java.io.EOFException("mTLS server rejected certificate-less peer");
                    }
                });
            }
            assertEquals(0, handled.get());
        }
    }

    private RpcServer server(Identity identity, Path policy,
                             java.util.function.Function<RpcMessage, RpcMessage> handler,
                             RpcRequestPolicy requestPolicy) {
        RpcAdmissionConfig admission = RpcAdmissionConfig.forTesting(
                8, 8, 5_000L, 1024, 4096, 16_384);
        return new RpcServer((request, context) -> handler.apply(request),
                0, config(identity, policy), admission,
                requestPolicy, ControlResidentBudget.forTesting(64L * 1024L));
    }

    private static int schedulableWorkerCount(
            io.rowweave.cluster.MasterServer masterServer) throws Exception {
        java.lang.reflect.Method method = io.rowweave.cluster.MasterServer.class
                .getDeclaredMethod("schedulableWorkers");
        method.setAccessible(true);
        return ((java.util.List<?>) method.invoke(masterServer)).size();
    }

    private static void assertOnlyTls13Aead(
            DataPlaneSecurityContext.AdmittedTlsChannel handler) {
        assertEquals(java.util.Collections.singletonList("TLSv1.3"),
                Arrays.asList(handler.enabledProtocols()));
        assertTrue(Arrays.stream(handler.enabledCipherSuites())
                .allMatch(cipher -> cipher.contains("GCM")
                        || cipher.contains("CHACHA20")));
    }

    private static byte[] hexBytes(String hex) {
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) ((Character.digit(hex.charAt(i * 2), 16) << 4)
                    | Character.digit(hex.charAt(i * 2 + 1), 16));
        }
        return result;
    }

    private RpcClient client(Identity identity, Path policy, int port) {
        return client(identity, policy, port, commonTrust);
    }

    private RpcClient client(Identity identity, Path policy, int port, Path trustStore) {
        return new RpcClient("127.0.0.1", port, 2_000, 2_000,
                config(identity, policy, trustStore),
                ControlResidentBudget.forTesting(64L * 1024L));
    }

    private ControlSecurityConfig config(Identity identity, Path policy) {
        return config(identity, policy, commonTrust);
    }

    private ControlSecurityConfig config(Identity identity, Path policy, Path trustStore) {
        return ControlSecurityConfig.fromProperties(
                properties(identity, policy, trustStore));
    }

    private Properties properties(Identity identity, Path policy, Path trustStore) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY, "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty(ControlTlsContext.KEY_STORE_PROPERTY, identity.keyStore.toString());
        properties.setProperty(ControlTlsContext.KEY_STORE_PASSWORD_PROPERTY, PASSWORD);
        properties.setProperty(ControlTlsContext.TRUST_STORE_PROPERTY, trustStore.toString());
        properties.setProperty(ControlTlsContext.TRUST_STORE_PASSWORD_PROPERTY, PASSWORD);
        properties.setProperty(ControlTlsContext.AUTHZ_FILE_PROPERTY, policy.toString());
        return properties;
    }

    private Identity identity(String name, String san) throws Exception {
        return identity(name, san, "EKU=serverAuth,clientAuth");
    }

    private Identity identity(String name, String san, String eku) throws Exception {
        Path keyStore = temp.resolve(name + ".p12");
        Path certificate = temp.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", san, "-ext", eku);
        keytool("-exportcert", "-alias", "identity", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream in = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(in);
        }
        return new Identity(keyStore, certificate, ControlTlsContext.fingerprint(leaf));
    }

    private void importCertificate(Path trustStore, String alias, Path certificate)
            throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    private Path writeAuthz(String name, String contents) throws Exception {
        Path path = temp.resolve(name);
        Files.write(path, contents.getBytes(StandardCharsets.US_ASCII));
        return path;
    }

    private static void keytool(String... arguments) throws Exception {
        java.util.List<String> command =
                io.rowweave.test.ForkedJvm.keytoolCommand(arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) bytes.write(buffer, 0, read);
        return bytes.toByteArray();
    }

    private static boolean hasCauseMessage(Throwable failure, String text) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(text)) return true;
        }
        return false;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class Identity {
        private final Path keyStore;
        private final Path certificate;
        private final String fingerprint;

        private Identity(Path keyStore, Path certificate, String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }

    private static final class RawTlsConnection implements AutoCloseable {
        private final io.netty.channel.Channel channel;
        private final DataPlaneSecurityContext.AdmittedTlsChannel tls;

        private RawTlsConnection(io.netty.channel.Channel channel,
                                 DataPlaneSecurityContext.AdmittedTlsChannel tls) {
            this.channel = channel;
            this.tls = tls;
        }

        @Override
        public void close() {
            channel.close().syncUninterruptibly();
        }
    }

    public static final class HijackTlsProvider extends Provider {
        public HijackTlsProvider() {
            super("RowWeaveHijack", 1.0, "test-only higher-priority TLS provider");
            put("SSLContext.TLS", HijackTlsContextSpi.class.getName());
            put("KeyManagerFactory.SunX509", HijackKeyManagerFactorySpi.class.getName());
            put("TrustManagerFactory.PKIX", HijackTrustManagerFactorySpi.class.getName());
        }
    }

    private static long handshakeResidentBytes() {
        return DataPlaneHandshakeAdmission.requiredResidentBytes(4);
    }

    public static final class HijackTlsContextSpi extends javax.net.ssl.SSLContextSpi {
        public HijackTlsContextSpi() {
        }

        @Override
        protected void engineInit(javax.net.ssl.KeyManager[] keyManagers,
                                  javax.net.ssl.TrustManager[] trustManagers,
                                  java.security.SecureRandom random) {
        }

        @Override
        protected javax.net.ssl.SSLSocketFactory engineGetSocketFactory() {
            return null;
        }

        @Override
        protected javax.net.ssl.SSLServerSocketFactory engineGetServerSocketFactory() {
            return null;
        }

        @Override
        protected javax.net.ssl.SSLEngine engineCreateSSLEngine() {
            return null;
        }

        @Override
        protected javax.net.ssl.SSLEngine engineCreateSSLEngine(String host, int port) {
            return null;
        }

        @Override
        protected javax.net.ssl.SSLSessionContext engineGetServerSessionContext() {
            return null;
        }

        @Override
        protected javax.net.ssl.SSLSessionContext engineGetClientSessionContext() {
            return null;
        }
    }

    public static final class HijackKeyManagerFactorySpi
            extends javax.net.ssl.KeyManagerFactorySpi {
        public HijackKeyManagerFactorySpi() {
        }

        @Override
        protected void engineInit(java.security.KeyStore store, char[] password) {
        }

        @Override
        protected void engineInit(javax.net.ssl.ManagerFactoryParameters parameters) {
        }

        @Override
        protected javax.net.ssl.KeyManager[] engineGetKeyManagers() {
            return new javax.net.ssl.KeyManager[0];
        }
    }

    public static final class HijackTrustManagerFactorySpi
            extends javax.net.ssl.TrustManagerFactorySpi {
        public HijackTrustManagerFactorySpi() {
        }

        @Override
        protected void engineInit(java.security.KeyStore store) {
        }

        @Override
        protected void engineInit(javax.net.ssl.ManagerFactoryParameters parameters) {
        }

        @Override
        protected javax.net.ssl.TrustManager[] engineGetTrustManagers() {
            return new javax.net.ssl.TrustManager[0];
        }
    }
}

