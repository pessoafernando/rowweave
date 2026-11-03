package io.rowweave.monitor.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.MonitorLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The HTTP server behind the runtime monitoring page. It serves the dashboard together with the REST
 * and streaming endpoints, and renders from the snapshots held by {@link JobMonitorRegistry}. Netty
 * carries the transport: a boss group accepts, a worker group reads and writes, and the codec,
 * aggregator and {@link MonitorHttpHandler} route the request. {@link #start()} binds the configured
 * port, where zero means any free one, and returns the port actually taken; {@link #stop()} shuts
 * the server down.
 */
public final class MonitorHttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorHttpServer.class);

    private final JobMonitorRegistry registry;
    private final int port;
    private final MonitorLauncher.LeaderAddressResolver leaderResolver; // cluster standby redirect; null for single-machine
    private final int ioThreads;
    private final MonitorHttpSettings settings;
    private EventLoopGroup boss;
    private EventLoopGroup workers;
    private Channel channel;

    public MonitorHttpServer(JobMonitorRegistry registry, int port,
                             MonitorLauncher.LeaderAddressResolver leaderResolver) {
        this(registry, port, leaderResolver,
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
    }

    private MonitorHttpServer(JobMonitorRegistry registry, int port,
                              MonitorLauncher.LeaderAddressResolver leaderResolver,
                              int ioThreads, MonitorHttpSettings settings) {
        this.registry = registry;
        this.port = port;
        this.leaderResolver = leaderResolver;
        this.ioThreads = ioThreads;
        this.settings = settings;
    }

    public MonitorHttpServer(JobMonitorRegistry registry, int port,
                             MonitorLauncher.LeaderAddressResolver leaderResolver,
                             int ioThreads) {
        this(registry, port, leaderResolver, ioThreads, defaultSettings(registry));
    }

    private static MonitorHttpSettings defaultSettings(JobMonitorRegistry registry) {
        return new MonitorHttpSettings(
                System.getProperty("rowweave.monitor.bindAddress", "127.0.0.1"),
                registry.historyLimit(), configuredOrigins());
    }

    private static java.util.List<String> configuredOrigins() {
        String raw = System.getProperty("rowweave.monitor.allowedOrigins", "");
        java.util.List<String> origins = new java.util.ArrayList<>();
        for (String value : raw.split(",")) {
            String origin = value.trim();
            if (!origin.isEmpty()) origins.add(origin);
        }
        return origins;
    }

    InetSocketAddress localAddress() {
        return channel == null ? null : (InetSocketAddress) channel.localAddress();
    }

    /** Start server, returns the actual listening port (0=random assignment).  */
    public int start() {
        try {
            boss = new NioEventLoopGroup(1, namedFactory("rowweave-monitor-boss"));
            workers = new NioEventLoopGroup(ioThreads,
                    namedFactory("rowweave-monitor-io"));
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(1 << 20)) // 1MB request limit(monitor requests are small)
                                    .addLast(new MonitorHttpHandler(registry, leaderResolver, settings));
                        }
                    });
            java.net.InetAddress bindAddress;
            try {
                bindAddress = java.net.InetAddress.getByName(settings.bindAddress());
            } catch (java.net.UnknownHostException exception) {
                throw new RuntimeException("monitor bindAddress is invalid: "
                        + settings.bindAddress(), exception);
            }
            if (!bindAddress.isLoopbackAddress()
                    && !"0.0.0.0".equals(settings.bindAddress())) {
                throw new IllegalArgumentException(
                        "monitor bindAddress must be a loopback address or explicit 0.0.0.0");
            }
            channel = b.bind(new java.net.InetSocketAddress(bindAddress, port)).sync().channel();
            int actual = ((InetSocketAddress) channel.localAddress()).getPort();
            LOG.info("monitor HTTP started port={}", actual);
            return actual;
        } catch (Throwable failure) {
            stop();
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            if (failure instanceof Error) throw (Error) failure;
            throw new IllegalStateException("monitor HTTP could not start", failure);
        }
    }

    public void stop() {
        Channel currentChannel = channel;
        EventLoopGroup currentBoss = boss;
        EventLoopGroup currentWorkers = workers;
        channel = null;
        boss = null;
        workers = null;
        if (currentChannel != null) currentChannel.close();
        if (currentBoss != null) currentBoss.shutdownGracefully(0, 5,
                java.util.concurrent.TimeUnit.SECONDS);
        if (currentWorkers != null) currentWorkers.shutdownGracefully(0, 5,
                java.util.concurrent.TimeUnit.SECONDS);
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable,
                    prefix + '-' + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
