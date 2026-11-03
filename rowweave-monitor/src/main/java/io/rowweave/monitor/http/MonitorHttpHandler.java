package io.rowweave.monitor.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.MonitorLauncher;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/** Read-only monitor API with loopback-first transport and explicit cross-origin opt-in. */
final class MonitorHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final String CONTENT_TYPE_PROMETHEUS =
            "text/plain; version=0.0.4; charset=utf-8";

    private final JobMonitorRegistry registry;
    private final MonitorLauncher.LeaderAddressResolver leaderResolver;
    private final MonitorHttpSettings settings;

    MonitorHttpHandler(JobMonitorRegistry registry,
                       MonitorLauncher.LeaderAddressResolver leaderResolver,
                       MonitorHttpSettings settings) {
        this.registry = registry;
        this.leaderResolver = leaderResolver;
        this.settings = settings;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) {
        Exchange exchange = new Exchange(context, request);
        if (!"GET".equals(request.method().name())) {
            exchange.methodNotAllowed();
            return;
        }
        if (leaderResolver != null && !leaderResolver.isLeader()) {
            String leaderHost = leaderResolver.leaderHost();
            if (leaderHost == null) {
                exchange.text(HttpResponseStatus.SERVICE_UNAVAILABLE,
                        "text/plain; charset=UTF-8", "no active leader");
            } else {
                exchange.redirect("http://" + leaderHost + exchange.path);
            }
            return;
        }
        route(exchange);
    }

    private void route(Exchange exchange) {
        String path = exchange.path;
        if ("/".equals(path) || "/index.html".equals(path)) {
            exchange.text(HttpResponseStatus.OK, "text/html; charset=UTF-8", loadPage());
        } else if ("/api/overview".equals(path)) {
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.overview(registry.jobs(), registry.workers()));
        } else if ("/api/jobs".equals(path)) {
            exchange.json(HttpResponseStatus.OK, MonitorJson.jobs(registry.jobs()));
        } else if (path.startsWith("/api/jobs/")) {
            handleJobPath(exchange, path);
        } else if ("/api/workers".equals(path)) {
            exchange.json(HttpResponseStatus.OK, MonitorJson.workers(registry.workers()));
        } else if ("/api/metrics".equals(path)) {
            exchange.text(HttpResponseStatus.OK, CONTENT_TYPE_PROMETHEUS,
                    MonitorPrometheus.render(registry.metrics()));
        } else if ("/api/config".equals(path)) {
            int port = exchange.port();
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.config(exchange.runtimeSettings.forListenerPort(port), port));
        } else if ("/api/health".equals(path)) {
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.health(registry.jobs(), registry.workers()));
        } else if ("/api/retention".equals(path)) {
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.retention(registry.runRetention()));
        } else if ("/api/artifact-retention".equals(path)) {
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.artifactRetention(registry.artifactRetention()));
        } else if ("/api/retry-recovery".equals(path)) {
            exchange.json(HttpResponseStatus.OK,
                    MonitorJson.retryRecovery(registry.retryRecovery()));
        } else {
            exchange.json(HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
        }
    }

    private void handleJobPath(Exchange exchange, String path) {
        String encoded = path.substring("/api/jobs/".length());
        int suffix = encoded.indexOf('/');
        if (suffix < 0) {
            JobView job = registry.job(exchange.decode(encoded));
            if (job == null) {
                exchange.json(HttpResponseStatus.NOT_FOUND, "{\"error\":\"job not found\"}");
            } else {
                exchange.json(HttpResponseStatus.OK, MonitorJson.jobDetail(job));
            }
            return;
        }
        JobView job = registry.job(exchange.decode(encoded.substring(0, suffix)));
        if (job == null || !"exceptions".equals(encoded.substring(suffix + 1))) {
            exchange.json(HttpResponseStatus.NOT_FOUND, "{\"error\":\"not found\"}");
        } else {
            exchange.json(HttpResponseStatus.OK, MonitorJson.exceptions(registry.jobs()));
        }
    }

    private String loadPage() {
        try (InputStream input = getClass()
                .getResourceAsStream("/io/rowweave/monitor/web/index.html")) {
            if (input == null) {
                return "<!doctype html><html><body><h1>RowWeave Monitor</h1>"
                        + "<p>page missing</p></body></html>";
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) != -1) output.write(buffer, 0, read);
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return "<!doctype html><html><body><h1>RowWeave Monitor</h1></body></html>";
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        context.close();
    }

    private final class Exchange {
        private final ChannelHandlerContext context;
        private final FullHttpRequest request;
        private final String path;
        private final MonitorHttpSettings runtimeSettings;

        private Exchange(ChannelHandlerContext context, FullHttpRequest request) {
            this.context = context;
            this.request = request;
            this.path = new QueryStringDecoder(request.uri()).path();
            this.runtimeSettings = settings.forListenerPort(port());
        }

        private int port() {
            InetSocketAddress address = (InetSocketAddress) context.channel().localAddress();
            return address.getPort();
        }

        private String decode(String value) {
            try {
                return java.net.URLDecoder.decode(value, "UTF-8");
            } catch (Exception ignored) {
                return value;
            }
        }

        private void methodNotAllowed() {
            byte[] bytes = "{\"error\":\"method not allowed\"}".getBytes(StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED,
                    Unpooled.wrappedBuffer(bytes));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                    "application/json; charset=UTF-8");
            response.headers().set(HttpHeaderNames.ALLOW, "GET");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
            write(response);
        }

        private void json(HttpResponseStatus status, String body) {
            text(status, "application/json; charset=UTF-8", body);
        }

        private void text(HttpResponseStatus status, String contentType, String body) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
            response.headers().set(HttpHeaderNames.CACHE_CONTROL,
                    "no-cache, no-store, must-revalidate");
            String requestedOrigin = request.headers().get(HttpHeaderNames.ORIGIN);
            if (requestedOrigin != null
                    && runtimeSettings.allowedOrigins().contains(requestedOrigin)) {
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, requestedOrigin);
            }
            write(response);
        }

        private void redirect(String location) {
            FullHttpResponse response = empty(HttpResponseStatus.FOUND, null);
            response.headers().set(HttpHeaderNames.LOCATION, location);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            write(response);
        }

        private FullHttpResponse empty(HttpResponseStatus status, String contentType) {
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
            if (contentType != null) {
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            }
            HttpUtil.setContentLength(response, 0);
            return response;
        }

        private void write(FullHttpResponse response) {
            context.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
