package com.payflow.eligibility;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs a plain grpc-java server inside the Spring context: every BindableService
 * bean is registered, plus the standard health and reflection services (so
 * grpcurl and k8s gRPC probes work).
 */
@Component
public class GrpcServerLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

    private final List<BindableService> services;
    private final int port;
    private final HealthStatusManager health = new HealthStatusManager();
    private volatile Server server;

    public GrpcServerLifecycle(List<BindableService> services, @Value("${payflow.grpc.port:9091}") int port) {
        this.services = services;
        this.port = port;
    }

    @Override
    public void start() {
        NettyServerBuilder b = NettyServerBuilder.forPort(port)
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance());
        services.forEach(b::addService);
        try {
            server = b.build().start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        health.setStatus("", ServingStatus.SERVING);
        log.info("gRPC listening on {} with {} service(s)", server.getPort(), services.size());
        // grpc's netty threads are daemons; keep the JVM up while the server runs.
        Thread keepAlive = new Thread(() -> {
            try {
                server.awaitTermination();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "grpc-await");
        keepAlive.setDaemon(false);
        keepAlive.start();
    }

    @Override
    public void stop() {
        health.enterTerminalState(); // NOT_SERVING first, so balancers drain us
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) server.shutdownNow();
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return server != null && !server.isShutdown();
    }

    public int getPort() {
        return server.getPort();
    }
}
