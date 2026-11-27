package com.example.grpc.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Starts/stops the gRPC server together with the Spring application context.
 */
@Component
public class GrpcServerLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

    private final PingPongServiceImpl pingPongService;
    private final int port;
    private final long shutdownGraceSeconds;

    private volatile Server server;

    public GrpcServerLifecycle(PingPongServiceImpl pingPongService,
                               @Value("${grpc.server.port:8080}") int port,
                               @Value("${grpc.server.shutdown-grace-seconds:5}") long shutdownGraceSeconds) {
        this.pingPongService = pingPongService;
        this.port = port;
        this.shutdownGraceSeconds = shutdownGraceSeconds;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void start() {
        try {
            server = ServerBuilder.forPort(port)
                    .addService(pingPongService)
                    .addService(ProtoReflectionService.newInstance())
                    .build()
                    .start();
            log.info("gRPC server started, listening on port {}", port);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start gRPC server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        Server current = server;
        if (current == null) {
            return;
        }
        log.info("Shutting down gRPC server...");
        current.shutdown();
        try {
            if (!current.awaitTermination(shutdownGraceSeconds, TimeUnit.SECONDS)) {
                current.shutdownNow();
            }
        } catch (InterruptedException e) {
            current.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("gRPC server stopped");
    }

    @Override
    public boolean isRunning() {
        Server current = server;
        return current != null && !current.isShutdown();
    }
}
