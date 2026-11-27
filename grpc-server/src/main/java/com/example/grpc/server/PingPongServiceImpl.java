package com.example.grpc.server;

import com.example.grpc.pingpong.v1.PingPongServiceGrpc;
import com.example.grpc.pingpong.v1.PingRequest;
import com.example.grpc.pingpong.v1.PongResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class PingPongServiceImpl extends PingPongServiceGrpc.PingPongServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(PingPongServiceImpl.class);

    private final String serverId;
    private final AtomicLong requestCounter = new AtomicLong();

    public PingPongServiceImpl(@Value("${spring.application.name:grpc-ping-server}") String serverId) {
        this.serverId = serverId;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PongResponse> responseObserver) {
        String incoming = request.getMessage().trim();
        log.info("Received request: message='{}', clientId='{}'", incoming, request.getClientId());

        if (incoming.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("'message' must not be empty")
                    .asRuntimeException());
            return;
        }

        String reply = "ping".equalsIgnoreCase(incoming) ? "Pong" : "Pong (you said: '" + incoming + "')";

        PongResponse response = PongResponse.newBuilder()
                .setMessage(reply)
                .setServerId(serverId)
                .setTimestamp(System.currentTimeMillis())
                .setRequestCount(requestCounter.incrementAndGet())
                .setEcho(incoming)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
