package com.example.grpc.client;

import com.example.grpc.pingpong.v1.PingPongServiceGrpc;
import com.example.grpc.pingpong.v1.PingRequest;
import com.example.grpc.pingpong.v1.PongResponse;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class PingClientRunner implements CommandLineRunner {

    private final String host;
    private final int port;
    private final String message;
    private final String clientId;
    private final long deadlineSeconds;

    public PingClientRunner(@Value("${grpc.client.host}") String host,
                            @Value("${grpc.client.port}") int port,
                            @Value("${grpc.client.message}") String message,
                            @Value("${grpc.client.client-id}") String clientId,
                            @Value("${grpc.client.deadline-seconds}") long deadlineSeconds) {
        this.host = host;
        this.port = port;
        this.message = message;
        this.clientId = clientId;
        this.deadlineSeconds = deadlineSeconds;
    }

    @Override
    public void run(String... args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            PingPongServiceGrpc.PingPongServiceBlockingStub stub = PingPongServiceGrpc
                    .newBlockingStub(channel)
                    .withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);

            PingRequest request = PingRequest.newBuilder()
                    .setMessage(message)
                    .setClientId(clientId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            System.out.println(">>> Sending request to " + host + ":" + port);
            System.out.println(JsonFormat.printer().print(request));

            PongResponse response = stub.ping(request);

            System.out.println("<<< Received response");
            System.out.println(JsonFormat.printer().print(response));
        } catch (StatusRuntimeException e) {
            System.err.println("gRPC call failed: " + e.getStatus());
            throw e;
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
