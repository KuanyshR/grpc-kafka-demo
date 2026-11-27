package com.example.grpc.server;

import com.example.grpc.pingpong.v1.PingRequest;
import com.example.grpc.pingpong.v1.PongResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PingPongServiceImplTest {

    private final PingPongServiceImpl service = new PingPongServiceImpl("test-server");

    @Test
    void respondsPongToPing() {
        RecordingObserver observer = new RecordingObserver();

        service.ping(PingRequest.newBuilder().setMessage("Ping").setClientId("junit").build(), observer);

        assertThat(observer.error).isNull();
        assertThat(observer.completed).isTrue();
        assertThat(observer.value.getMessage()).isEqualTo("Pong");
        assertThat(observer.value.getServerId()).isEqualTo("test-server");
        assertThat(observer.value.getRequestCount()).isEqualTo(1);
        assertThat(observer.value.getEcho()).isEqualTo("Ping");
    }

    @Test
    void rejectsEmptyMessage() {
        RecordingObserver observer = new RecordingObserver();

        service.ping(PingRequest.newBuilder().setMessage("  ").build(), observer);

        assertThat(observer.value).isNull();
        assertThat(Status.fromThrowable(observer.error).getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    private static final class RecordingObserver implements StreamObserver<PongResponse> {
        PongResponse value;
        Throwable error;
        boolean completed;

        @Override public void onNext(PongResponse v) { value = v; }
        @Override public void onError(Throwable t) { error = t; }
        @Override public void onCompleted() { completed = true; }
    }
}
