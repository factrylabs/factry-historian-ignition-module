package io.factry.historian.gateway;

import com.google.protobuf.Empty;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Measurements;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The connection check runs every 30 s and decides when store and forward resumes, so it must
 * stay cheap: loading every measurement times out on a large site and the check never passes.
 */
class FactryGrpcClientConnectionTest {

    private Server server;
    private FactryGrpcClient client;
    private final AtomicInteger getMeasurementsCalls = new AtomicInteger();
    private volatile Status collectorStatus = Status.OK;

    /** A Factry whose measurement list never arrives, as on a slow link with many measurements. */
    private class SlowMeasurementsHistorian extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getCollector(Empty request, StreamObserver<Collector> observer) {
            if (!collectorStatus.isOk()) {
                observer.onError(collectorStatus.asRuntimeException());
                return;
            }
            observer.onNext(Collector.newBuilder().setUuid("collector-uuid").build());
            observer.onCompleted();
        }

        @Override
        public void getMeasurements(MeasurementRequest request, StreamObserver<Measurements> observer) {
            getMeasurementsCalls.incrementAndGet();
            // Never answer.
        }
    }

    @BeforeEach
    void startServer() throws Exception {
        server = NettyServerBuilder.forPort(0).addService(new SlowMeasurementsHistorian()).build().start();
        client = new FactryGrpcClient("localhost", server.getPort(), "collector-uuid", "token",
                false, false, null);
    }

    @AfterEach
    void stopServer() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void connectedWithoutLoadingMeasurements() {
        assertTrue(client.testConnection());
        assertTrue(client.isConnected());
        assertEquals(0, getMeasurementsCalls.get());
    }

    @Test
    void rejectedTokenIsNotConnected() {
        collectorStatus = Status.UNAUTHENTICATED;
        assertFalse(client.testConnection());
        assertFalse(client.isConnected());
    }

    @Test
    void unreachableServerIsNotConnected() throws Exception {
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        assertFalse(client.testConnection());
        assertFalse(client.isConnected());
    }
}
