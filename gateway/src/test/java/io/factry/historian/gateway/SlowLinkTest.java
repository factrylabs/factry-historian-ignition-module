package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.CreateMeasurementsReply;
import io.factry.historian.proto.CreateMeasurementsRequest;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Point;
import io.factry.historian.proto.Points;
import io.grpc.DecompressorRegistry;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * On a slow link a full store and forward batch takes longer to send than a small one. The
 * write deadline must grow with the batch, or the same batch times out on every retry and the
 * backlog never shrinks. Limits come from the test copy of factry-historian.properties.
 */
class SlowLinkTest {

    /** Time the fake Factry needs per point: 10,000 points take 3 s, three times the base deadline. */
    private static final long NANOS_PER_POINT = 300_000;

    private Server server;
    private FactryGrpcClient client;
    private final AtomicInteger storedPoints = new AtomicInteger();
    private final AtomicInteger createdMeasurements = new AtomicInteger();
    private final AtomicReference<String> writeEncoding = new AtomicReference<>();
    private final List<String> writeEncodings = Collections.synchronizedList(new ArrayList<>());

    private class SlowFactry extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                            StreamObserver<Measurements> observer) {
            observer.onNext(Measurements.newBuilder()
                    .addMeasurements(Measurement.newBuilder()
                            .setUuid("uuid-" + request.getKeyword())
                            .setName(request.getKeyword())
                            .setStatus("Active"))
                    .setTotal(1)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void createPoints(Points request, StreamObserver<CreatePointsReply> observer) {
            try {
                TimeUnit.NANOSECONDS.sleep(NANOS_PER_POINT * request.getPointsCount());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            storedPoints.addAndGet(request.getPointsCount());
            observer.onNext(CreatePointsReply.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public void createMeasurements(CreateMeasurementsRequest request,
                                       StreamObserver<CreateMeasurementsReply> observer) {
            try {
                TimeUnit.NANOSECONDS.sleep(NANOS_PER_POINT * request.getMeasurementsCount());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            createdMeasurements.addAndGet(request.getMeasurementsCount());
            observer.onNext(CreateMeasurementsReply.getDefaultInstance());
            observer.onCompleted();
        }
    }

    /** Records how the CreatePoints request body was encoded. */
    private class EncodingRecorder implements ServerInterceptor {
        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                                                          ServerCallHandler<Q, R> next) {
            if (call.getMethodDescriptor().getBareMethodName().equals("CreatePoints")) {
                String encoding = headers.get(Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER));
                writeEncoding.set(encoding);
                writeEncodings.add(encoding);
            }
            return next.startCall(call, headers);
        }
    }

    @BeforeEach
    void startServer() throws Exception {
        server = NettyServerBuilder.forAddress(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0))
                .addService(ServerInterceptors.intercept(new SlowFactry(), new EncodingRecorder()))
                .build().start();
        client = new FactryGrpcClient(java.net.InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(), "collector-uuid", "token",
                false, false, null);
    }

    @AfterEach
    void stopServer() throws Exception {
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        client.shutdown();
    }

    private static FactryStorageEngine engine(FactryGrpcClient client) {
        // The engine only asks the context for a metric registry; fail loudly on anything else.
        MetricRegistry registry = new MetricRegistry();
        GatewayContext context = (GatewayContext) Proxy.newProxyInstance(
                GatewayContext.class.getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetricRegistry")) {
                        return registry;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new FactryStorageEngine(context, "factry", new FactryHistorianSettings(), client,
                new MeasurementCache(), new HistorianMetrics());
    }

    private static List<AtomicPoint<?>> batch(int size) {
        QualifiedPath path = QualifiedPath.newBuilder().setProvider("default").setTag("Pump1/Flow").build();
        Instant start = Instant.now().minusSeconds(size);
        List<AtomicPoint<?>> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(DataPointFactory.createAtomicPoint((double) i, QualityCode.Good, start.plusSeconds(i), path));
        }
        return points;
    }

    @Test
    void largeBatchOnSlowLinkIsStored() throws Exception {
        StorageResult<AtomicPoint<?>> result = engine(client)
                .storeAtomic(batch(10_000)).toCompletableFuture().get(60, TimeUnit.SECONDS);

        assertTrue(result.error().isEmpty(), "batch failed: " + result.error());
        assertEquals(10_000, storedPoints.get());
    }

    @Test
    void writesAreGzipCompressed() throws Exception {
        engine(client).storeAtomic(batch(10)).toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertEquals("gzip", writeEncoding.get());
    }

    @Test
    void writesFallBackToUncompressedWhenFactryRejectsGzip() throws Exception {
        // A Factry without the gzip decompressor answers UNIMPLEMENTED. An empty registry gives
        // this server the same gap.
        Server noGzipServer = NettyServerBuilder.forAddress(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0))
                .decompressorRegistry(DecompressorRegistry.emptyInstance())
                .addService(ServerInterceptors.intercept(new SlowFactry(), new EncodingRecorder()))
                .build().start();
        FactryGrpcClient noGzipClient = new FactryGrpcClient(java.net.InetAddress.getLoopbackAddress().getHostAddress(), noGzipServer.getPort(),
                "collector-uuid", "token", false, false, null);
        try {
            FactryStorageEngine engine = engine(noGzipClient);
            StorageResult<AtomicPoint<?>> first = engine.storeAtomic(batch(10))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
            StorageResult<AtomicPoint<?>> second = engine.storeAtomic(batch(5))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);

            // A rejection comes back as a failure without an error, which drops the points.
            assertTrue(first.failedData().isEmpty() && first.error().isEmpty(), "first batch: " + first.type());
            assertTrue(second.failedData().isEmpty() && second.error().isEmpty(), "second batch: " + second.type());
            assertEquals(15, storedPoints.get());
            // Rejected gzip calls never reach the service, so every recorded write is uncompressed.
            assertEquals(Arrays.asList(null, null), writeEncodings);
        } finally {
            noGzipServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            noGzipClient.shutdown();
        }
    }

    @Test
    void manyNewMeasurementsOnSlowLinkAreCreated() {
        // A repeated create would add Discovered duplicates, so it must fit in one call.
        CreateMeasurementsRequest.Builder request = CreateMeasurementsRequest.newBuilder();
        for (int i = 0; i < 10_000; i++) {
            request.addMeasurements(CreateMeasurement.newBuilder().setName("default/Tag" + i));
        }

        client.createMeasurements(request.build());

        assertEquals(10_000, createdMeasurements.get());
    }

    @Test
    void newSettingsTurnGzipBackOn() throws Exception {
        Server noGzipServer = NettyServerBuilder.forAddress(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0))
                .decompressorRegistry(DecompressorRegistry.emptyInstance())
                .addService(ServerInterceptors.intercept(new SlowFactry(), new EncodingRecorder()))
                .build().start();
        Points points = Points.newBuilder().addPoints(Point.newBuilder().setMeasurementUUID("uuid")).build();
        try {
            client.reconfigure(java.net.InetAddress.getLoopbackAddress().getHostAddress(), noGzipServer.getPort(), "collector-uuid", "token", false, false, null);
            client.createPoints(points);
            assertNull(writeEncoding.get(), "gzip turned off by the fallback");

            client.reconfigure(java.net.InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(), "collector-uuid", "token", false, false, null);
            client.createPoints(points);

            assertEquals("gzip", writeEncoding.get(), "a settings save sends gzip again");
        } finally {
            noGzipServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void onlyAMissingDecompressorCountsAsGzipRejected() {
        // Factry is grpc-go; this is its message for a missing decompressor.
        assertTrue(FactryGrpcClient.isGzipRejected(Status.UNIMPLEMENTED
                .withDescription("grpc: Decompressor is not installed for grpc-encoding \"gzip\"").asRuntimeException()));
        assertFalse(FactryGrpcClient.isGzipRejected(Status.UNIMPLEMENTED
                .withDescription("unknown method CreatePoints for service historian.Historian").asRuntimeException()));
        assertFalse(FactryGrpcClient.isGzipRejected(Status.UNIMPLEMENTED.asRuntimeException()));
    }
}
