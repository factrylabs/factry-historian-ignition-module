package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Points;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A batch Factry refuses must come back as a result that carries an error. Ignition's
 * TagHistoryDataSinkBridge only throws (so Store and Forward keeps the data) when
 * {@link StorageResult#error()} is present; a failure without an error is treated as
 * stored and the points are gone.
 */
class FactryStorageEngineRefusalTest {

    private Server server;
    private FactryGrpcClient client;
    private volatile Status createPointsStatus;
    private final AtomicInteger createPointsCalls = new AtomicInteger();

    /** Answers every lookup with an Active measurement of that name, refuses CreatePoints. */
    private class RefusingHistorian extends HistorianGrpc.HistorianImplBase {
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
            createPointsCalls.incrementAndGet();
            observer.onError(createPointsStatus.asRuntimeException());
        }
    }

    @BeforeEach
    void startServer() throws Exception {
        server = NettyServerBuilder.forPort(0).addService(new RefusingHistorian()).build().start();
        client = new FactryGrpcClient("localhost", server.getPort(), "collector-uuid", "token",
                false, false, null);
    }

    @AfterEach
    void stopServer() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static FactryStorageEngine engine(FactryGrpcClient client) {
        // The base engine only asks the context for a metric registry; fail loudly on anything else.
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

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {
            "INTERNAL", "UNKNOWN", "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED",
            "UNAUTHENTICATED", "PERMISSION_DENIED", "FAILED_PRECONDITION"})
    void refusedBatchIsNotReportedAsHandled(Status.Code code) throws Exception {
        createPointsStatus = Status.fromCode(code).withDescription("refused by test server");
        AtomicPoint<?> point = DataPointFactory.createAtomicPoint(42.0, QualityCode.Good, Instant.now(),
                QualifiedPath.newBuilder().setProvider("default").setTag("Pump1/Flow").build());

        StorageResult<AtomicPoint<?>> result = engine(client)
                .storeAtomic(List.of(point)).toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertTrue(result.error().isPresent(),
                "a refused batch must carry an error so Store and Forward keeps it, got " + result.type());
        assertTrue(result.successfulData().isEmpty(), "nothing was stored");
    }

    /**
     * Only an answer about the measurements is worth a cache eviction and a second attempt.
     * Factry v8.2.3 answers INTERNAL while InfluxDB is down; retrying then just doubles the
     * load on a Factry that is already in trouble.
     */
    @ParameterizedTest
    @CsvSource({"NOT_FOUND, 2", "INVALID_ARGUMENT, 2", "INTERNAL, 1", "UNKNOWN, 1",
            "RESOURCE_EXHAUSTED, 1", "UNAUTHENTICATED, 1"})
    void onlyMeasurementErrorsTriggerARetry(Status.Code code, int expectedCalls) throws Exception {
        createPointsStatus = Status.fromCode(code).withDescription("refused by test server");
        AtomicPoint<?> point = DataPointFactory.createAtomicPoint(42.0, QualityCode.Good, Instant.now(),
                QualifiedPath.newBuilder().setProvider("default").setTag("Pump1/Flow").build());

        engine(client).storeAtomic(List.of(point)).toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertEquals(expectedCalls, createPointsCalls.get(), "CreatePoints calls for " + code);
    }
}
