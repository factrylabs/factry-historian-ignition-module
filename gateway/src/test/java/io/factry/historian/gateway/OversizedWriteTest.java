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
import io.factry.historian.proto.Point;
import io.factry.historian.proto.Points;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Factry refuses a CreatePoints message over its size limit with RESOURCE_EXHAUSTED, every time
 * (64 MiB on v8.2.3, checked after decompression). Store and forward would keep such a flush and
 * retry it for ever, together with the good points in it. The module must send it in parts.
 */
class OversizedWriteTest {

    private Server server;
    private FactryGrpcClient client;
    /** Every CreatePoints message the server accepted. */
    private final List<Points> received = new CopyOnWriteArrayList<>();
    private volatile int maxMessageBytes;
    /** How a too-large write is refused: Factry's own answer, or a proxy's HTTP 413 as grpc-java sees it. */
    private volatile Status tooLarge;
    /** Writes refused for their size. */
    private final AtomicInteger refused = new AtomicInteger();
    /** How long the server takes to answer a write, like a slow link. */
    private volatile long answerDelayMs;
    private FactryStorageEngine engine;

    /** Answers every lookup with an Active string measurement of that name, keeps every write. */
    private class Factry extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                            StreamObserver<Measurements> observer) {
            observer.onNext(Measurements.newBuilder()
                    .addMeasurements(Measurement.newBuilder()
                            .setUuid("uuid-" + request.getKeyword())
                            .setName(request.getKeyword())
                            .setDatatype("string")
                            .setStatus("Active"))
                    .setTotal(1)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void createPoints(Points request, StreamObserver<CreatePointsReply> observer) {
            if (request.getSerializedSize() > maxMessageBytes) {
                refused.incrementAndGet();
                observer.onError(tooLarge.asRuntimeException());
                return;
            }
            if (answerDelayMs > 0) {
                try {
                    Thread.sleep(answerDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(request);
            observer.onNext(CreatePointsReply.getDefaultInstance());
            observer.onCompleted();
        }
    }

    /** A Factry whose message limit is {@code maxBytes}. */
    private void startFactry(int maxBytes) throws Exception {
        maxMessageBytes = maxBytes;
        // Factry v8.2.3's answer, verbatim (grpc-go checks the size after decompression)
        tooLarge = Status.RESOURCE_EXHAUSTED.withDescription(
                "grpc: received message after decompression larger than max " + maxBytes);
        server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .maxInboundMessageSize(Integer.MAX_VALUE) // the limit is checked in createPoints
                .addService(new Factry()).build().start();
        client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(),
                "collector-uuid", "token", false, false, null);
    }

    @AfterEach
    void stop() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private StorageResult<AtomicPoint<?>> store(List<AtomicPoint<?>> points) throws Exception {
        MetricRegistry registry = new MetricRegistry();
        GatewayContext context = (GatewayContext) Proxy.newProxyInstance(
                GatewayContext.class.getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetricRegistry")) {
                        return registry;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        if (engine == null) {
            engine = new FactryStorageEngine(context, "factry", new FactryHistorianSettings(),
                    client, new MeasurementCache(), new HistorianMetrics());
        }
        return engine.storeAtomic(points).toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static List<AtomicPoint<?>> textPoints(int count, int chars) {
        List<AtomicPoint<?>> points = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            points.add(DataPointFactory.createAtomicPoint(i + ":" + "x".repeat(chars), QualityCode.Good,
                    Instant.ofEpochMilli(1_000L + i),
                    QualifiedPath.newBuilder().setProvider("default").setTag("Doc/Text").build()));
        }
        return points;
    }

    private List<Point> receivedPoints() {
        return received.stream().flatMap(p -> p.getPointsList().stream()).toList();
    }

    @Test
    void aFlushOverFactrysLimitIsStoredInParts() throws Exception {
        startFactry(64 * 1024);
        List<AtomicPoint<?>> points = textPoints(40, 10 * 1024); // about 400 KiB in one flush

        StorageResult<AtomicPoint<?>> result = store(points);

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
        assertTrue(received.size() > 1, "sent in parts");
        List<Point> sent = receivedPoints();
        assertEquals(40, sent.size(), "every point arrives once");
        for (int i = 0; i < sent.size(); i++) {
            assertEquals(1_000L + i, sent.get(i).getTimestamp().getSeconds() * 1000
                    + sent.get(i).getTimestamp().getNanos() / 1_000_000, "in order");
            assertTrue(sent.get(i).getValue().getStringValue().startsWith(i + ":"));
        }
    }

    @Test
    void aFlushOverTheModulesMessageSizeGoesInSeveralCalls() throws Exception {
        startFactry(64 * 1024 * 1024);
        List<AtomicPoint<?>> points = textPoints(10, 1024 * 1024); // 10 MiB, Factry would take it

        StorageResult<AtomicPoint<?>> result = store(points);

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
        assertEquals(10, receivedPoints().size());
        assertTrue(received.size() >= 3, "at most 4 MiB per call, got " + received.size() + " calls");
        received.forEach(p -> assertTrue(p.getSerializedSize() <= 4 * 1024 * 1024, "message of "
                + p.getSerializedSize() + " bytes"));
    }

    @Test
    void aSingleValueTooLargeForFactryIsStoredAsBadAndTheRestWithTheirValues() throws Exception {
        startFactry(64 * 1024);
        List<AtomicPoint<?>> points = new ArrayList<>(textPoints(3, 100));
        points.set(1, DataPointFactory.createAtomicPoint("y".repeat(100 * 1024), QualityCode.Good,
                Instant.ofEpochMilli(1_001L), QualifiedPath.newBuilder().setProvider("default").setTag("Doc/Text").build()));

        StorageResult<AtomicPoint<?>> result = store(points);

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
        List<Point> sent = receivedPoints();
        assertEquals(3, sent.size());
        assertTrue(sent.get(0).getValue().getStringValue().startsWith("0:"));
        assertEquals("Bad", sent.get(1).getStatus());
        assertFalse(sent.get(1).hasValue(), "no value for the point Factry cannot take");
        assertEquals(1_001L, sent.get(1).getTimestamp().getNanos() / 1_000_000 + sent.get(1).getTimestamp().getSeconds() * 1000);
        assertTrue(sent.get(2).getValue().getStringValue().startsWith("2:"));
    }

    /**
     * A proxy in front of Factry that refuses a large body (nginx: 1 MiB by default) answers HTTP
     * 413, which grpc-java reports as UNKNOWN "HTTP status code 413".
     */
    @Test
    void aProxysHttp413IsTooLargeToo() throws Exception {
        startFactry(64 * 1024);
        tooLarge = Status.UNKNOWN.withDescription("HTTP status code 413\ninvalid content-type: text/html");

        StorageResult<AtomicPoint<?>> result = store(textPoints(40, 10 * 1024));

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
        assertEquals(40, receivedPoints().size());
    }

    @Test
    void theSizeThatWorkedIsRememberedForTheNextFlush() throws Exception {
        startFactry(64 * 1024);
        store(textPoints(40, 10 * 1024));
        int refusedFirst = refused.get();
        assertTrue(refusedFirst > 0, "the first flush finds the limit");

        StorageResult<AtomicPoint<?>> result = store(textPoints(40, 10 * 1024));

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
        assertEquals(refusedFirst, refused.get(), "the second flush sends parts that fit at once");
        assertEquals(80, receivedPoints().size());
    }

    /**
     * The write deadline grew with the point count only: a part of a few big strings got about
     * the base deadline, and on a slow link timed out on every retry.
     */
    @Test
    void aBigPartGetsTimeToTravelOverASlowLink() throws Exception {
        startFactry(64 * 1024 * 1024);
        answerDelayMs = 2_500; // the test write deadline is 1 s plus 1 ms per point

        StorageResult<AtomicPoint<?>> result = store(textPoints(1, 1024 * 1024));

        assertEquals(StorageResult.Type.SUCCESS, result.type(), "error: " + result.error());
    }
}
