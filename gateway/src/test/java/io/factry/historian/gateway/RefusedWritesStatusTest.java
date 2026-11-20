package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import io.factry.historian.proto.Collector;
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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Factry can answer the connection check (GetCollector) while it refuses every write, for example
 * INTERNAL while its InfluxDB is down (#59). Store and forward keeps that data, but the profile
 * status must say that nothing gets stored. The status here is what the provider's getStatus()
 * returns once started: the same {@link WriteHealth}, fed by the real storage engine.
 */
class RefusedWritesStatusTest {

    private Server server;
    private FactryGrpcClient client;
    private volatile Status createPointsStatus = Status.OK;
    private volatile long createPointsDelayMs = 0;
    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final WriteHealth health = new WriteHealth("factry-a", now::get);
    private FactryStorageEngine engine;

    private class Factry extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getCollector(com.google.protobuf.Empty request, StreamObserver<Collector> observer) {
            observer.onNext(Collector.newBuilder().setUuid("collector-uuid").setStatus("Active").build());
            observer.onCompleted();
        }

        @Override
        public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                            StreamObserver<Measurements> observer) {
            observer.onNext(Measurements.newBuilder()
                    .addMeasurements(Measurement.newBuilder().setUuid("uuid-" + request.getKeyword())
                            .setName(request.getKeyword()).setStatus("Active"))
                    .setTotal(1).build());
            observer.onCompleted();
        }

        @Override
        public void createPoints(Points request, StreamObserver<CreatePointsReply> observer) {
            if (createPointsDelayMs > 0) {
                try {
                    Thread.sleep(createPointsDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (createPointsStatus.isOk()) {
                observer.onNext(CreatePointsReply.getDefaultInstance());
                observer.onCompleted();
            } else {
                observer.onError(createPointsStatus.asRuntimeException());
            }
        }
    }

    @BeforeEach
    void start() throws Exception {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .addService(new Factry()).build().start();
        client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(),
                "collector-uuid", "token", false, false, null);
        MetricRegistry registry = new MetricRegistry();
        GatewayContext context = (GatewayContext) Proxy.newProxyInstance(
                GatewayContext.class.getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetricRegistry")) {
                        return registry;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        engine = new FactryStorageEngine(context, "factry", new FactryHistorianSettings(), client,
                new MeasurementCache(), new HistorianMetrics(), health);
        // What the provider's scheduled connection check does
        health.connectionChecked(client.testConnection());
    }

    @AfterEach
    void stop() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private StorageResult<AtomicPoint<?>> flush() throws Exception {
        AtomicPoint<?> point = DataPointFactory.createAtomicPoint(42.0, QualityCode.Good, Instant.now(),
                QualifiedPath.newBuilder().setProvider("default").setTag("Pump1/Flow").build());
        return engine.storeAtomic(List.of(point)).toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private void after(long seconds) {
        now.addAndGet(seconds * 1000);
    }

    /** Runs {@code action} and returns what it logged. */
    private static String logOf(ThrowingRunnable action) throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            action.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString();
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void everyWriteRefusedFor30SecondsTurnsTheProfileErroredUntilOneIsAccepted() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("influxdb: connection refused");

        assertTrue(flush().error().isPresent(), "store and forward keeps the refused flush");
        assertEquals(ProfileStatus.State.Good, health.status().getState(), "one refusal is no alarm");
        after(29);
        flush();
        assertEquals(ProfileStatus.State.Good, health.status().getState());

        after(1);
        flush();
        health.connectionChecked(client.testConnection());
        ProfileStatus status = health.status();
        assertEquals(ProfileStatus.State.Errored, status.getState(), "the connection check passes, writes don't");
        assertEquals("Factry does not store writes", status.getMessage().toString());
        assertEquals("No write to Factry has succeeded for 30 s (last answer: INTERNAL: influxdb: connection"
                + " refused). Store and forward keeps the data and retries it.", status.getErrorDetails());

        createPointsStatus = Status.OK;
        assertEquals(StorageResult.Type.SUCCESS, flush().type());
        assertSame(ProfileStatus.RUNNING, health.status(), "back at the first accepted write");
    }

    @Test
    void anAcceptedWriteInBetweenStartsTheCountAgain() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("refused");
        flush();
        after(20);
        createPointsStatus = Status.OK;
        flush();
        createPointsStatus = Status.INTERNAL.withDescription("refused");
        after(20);
        flush();

        after(20);
        flush();
        assertEquals(ProfileStatus.State.Good, health.status().getState(), "refused for 20 s only");
        after(10);
        flush();
        assertEquals(ProfileStatus.State.Errored, health.status().getState());
    }

    /**
     * InfluxDB hanging instead of refusing: Factry answers GetCollector, but every CreatePoints
     * times out. Nothing is stored, so the status must say so.
     */
    @Test
    void writesThatAlwaysTimeOutTurnErrored() throws Exception {
        createPointsDelayMs = 1_500; // over the test write deadline of 1 s plus 1 ms per point
        assertTrue(flush().error().isPresent());
        after(60);
        assertTrue(flush().error().isPresent());
        health.connectionChecked(client.testConnection());

        ProfileStatus status = health.status();
        assertEquals(ProfileStatus.State.Errored, status.getState(), "nothing stored for 60 s, check passes");
        assertTrue(status.getErrorDetails().contains("DEADLINE_EXCEEDED"), status.getErrorDetails());
    }

    /** One refused flush, then a quiet profile (no writes) for 30 s: one refusal is no alarm. */
    @Test
    void oneRefusalThenQuietStaysGood() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("transient");
        flush();
        createPointsStatus = Status.OK;
        after(31);
        health.connectionChecked(client.testConnection());

        assertEquals(ProfileStatus.State.Good, health.status().getState(), "only one write was refused");
    }

    /**
     * Refusals over 25 s, then Factry restarted (the check fails for 2 min). Back, the first write
     * is refused again: one refusal since it came back is no alarm, whatever happened before.
     */
    @Test
    void refusalsBeforeAnOutageAreForgottenAfterIt() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("influx down");
        flush();
        after(25);
        flush();
        after(10);
        health.connectionChecked(false);
        after(120);
        health.connectionChecked(client.testConnection());
        assertEquals(ProfileStatus.State.Good, health.status().getState(), "back, no write tried yet");

        after(5);
        flush();

        assertEquals(ProfileStatus.State.Good, health.status().getState(), "one refusal since it came back");
    }

    @Test
    void newSettingsJudgeWritesAfresh() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("refused");
        flush();
        after(30);
        flush();
        assertEquals(ProfileStatus.State.Errored, health.status().getState());

        health.clearWrites();

        assertEquals(ProfileStatus.State.Good, health.status().getState());
    }

    @Test
    void aFailedConnectionCheckIsErrored() {
        health.connectionChecked(false);

        assertEquals(ProfileStatus.State.Errored, health.status().getState());
    }

    /**
     * Live, a refusing Factry gave 216 identical ERROR lines in 3 minutes. One WARN per new answer,
     * a summary about every 5 minutes, and the totals when Factry stores again; each line names
     * the profile.
     */
    @Test
    void refusalsAreLoggedOncePerAnswerWithASummary() throws Exception {
        createPointsStatus = Status.INTERNAL.withDescription("influx down");
        String first = logOf(this::flush);
        String repeats = logOf(() -> {
            for (int i = 0; i < 5; i++) {
                after(10);
                flush();
                health.connectionChecked(client.testConnection());
            }
        });
        createPointsStatus = Status.INTERNAL.withDescription("influx timeout");
        String changed = logOf(this::flush);
        createPointsStatus = Status.INTERNAL.withDescription("influx timeout");
        String summary = logOf(() -> {
            after(300);
            flush();
            health.connectionChecked(client.testConnection());
        });
        createPointsStatus = Status.OK;
        String back = logOf(this::flush);

        assertTrue(first.contains("WARN") && first.contains("Historian 'factry-a': Factry did not store 1 points"
                + " (INTERNAL: influx down)"), first);
        assertFalse(first.contains("ERROR"), "nothing is lost: " + first);
        assertEquals(1, count(repeats, "WARN"), "only the switch to Errored: " + repeats);
        assertTrue(repeats.contains("Historian 'factry-a': No write to Factry has succeeded for 30 s"), repeats);
        assertTrue(changed.contains("WARN") && changed.contains("influx timeout"), changed);
        assertTrue(summary.contains("Historian 'factry-a': Factry still does not store writes: 4 flushes / 4 points"
                + " in the last 5 min (last answer: INTERNAL: influx timeout)"), summary);
        assertTrue(back.contains("INFO") && back.contains("Historian 'factry-a': Factry stores writes again,"
                + " after 8 failed flushes / 8 points"), back);
    }

    private static int count(String text, String word) {
        return text.split(word, -1).length - 1;
    }
}
