package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import io.factry.historian.proto.AssetProperties;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetAssetPropertiesRequest;
import io.factry.historian.proto.GetAssetsRequest;
import io.factry.historian.proto.GetCollectorsRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HealthUpdateReply;
import io.factry.historian.proto.HealthUpdates;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Points;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.RegisterCollectorSchema;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A Factry that accepts the connection but never answers must not hang the module. Every call
 * gives up after its time limit, and shutdown and a settings save do not wait for calls that
 * never end. The limits come from the test copy of factry-historian.properties: write 1 s,
 * read 6 s, small calls 1 s, shutdown grace 1 s.
 */
class GrpcTimeLimitsTest {

    private static final String KNOWN = "w1/Known";

    private Server server;
    private FactryGrpcClient client;
    private final FakeFactry factry = new FakeFactry();

    /** Answers normally until {@link #hang} is set; then it accepts every call and never answers. */
    private static class FakeFactry extends HistorianGrpc.HistorianImplBase {
        volatile boolean hang;
        final CountDownLatch queryArrived = new CountDownLatch(1);

        private <T> void answer(StreamObserver<T> observer, T reply) {
            if (hang) {
                return;
            }
            observer.onNext(reply);
            observer.onCompleted();
        }

        @Override
        public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                            StreamObserver<Measurements> observer) {
            answer(observer, Measurements.newBuilder()
                    .addMeasurements(Measurement.newBuilder()
                            .setUuid("uuid-known").setName(KNOWN).setStatus("Active"))
                    .setTotal(1)
                    .build());
        }

        @Override
        public void getMeasurements(MeasurementRequest request, StreamObserver<Measurements> observer) {
            answer(observer, Measurements.getDefaultInstance());
        }

        @Override
        public void createPoints(Points request, StreamObserver<CreatePointsReply> observer) {
            answer(observer, CreatePointsReply.getDefaultInstance());
        }

        @Override
        public void queryTimeseries(QueryTimeseriesRequest request,
                                    StreamObserver<QueryTimeseriesResponse> observer) {
            queryArrived.countDown();
            answer(observer, QueryTimeseriesResponse.getDefaultInstance());
        }

        @Override
        public void getCollector(com.google.protobuf.Empty request, StreamObserver<Collector> observer) {
            answer(observer, Collector.getDefaultInstance());
        }

        @Override
        public void getCollectors(GetCollectorsRequest request, StreamObserver<Collectors> observer) {
            answer(observer, Collectors.getDefaultInstance());
        }

        @Override
        public void getAssets(GetAssetsRequest request, StreamObserver<Assets> observer) {
            answer(observer, Assets.getDefaultInstance());
        }

        @Override
        public void getAssetProperties(GetAssetPropertiesRequest request,
                                       StreamObserver<AssetProperties> observer) {
            answer(observer, AssetProperties.getDefaultInstance());
        }

        @Override
        public void registerCollector(RegisterCollectorSchema request, StreamObserver<Collector> observer) {
            answer(observer, Collector.getDefaultInstance());
        }

        @Override
        public void updateHealth(HealthUpdates request, StreamObserver<HealthUpdateReply> observer) {
            answer(observer, HealthUpdateReply.getDefaultInstance());
        }

        @Override
        public void updateCollectorState(Struct request, StreamObserver<Empty> observer) {
            answer(observer, Empty.getDefaultInstance());
        }
    }

    @BeforeEach
    void startServer() throws Exception {
        server = NettyServerBuilder.forAddress(new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0))
                .sslContext(TestTls.server(TestTls.LOOPBACK)).addService(factry).build().start();
        client = new FactryGrpcClient(java.net.InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(), "collector-uuid", "token",
                true, false, TestTls.caPem());
    }

    @AfterEach
    void stopServer() throws Exception {
        // Server first: that ends calls a test left hanging, so the client can shut down.
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        client.shutdown();
    }

    private static GatewayContext gatewayContext() {
        // The engines only ask the context for a metric registry; fail loudly on anything else.
        MetricRegistry registry = new MetricRegistry();
        return (GatewayContext) Proxy.newProxyInstance(
                GatewayContext.class.getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getMetricRegistry")) {
                        return registry;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Runs {@code call}, fails if it takes longer than {@code limit}, returns how long it took. */
    private static Duration timed(Duration limit, Executable call) {
        Instant start = Instant.now();
        assertTimeoutPreemptively(limit, call);
        return Duration.between(start, Instant.now());
    }

    private static void assertDeadlineExceeded(Executable call) {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, call);
        assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
    }

    @Test
    void storeGivesUpAfterWriteDeadline() {
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(client);
        factry.hang = true;
        FactryStorageEngine engine = new FactryStorageEngine(gatewayContext(), "factry",
                new FactryHistorianSettings(), client, cache, new HistorianMetrics());
        AtomicPoint<?> point = DataPointFactory.createAtomicPoint(42.0, QualityCode.Good, Instant.now(),
                QualifiedPath.newBuilder().setProvider("default").setTag("Known").build());

        timed(Duration.ofSeconds(5), () -> {
            StorageResult<AtomicPoint<?>> result =
                    engine.storeAtomic(List.of(point)).toCompletableFuture().get();
            assertTrue(result.error().isPresent(), "an unanswered store must come back as an error");
        });
    }

    @Test
    void queryGivesUpAfterReadDeadline() {
        factry.hang = true;
        Duration took = timed(Duration.ofSeconds(10), () -> assertDeadlineExceeded(
                () -> client.queryTimeseries(QueryTimeseriesRequest.getDefaultInstance())));
        assertTrue(took.toMillis() >= 5_000, "a query gets the read deadline, not the write one: " + took);
    }

    @Test
    void cacheRefreshGivesUpAndKeepsThePreviousCache() {
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(client);
        assertEquals("uuid-known", cache.getUUID(KNOWN));

        factry.hang = true;
        timed(Duration.ofSeconds(10), () -> assertThrows(RuntimeException.class, () -> cache.refresh(client)));
        assertEquals("uuid-known", cache.getUUID(KNOWN), "a failed refresh keeps what was cached");
    }

    @Test
    void assetCallsGiveUpAfterReadDeadline() {
        factry.hang = true;
        List<Executable> calls = List.of(
                client::getAssets,
                () -> client.getAssetProperties(GetAssetPropertiesRequest.getDefaultInstance()));
        for (Executable call : calls) {
            Duration took = timed(Duration.ofSeconds(10), () -> assertDeadlineExceeded(call));
            assertTrue(took.toMillis() >= 5_000, "asset calls get the read deadline: " + took);
        }
    }

    @Test
    void smallCallsGiveUpAfterCallDeadline() {
        factry.hang = true;
        List<Executable> calls = List.of(
                client::getCollectors,
                () -> client.getMeasurements(MeasurementRequest.getDefaultInstance()),
                () -> client.registerCollector(RegisterCollectorSchema.getDefaultInstance()),
                () -> client.updateHealth(HealthUpdates.getDefaultInstance()),
                () -> client.updateCollectorState(Struct.getDefaultInstance()));
        for (Executable call : calls) {
            timed(Duration.ofSeconds(3), () -> assertDeadlineExceeded(call));
        }
    }

    @Test
    void shutdownDoesNotWaitForACallThatNeverEnds() throws Exception {
        factry.hang = true;
        CompletableFuture<Throwable> query = startQuery();

        timed(Duration.ofSeconds(4), client::shutdown);

        Throwable t = query.get(2, TimeUnit.SECONDS);
        assertInstanceOf(StatusRuntimeException.class, t, "the hanging query is cancelled");
        assertEquals(Status.Code.UNAVAILABLE, ((StatusRuntimeException) t).getStatus().getCode(),
                "a cancelled store must look like a connection error so store and forward retries it");
    }

    @Test
    void settingsSaveFinishesWhileFactryHangs() throws Exception {
        FactryHistoryProvider provider =
                new FactryHistoryProvider(gatewayContext(), "factry", settingsFor(server.getPort()));
        factry.hang = true;
        startQuery();

        // Reconfigure (grace 1 s), cache refresh (read deadline 6 s), connection test (5 s).
        timed(Duration.ofSeconds(20), () -> provider.handleSettingsChange(settingsFor(server.getPort())));
    }

    /** Starts a query on another thread and waits until Factry has it; completes with its error. */
    private CompletableFuture<Throwable> startQuery() throws InterruptedException {
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread t = new Thread(() -> {
            try {
                client.queryTimeseries(QueryTimeseriesRequest.getDefaultInstance());
                result.complete(null);
            } catch (Throwable e) {
                result.complete(e);
            }
        });
        t.setDaemon(true);
        t.start();
        assertTrue(factry.queryArrived.await(5, TimeUnit.SECONDS), "query reached the server");
        return result;
    }

    private static FactryHistorianSettings settingsFor(int port) {
        String payload = "{\"uuid\":\"collector-uuid\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"" + port + "\"}";
        String token = "e30." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig";
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setLegacyToken(token);
        settings.setCustomCaCert(TestTls.caPem());
        return settings;
    }

    @Test
    void failedConnectionTestDropsADeadConnection() throws Exception {
        try (SilentDropProxy proxy = new SilentDropProxy(server.getPort())) {
            FactryGrpcClient viaProxy = new FactryGrpcClient(java.net.InetAddress.getLoopbackAddress().getHostAddress(), proxy.port(), "collector-uuid",
                    "token", true, false, TestTls.caPem());
            try {
                assertTrue(viaProxy.testConnection());

                // The network drops the connection without closing it: no FIN, no RST.
                proxy.dropExistingConnections();
                assertFalse(viaProxy.testConnection());

                // The next call must open a new connection instead of waiting on the dead one.
                timed(Duration.ofSeconds(3), () -> assertDoesNotThrow(viaProxy::getCollectors));
            } finally {
                viaProxy.shutdown();
            }
        }
    }

    /**
     * TCP proxy whose existing connections can be made to swallow every byte while staying open,
     * like a connection a firewall or NAT forgot. New connections still go through.
     */
    private static class SilentDropProxy implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        private final List<AtomicBoolean> links = new CopyOnWriteArrayList<>();
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();

        SilentDropProxy(int targetPort) throws Exception {
            Thread accept = new Thread(() -> {
                try {
                    while (true) {
                        Socket in = listener.accept();
                        Socket out = new Socket(java.net.InetAddress.getLoopbackAddress().getHostAddress(), targetPort);
                        sockets.add(in);
                        sockets.add(out);
                        AtomicBoolean dropped = new AtomicBoolean();
                        links.add(dropped);
                        pump(in, out, dropped);
                        pump(out, in, dropped);
                    }
                } catch (Exception closed) {
                    // listener closed
                }
            });
            accept.setDaemon(true);
            accept.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        void dropExistingConnections() {
            for (AtomicBoolean dropped : links) {
                dropped.set(true);
            }
        }

        private static void pump(Socket from, Socket to, AtomicBoolean dropped) {
            Thread t = new Thread(() -> {
                byte[] buf = new byte[8192];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        if (!dropped.get()) {
                            out.write(buf, 0, n);
                            out.flush();
                        }
                    }
                } catch (Exception closed) {
                    // socket closed
                }
            });
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void close() throws Exception {
            listener.close();
            for (Socket s : sockets) {
                s.close();
            }
        }
    }
}
