package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Timestamps;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.GetAssetsRequest;
import io.factry.historian.proto.GetCollectorsRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.HistorianInfo;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.MigrationRequest;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-process stand-in for Factry Historian's gRPC service, enough for the measurement cache:
 * it lists measurements (paged, filtered by collector), reports its version and answers
 * MigrateSettings the way Factry v8.2.3 and v8.3.1 do. The module talks to it through a real
 * {@link FactryGrpcClient}.
 */
final class FakeFactryServer extends HistorianGrpc.HistorianImplBase implements AutoCloseable {

    private final Map<String, Measurement> measurements = new ConcurrentHashMap<>();
    private final Server server;

    FakeFactryServer() throws Exception {
        // Loopback only: forPort(0) binds the wildcard address, and macOS can hand out a port that
        // another process already holds on 127.0.0.1, so the client would talk to that process.
        server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .addService(this).build().start();
    }

    FactryGrpcClient client() {
        return new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(),
                "collector", "token", false, false, null);
    }

    /** Adds an Active measurement of {@code collector} with these settings and returns its UUID. */
    String addMeasurement(String name, String datatype, String collector, Struct settings) {
        String uuid = "uuid-" + (measurements.size() + 1);
        measurements.put(uuid, Measurement.newBuilder().setUuid(uuid).setName(name).setStatus("active")
                .setDatatype(datatype).setCollectorUUID(collector).setSettings(settings).build());
        return uuid;
    }

    Measurement measurement(String uuid) {
        return measurements.get(uuid);
    }

    /** Sets a measurement's status, as a change in Factry does. */
    void change(String uuid, String status) {
        measurements.compute(uuid, (k, m) -> m.toBuilder().setStatus(status).build());
    }

    /** Changes a measurement's settings as an operator in Factry would. */
    void editSettings(String uuid, Struct settings) {
        measurements.compute(uuid, (k, m) -> m.toBuilder().setSettings(settings)
                .setUpdatedAt(Timestamps.fromMillis(System.currentTimeMillis())).build());
    }

    /** Measurements GetMeasurementsByFilter leaves out, though they exist. */
    final Set<String> hiddenFromListing = ConcurrentHashMap.newKeySet();

    /** When set, GetMeasurementsByFilter fails with this status. */
    volatile Status filterFailure = null;

    /** Every GetMeasurementsByFilter call, in order. */
    final List<GetMeasurementsByFilterRequest> filterRequests = new CopyOnWriteArrayList<>();

    @Override
    public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                        StreamObserver<Measurements> observer) {
        filterRequests.add(request);
        if (filterFailure != null) {
            observer.onError(filterFailure.asRuntimeException());
            return;
        }
        // As Factry 8.3.1 matches it (checked live): a full UUID exactly, else a name ignoring case.
        String keyword = request.hasKeyword() ? request.getKeyword().toLowerCase(Locale.ROOT) : null;
        List<Measurement> all = new ArrayList<>(measurements.values().stream()
                // In the order they were added.
                .sorted(Comparator.comparingInt(m -> Integer.parseInt(m.getUuid().substring("uuid-".length()))))
                .filter(m -> keyword == null || m.getUuid().equalsIgnoreCase(keyword)
                        || m.getName().toLowerCase(Locale.ROOT).contains(keyword))
                .filter(m -> !hiddenFromListing.contains(m.getUuid()))
                .filter(m -> request.getCollectorUUIDsCount() == 0
                        || request.getCollectorUUIDsList().contains(m.getCollectorUUID()))
                .toList());
        int from = (int) Math.min(all.size(), request.getPagination().getOffset());
        int to = (int) Math.min(all.size(), from + request.getPagination().getLimit());
        observer.onNext(Measurements.newBuilder().addAllMeasurements(all.subList(from, to))
                .setTotal(all.size()).build());
        observer.onCompleted();
    }

    /** When set, GetInfo fails with this status. */
    volatile Status getInfoFailure = null;

    /** The version GetInfo reports. */
    volatile String version = "v8.3.1";

    @Override
    public void getInfo(Empty request, StreamObserver<HistorianInfo> observer) {
        if (getInfoFailure != null) {
            observer.onError(getInfoFailure.asRuntimeException());
            return;
        }
        observer.onNext(HistorianInfo.newBuilder().setVersion(version).setApiVersion("1.1.0").build());
        observer.onCompleted();
    }

    /** Runs inside every MigrateSettings call, before it answers; tests race the cache with it. */
    volatile Runnable onMigrate = null;

    /** Every MigrateSettings call, in order. */
    final List<MigrationRequest> migrations = new CopyOnWriteArrayList<>();

    /** When set, MigrateSettings fails with this status, as a proxy that refuses it does. */
    volatile Status migrateFailure = null;

    @Override
    public void migrateSettings(MigrationRequest request, StreamObserver<Empty> observer) {
        migrations.add(request);
        if (onMigrate != null) {
            onMigrate.run();
        }
        if (migrateFailure != null) {
            observer.onError(migrateFailure.asRuntimeException());
            return;
        }
        // Factry replaces the collector's settings with field 1, unset or not, and each
        // measurement's whole settings Struct (checked live on v8.2.3 and v8.3.1).
        collectorSettings = request.getSettings();
        for (var change : request.getMeasurementSettingsList()) {
            measurements.computeIfPresent(change.getMeasurementUUID(),
                    (k, m) -> m.toBuilder().setSettings(change.getSettings()).build());
        }
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    /** The collector's own settings; MigrateSettings replaces them with its field 1, as Factry does. */
    volatile Struct collectorSettings = Struct.getDefaultInstance();

    @Override
    public void getCollector(Empty request, StreamObserver<Collector> observer) {
        observer.onNext(Collector.newBuilder().setUuid("collector").setSettings(collectorSettings).build());
        observer.onCompleted();
    }

    @Override
    public void getCollectors(GetCollectorsRequest request, StreamObserver<Collectors> observer) {
        observer.onNext(Collectors.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void getAssets(GetAssetsRequest request, StreamObserver<Assets> observer) {
        observer.onNext(Assets.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow().awaitTermination();
    }

    /** A GatewayContext that only offers the metric registry the engines register in. */
    static GatewayContext gatewayContext() {
        MetricRegistry registry = new MetricRegistry();
        return (GatewayContext) Proxy.newProxyInstance(FakeFactryServer.class.getClassLoader(),
                new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> method.getName().equals("getMetricRegistry") ? registry : null);
    }
}
