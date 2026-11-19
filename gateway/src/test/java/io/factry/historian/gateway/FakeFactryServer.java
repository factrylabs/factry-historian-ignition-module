package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.codahale.metrics.MetricRegistry;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.GetAssetsRequest;
import io.factry.historian.proto.GetCollectorsRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-process stand-in for Factry Historian's gRPC service, enough for the query engine: it
 * lists measurements and answers QueryTimeseries the way Factry v8.2.3 does when grouped by
 * status. Limit and offset apply to each series (measurement and status) on its own, series come
 * back in no fixed order, and empty series are left out.
 */
final class FakeFactryServer extends HistorianGrpc.HistorianImplBase implements AutoCloseable {

    record Pt(long ts, Value value, String status) {}

    private final Map<String, Measurement> measurements = new LinkedHashMap<>();
    private final Map<String, List<Pt>> points = new LinkedHashMap<>();
    final List<QueryTimeseriesRequest> queries = new CopyOnWriteArrayList<>();
    /** Answers larger than this fail like Factry's own send limit. */
    volatile int maxAnswerBytes = Integer.MAX_VALUE;
    /** Answers every page from the start, like a Factry that does not support offset. */
    volatile boolean ignoreOffset = false;

    private final Server server;

    FakeFactryServer() throws Exception {
        // Loopback only: forPort(0) binds the wildcard address, and macOS can hand out a port that
        // another process already holds on 127.0.0.1, so the client would talk to that process.
        server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .addService(this).build().start();
    }

    int port() {
        return server.getPort();
    }

    FactryGrpcClient client() {
        return new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), port(), "collector", "token", false, false, null);
    }

    /** Adds a numeric measurement and returns its UUID. */
    String addMeasurement(String name, String datatype) {
        String uuid = "uuid-" + (measurements.size() + 1);
        measurements.put(uuid, Measurement.newBuilder().setUuid(uuid).setName(name)
                .setStatus("active").setDatatype(datatype).build());
        points.put(uuid, new ArrayList<>());
        return uuid;
    }

    void addPoint(String uuid, long ts, Value value, String status) {
        points.get(uuid).add(new Pt(ts, value, status));
    }

    void addPoint(String uuid, long ts, double value) {
        addPoint(uuid, ts, Value.newBuilder().setNumberValue(value).build(), "Good");
    }

    @Override
    public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                        StreamObserver<Measurements> observer) {
        List<Measurement> all = new ArrayList<>(measurements.values());
        int from = (int) Math.min(all.size(), request.getPagination().getOffset());
        int to = (int) Math.min(all.size(), from + request.getPagination().getLimit());
        observer.onNext(Measurements.newBuilder().addAllMeasurements(all.subList(from, to))
                .setTotal(all.size()).build());
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
    public void queryTimeseries(QueryTimeseriesRequest request,
                                StreamObserver<QueryTimeseriesResponse> observer) {
        queries.add(request);
        long start = request.hasStart() ? com.google.protobuf.util.Timestamps.toMillis(request.getStart()) : Long.MIN_VALUE;
        long end = request.hasEnd() ? com.google.protobuf.util.Timestamps.toMillis(request.getEnd()) : Long.MAX_VALUE;
        List<Series> series = new ArrayList<>();
        for (String uuid : request.getMeasurementUUIDsList()) {
            Map<String, List<Pt>> byStatus = new TreeMap<>();
            for (Pt p : points.getOrDefault(uuid, List.of())) {
                if (p.ts() >= start && p.ts() <= end) {
                    byStatus.computeIfAbsent(p.status(), k -> new ArrayList<>()).add(p);
                }
            }
            for (Map.Entry<String, List<Pt>> e : byStatus.entrySet()) {
                List<Pt> pts = e.getValue();
                pts.sort(Comparator.comparingLong(Pt::ts));
                int from = (int) Math.min(pts.size(),
                        request.hasOffset() && !ignoreOffset ? request.getOffset() : 0);
                int to = request.hasLimit() ? (int) Math.min(pts.size(), from + request.getLimit()) : pts.size();
                if (from >= to) {
                    continue;
                }
                Series.Builder sb = Series.newBuilder()
                        .setMeasurementUUID(uuid)
                        .setMeasurement(measurements.get(uuid).getName())
                        .setTags(Struct.newBuilder().putFields("status",
                                Value.newBuilder().setStringValue(e.getKey()).build()));
                for (Pt p : pts.subList(from, to)) {
                    sb.addDataPoints(SeriesPoint.newBuilder().setTimestamp(p.ts()).setValue(p.value()));
                }
                series.add(sb.build());
            }
        }
        // Factry does not keep series in request order; don't let the module rely on it.
        Collections.reverse(series);
        QueryTimeseriesResponse answer = QueryTimeseriesResponse.newBuilder().addAllSeries(series).build();
        if (answer.getSerializedSize() > maxAnswerBytes) {
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("grpc: trying to send message larger than max").asRuntimeException());
            return;
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    @Override
    public void close() throws Exception {
        server.shutdownNow().awaitTermination();
    }

    /** A GatewayContext that only offers the metric registry the query framework registers in. */
    static GatewayContext gatewayContext() {
        MetricRegistry registry = new MetricRegistry();
        return (GatewayContext) Proxy.newProxyInstance(FakeFactryServer.class.getClassLoader(),
                new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> method.getName().equals("getMetricRegistry") ? registry : null);
    }
}
