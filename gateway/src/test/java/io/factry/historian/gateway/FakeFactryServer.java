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
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
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
    /** QueryTimeseries calls that include one of these measurements fail. */
    final java.util.Set<String> failingMeasurements = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** QueryTimeseries calls past the first page (offset above 0) that include one of these fail. */
    final java.util.Set<String> failingPastFirstPage = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** When set, every QueryTimeseries call fails with this status, like a Factry that is down or hangs. */
    volatile Status queryFailure = null;
    /** Answers every page from the start, like a Factry that does not support offset. */
    volatile boolean ignoreOffset = false;
    /** The authorization header of every call, in order. */
    final List<String> authorizations = new CopyOnWriteArrayList<>();
    /** When set, calls with another token fail UNAUTHENTICATED, as Factry answers a revoked token. */
    volatile String acceptedToken = null;

    private final Server server;

    FakeFactryServer() throws Exception {
        this(null);
    }

    /**
     * @param certificate null for plain text, else the name of a server certificate in
     *                    {@link TestTls}
     */
    FakeFactryServer(String certificate) throws Exception {
        // Loopback only: forPort(0) binds the wildcard address, and macOS can hand out a port that
        // another process already holds on 127.0.0.1, so the client would talk to that process.
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .addService(ServerInterceptors.intercept(this, recordAuthorization()));
        if (certificate != null) {
            builder.sslContext(TestTls.server(certificate));
        }
        server = builder.build().start();
    }

    private ServerInterceptor recordAuthorization() {
        Metadata.Key<String> key = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        return new ServerInterceptor() {
            @Override
            public <Q, A> ServerCall.Listener<Q> interceptCall(ServerCall<Q, A> call, Metadata headers,
                                                               ServerCallHandler<Q, A> next) {
                String authorization = String.valueOf(headers.get(key));
                authorizations.add(authorization);
                String accepted = acceptedToken;
                if (accepted != null && !authorization.equals("Bearer " + accepted)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("invalid auth token"), new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return next.startCall(call, headers);
            }
        };
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

    /** RegisterCollector calls, then the "status" of every UpdateCollectorState, in order. */
    final List<String> registrations = new CopyOnWriteArrayList<>();

    @Override
    public void registerCollector(io.factry.historian.proto.RegisterCollectorSchema request,
                                  StreamObserver<io.factry.historian.proto.Collector> observer) {
        registrations.add("register");
        observer.onNext(io.factry.historian.proto.Collector.newBuilder().setUuid("collector").build());
        observer.onCompleted();
    }

    @Override
    public void updateCollectorState(Struct request, StreamObserver<com.google.protobuf.Empty> observer) {
        registrations.add(request.getFieldsOrDefault("status", Value.getDefaultInstance()).getStringValue());
        observer.onNext(com.google.protobuf.Empty.getDefaultInstance());
        observer.onCompleted();
    }

    /** When set, GetTimeseriesDatabases lists this one database, so a cache reload also asks the REST API. */
    volatile String database = null;

    @Override
    public void getTimeseriesDatabases(io.factry.historian.proto.GetTimeseriesDatabasesRequest request,
                                       StreamObserver<io.factry.historian.proto.TimeseriesDatabases> observer) {
        if (database == null) {
            observer.onError(Status.UNIMPLEMENTED.asRuntimeException());
            return;
        }
        observer.onNext(io.factry.historian.proto.TimeseriesDatabases.newBuilder()
                .addTimeseriesDatabases(io.factry.historian.proto.TimeseriesDatabase.newBuilder()
                        .setUuid(database).setName(database).setStatus("Active"))
                .build());
        observer.onCompleted();
    }

    /** The connection check. */
    @Override
    public void getCollector(com.google.protobuf.Empty request, StreamObserver<io.factry.historian.proto.Collector> observer) {
        observer.onNext(io.factry.historian.proto.Collector.newBuilder().setUuid("collector").build());
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
        if (queryFailure != null) {
            observer.onError(queryFailure.asRuntimeException());
            return;
        }
        if (request.getMeasurementUUIDsList().stream().anyMatch(failingMeasurements::contains)
                || request.getOffset() > 0
                && request.getMeasurementUUIDsList().stream().anyMatch(failingPastFirstPage::contains)) {
            observer.onError(Status.INTERNAL.withDescription("query failed").asRuntimeException());
            return;
        }
        long start = request.hasStart() ? com.google.protobuf.util.Timestamps.toMillis(request.getStart()) : Long.MIN_VALUE;
        long end = request.hasEnd() ? com.google.protobuf.util.Timestamps.toMillis(request.getEnd()) : Long.MAX_VALUE;
        List<Series> series = new ArrayList<>();
        for (String uuid : request.getMeasurementUUIDsList()) {
            Map<String, List<Pt>> byStatus = new TreeMap<>();
            for (Pt p : points.getOrDefault(uuid, List.of())) {
                var tf = request.getTags().getFieldsMap().get("status");
                if (tf != null && !tf.getStringValue().equals(p.status())) continue;
                // Factry's end is exclusive (checked live on 8.2.3).
                if (p.ts() >= start && p.ts() < end) {
                    String grp = request.getGroupByList().contains("status") ? p.status() : "";
                    byStatus.computeIfAbsent(grp, k -> new ArrayList<>()).add(p);
                }
            }
            for (Map.Entry<String, List<Pt>> e : byStatus.entrySet()) {
                List<Pt> pts = e.getValue();
                pts.sort(Comparator.comparingLong(Pt::ts));
                if (request.getDesc()) { pts = new ArrayList<>(pts); Collections.reverse(pts); }
                if (request.hasAggregation()) {
                    pts = aggregate(pts, request.getAggregation().getName(),
                            periodMillis(request.getAggregation().getPeriod()), e.getKey());
                }
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

    /**
     * Buckets aligned to the epoch, like InfluxQL's GROUP BY time(), stamped with the bucket start,
     * empty buckets left out (fill none). Only the functions the tests use.
     */
    private static List<Pt> aggregate(List<Pt> pts, String function, long period, String status) {
        Map<Long, List<Double>> buckets = new TreeMap<>();
        for (Pt p : pts) {
            long bucket = Math.floorDiv(p.ts(), period) * period;
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(p.value().getNumberValue());
        }
        List<Pt> out = new ArrayList<>();
        for (Map.Entry<Long, List<Double>> b : buckets.entrySet()) {
            var stats = b.getValue().stream().mapToDouble(Double::doubleValue).summaryStatistics();
            double v = switch (function) {
                case "min" -> stats.getMin();
                case "max" -> stats.getMax();
                case "count" -> stats.getCount();
                case "sum" -> stats.getSum();
                case "last" -> b.getValue().get(b.getValue().size() - 1);
                default -> stats.getAverage();
            };
            out.add(new Pt(b.getKey(), Value.newBuilder().setNumberValue(v).build(), status));
        }
        return out;
    }

    /** Go duration strings as the module sends them: "1h30m", "10s", "250ms". */
    static long periodMillis(String period) {
        var m = java.util.regex.Pattern.compile("(\\d+)(ms|h|m|s)").matcher(period);
        long total = 0;
        while (m.find()) {
            long n = Long.parseLong(m.group(1));
            total += switch (m.group(2)) {
                case "h" -> n * 3_600_000L;
                case "m" -> n * 60_000L;
                case "s" -> n * 1_000L;
                default -> n;
            };
        }
        return total;
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
