package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import io.grpc.Status;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Answers QueryTimeseries the way Factry v8.2.3 did in live checks, for the aggregates the
 * Average path uses: raw (grouped by status, end exclusive, limit per series, desc), {@code twa}
 * with fill "previous" (holds the value from before each bucket, weights the last bucket up to
 * the query end, leaves out buckets before the first sample, refuses group by and booleans, and
 * applies a {@code status} tag filter by exact match), and {@code last}/{@code count}/{@code mean}
 * per status with fill "none". Buckets start at the query start. The gRPC channel is never used.
 */
class TwaFakeFactry extends FactryGrpcClient {

    record Pt(long ts, Value value, String status) {}

    private final Map<String, Measurement> measurements = new LinkedHashMap<>();
    private final Map<String, List<Pt>> points = new LinkedHashMap<>();
    final List<QueryTimeseriesRequest> queries = new ArrayList<>();
    /** Points in each answer to a raw (not aggregated, unlimited) request, in order. */
    final List<Integer> rawAnswerPoints = new ArrayList<>();
    /** Answers larger than this fail like a gRPC message over the client's limit. */
    int maxAnswerBytes = Integer.MAX_VALUE;

    TwaFakeFactry() {
        super("localhost", 1, "collector", "token", false, false, null);
    }

    /** Adds a measurement ("number" or "boolean") and returns its UUID. */
    String addMeasurement(String name, String datatype) {
        return addMeasurement(Measurement.newBuilder().setName(name).setDatatype(datatype));
    }

    /** Adds a number measurement holding an Ignition DateTime tag (epoch ms), as the module creates it. */
    String addDateTimeMeasurement(String name) {
        return addMeasurement(Measurement.newBuilder().setName(name).setDatatype("number")
                .setSettings(Struct.newBuilder().putFields(MeasurementCache.IGNITION_DATA_TYPE_SETTING,
                        Value.newBuilder().setStringValue("DateTime").build())));
    }

    private String addMeasurement(Measurement.Builder m) {
        String uuid = "uuid-" + (measurements.size() + 1);
        measurements.put(uuid, m.setUuid(uuid).setStatus("Active").setCollectorUUID("collector").build());
        points.put(uuid, new ArrayList<>());
        return uuid;
    }

    void add(String uuid, long ts, double value, String status) {
        points.get(uuid).add(new Pt(ts, Value.newBuilder().setNumberValue(value).build(), status));
    }

    void add(String uuid, long ts, boolean value, String status) {
        points.get(uuid).add(new Pt(ts, Value.newBuilder().setBoolValue(value).build(), status));
    }

    /** The raw (not aggregated) requests the module made. */
    List<QueryTimeseriesRequest> rawQueries() {
        return queries.stream().filter(q -> !q.hasAggregation() && !q.hasLimit()).toList();
    }

    List<QueryTimeseriesRequest> aggregateQueries(String name) {
        return queries.stream().filter(q -> q.hasAggregation() && q.getAggregation().getName().equals(name)).toList();
    }

    @Override
    public Measurements getMeasurementsByFilter(GetMeasurementsByFilterRequest request) {
        return Measurements.newBuilder().addAllMeasurements(measurements.values())
                .setTotal(measurements.size()).build();
    }

    @Override
    public Collectors getCollectors() {
        return Collectors.getDefaultInstance();
    }

    @Override
    public Assets getAssets() {
        return Assets.getDefaultInstance();
    }

    @Override
    public QueryTimeseriesResponse queryTimeseries(QueryTimeseriesRequest request) {
        queries.add(request);
        long start = Timestamps.toMillis(request.getStart());
        long end = request.hasEnd() ? Timestamps.toMillis(request.getEnd()) : Long.MAX_VALUE;
        boolean byStatus = request.getGroupByList().contains("status");
        String statusFilter = request.hasTags() && request.getTags().containsFields("status")
                ? request.getTags().getFieldsOrThrow("status").getStringValue() : null;

        QueryTimeseriesResponse.Builder answer = QueryTimeseriesResponse.newBuilder();
        for (String uuid : request.getMeasurementUUIDsList()) {
            List<Pt> all = new ArrayList<>(points.get(uuid));
            all.removeIf(p -> statusFilter != null && !p.status().equals(statusFilter));
            all.sort(Comparator.comparingLong(Pt::ts));

            if (request.hasAggregation() && request.getAggregation().getName().equals("twa")) {
                if (byStatus) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("group by is not supported for TWA aggregation query").asRuntimeException();
                }
                if (measurements.get(uuid).getDatatype().equals("boolean")) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("unsupported datatype boolean for TWA aggregation").asRuntimeException();
                }
                if (!request.getAggregation().getFillType().equals("previous")) {
                    throw Status.UNIMPLEMENTED.withDescription("fake only models twa with fill previous")
                            .asRuntimeException();
                }
                Series.Builder s = series(uuid, null);
                long period = periodMillis(request.getAggregation().getPeriod());
                for (long b = start; b < end; b += period) {
                    Double v = twa(all, b, Math.min(b + period, end));
                    if (v != null) {
                        s.addDataPoints(point(b, Value.newBuilder().setNumberValue(v).build()));
                    }
                }
                answer.addSeries(s);
                continue;
            }

            Map<String, List<Pt>> groups = new TreeMap<>();
            for (Pt p : all) {
                if (p.ts() >= start && p.ts() < end) {
                    groups.computeIfAbsent(byStatus ? p.status() : "", k -> new ArrayList<>()).add(p);
                }
            }
            for (Map.Entry<String, List<Pt>> g : groups.entrySet()) {
                List<Pt> pts = g.getValue();
                Series.Builder s = series(uuid, byStatus ? g.getKey() : null);
                if (!request.hasAggregation()) {
                    if (request.getDesc()) {
                        pts = new ArrayList<>(pts);
                        java.util.Collections.reverse(pts);
                    }
                    int from = (int) Math.min(pts.size(), request.getOffset());
                    int to = request.hasLimit() ? (int) Math.min(pts.size(), from + request.getLimit()) : pts.size();
                    for (Pt p : pts.subList(from, to)) {
                        s.addDataPoints(point(p.ts(), p.value()));
                    }
                } else {
                    String fn = request.getAggregation().getName();
                    long period = periodMillis(request.getAggregation().getPeriod());
                    for (long b = start; b < end; b += period) {
                        long bucketStart = b;
                        long bucketEnd = Math.min(b + period, end);
                        List<Pt> in = pts.stream().filter(p -> p.ts() >= bucketStart && p.ts() < bucketEnd).toList();
                        if (in.isEmpty()) {
                            continue; // fill "none"
                        }
                        s.addDataPoints(point(b, aggregate(fn, in)));
                    }
                }
                if (s.getDataPointsCount() > 0) {
                    answer.addSeries(s);
                }
            }
        }
        QueryTimeseriesResponse built = answer.build();
        if (!request.hasAggregation() && !request.hasLimit()) {
            rawAnswerPoints.add(built.getSeriesList().stream().mapToInt(Series::getDataPointsCount).sum());
        }
        if (built.getSerializedSize() > maxAnswerBytes) {
            throw Status.RESOURCE_EXHAUSTED.withDescription("gRPC message exceeds maximum size")
                    .asRuntimeException();
        }
        return built;
    }

    private Series.Builder series(String uuid, String status) {
        Series.Builder s = Series.newBuilder().setMeasurementUUID(uuid)
                .setMeasurement(measurements.get(uuid).getName());
        if (status != null) {
            s.setTags(Struct.newBuilder().putFields("status", Value.newBuilder().setStringValue(status).build()));
        }
        return s;
    }

    private static SeriesPoint point(long ts, Value v) {
        return SeriesPoint.newBuilder().setTimestamp(ts).setValue(v).build();
    }

    /** Each value held until the next sample, over [from, to); null before the first sample. */
    private static Double twa(List<Pt> all, long from, long to) {
        Pt held = null;
        double sum = 0;
        long covered = 0;
        long t = from;
        for (Pt p : all) {
            if (p.ts() >= to) {
                break;
            }
            if (p.ts() >= from && held != null) {
                sum += num(held.value()) * (p.ts() - t);
                covered += p.ts() - t;
            }
            if (p.ts() >= from) {
                t = p.ts();
            }
            held = p;
        }
        if (held == null) {
            return null;
        }
        sum += num(held.value()) * (to - t);
        covered += to - t;
        return covered > 0 ? sum / covered : null;
    }

    private static Value aggregate(String fn, List<Pt> in) {
        switch (fn) {
            case "last":
                return in.get(in.size() - 1).value();
            case "count":
                return Value.newBuilder().setNumberValue(in.size()).build();
            case "mean":
                return Value.newBuilder().setNumberValue(
                        in.stream().mapToDouble(p -> num(p.value())).average().orElseThrow()).build();
            default:
                throw Status.UNIMPLEMENTED.withDescription("fake has no aggregate " + fn).asRuntimeException();
        }
    }

    private static double num(Value v) {
        return v.getKindCase() == Value.KindCase.BOOL_VALUE ? (v.getBoolValue() ? 1 : 0) : v.getNumberValue();
    }

    private static final Pattern DURATION = Pattern.compile("(\\d+)(ms|h|m|s)");

    /** A Go duration such as "1h", "10m", "1m30s" or "600ms". */
    static long periodMillis(String period) {
        Matcher m = DURATION.matcher(period);
        long total = 0;
        while (m.find()) {
            long n = Long.parseLong(m.group(1));
            total += switch (m.group(2)) {
                case "h" -> n * 3_600_000;
                case "m" -> n * 60_000;
                case "s" -> n * 1000;
                default -> n;
            };
        }
        if (total <= 0) {
            // Factry's InfluxDB query panics on a zero period.
            throw Status.INTERNAL.withDescription("integer divide by zero").asRuntimeException();
        }
        return total;
    }

    /** A GatewayContext that only offers the metric registry the query framework registers in. */
    static GatewayContext gatewayContext() {
        MetricRegistry registry = new MetricRegistry();
        return (GatewayContext) Proxy.newProxyInstance(TwaFakeFactry.class.getClassLoader(),
                new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> method.getName().equals("getMetricRegistry") ? registry : null);
    }
}
