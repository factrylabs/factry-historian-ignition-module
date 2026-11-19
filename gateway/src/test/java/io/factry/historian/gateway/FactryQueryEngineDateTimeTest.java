package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.HistorianProperties;
import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.data.DataPoint;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.common.model.options.QueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.DefaultProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.QualifiedPathUtils;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A DateTime tag is stored as epoch milliseconds in a number measurement. Its history must read
 * back as Dates. Driven through the real query engine against an in-test gRPC server that answers
 * like Factry: the DateTime measurement carries the Ignition data type in its settings, as
 * MeasurementCache writes it at creation.
 */
class FactryQueryEngineDateTimeTest {

    private static final long T0 = 1_794_480_000_000L;
    private static final String START = "2026-11-12T10:00:00Z";

    private Server server;
    private FactryGrpcClient client;
    private FactryQueryEngine engine;

    /** Measurements Factry knows: one DateTime tag, one plain number tag and one String tag. */
    private static final List<Measurement> MEASUREMENTS = List.of(
            Measurement.newBuilder().setUuid("uuid-stamp").setName("default/Test/Stamp").setDatatype("number")
                    .setStatus("Active")
                    .setSettings(Struct.newBuilder().putFields(MeasurementCache.IGNITION_DATA_TYPE_SETTING,
                            Value.newBuilder().setStringValue("DateTime").build()))
                    .build(),
            Measurement.newBuilder().setUuid("uuid-sine").setName("default/Test/Sine").setDatatype("number")
                    .setStatus("Active").build(),
            Measurement.newBuilder().setUuid("uuid-name").setName("default/Test/Name").setDatatype("string")
                    .setStatus("Active").build());

    private static class FakeFactry extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getMeasurementsByFilter(GetMeasurementsByFilterRequest request,
                                            StreamObserver<Measurements> observer) {
            observer.onNext(Measurements.newBuilder().addAllMeasurements(MEASUREMENTS)
                    .setTotal(MEASUREMENTS.size()).build());
            observer.onCompleted();
        }

        /** Raw: three samples one second apart. Aggregated: one bucket, the mean of those samples. */
        @Override
        public void queryTimeseries(QueryTimeseriesRequest request, StreamObserver<QueryTimeseriesResponse> observer) {
            Series.Builder series = Series.newBuilder()
                    .setMeasurementUUID(request.getMeasurementUUIDs(0))
                    .setTags(Struct.newBuilder().putFields("status", Value.newBuilder().setStringValue("Good").build()));
            if (request.hasAggregation()) {
                double value = request.getAggregation().getName().equals("count") ? 3 : T0 + 1_000;
                series.addDataPoints(SeriesPoint.newBuilder().setTimestamp(T0)
                        .setValue(Value.newBuilder().setNumberValue(value)));
            } else {
                for (int i = 0; i < 3; i++) {
                    series.addDataPoints(SeriesPoint.newBuilder().setTimestamp(T0 + i * 1_000L)
                            .setValue(Value.newBuilder().setNumberValue(T0 + i * 1_000L)));
                }
            }
            observer.onNext(QueryTimeseriesResponse.newBuilder().addSeries(series).build());
            observer.onCompleted();
        }
    }

    /** Records what the engine hands to Ignition. */
    private static class Recorder {
        ProcessingContext<?, ?> context;
        final List<Object> values = new ArrayList<>();
        final List<QualityCode> failures = new ArrayList<>();

        /** A processor for raw and aggregated queries that writes into this recorder. */
        Object processor() {
            return Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{RawPointProcessor.class, AggregatedPointProcessor.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "onInitialize" -> {
                            context = (ProcessingContext<?, ?>) args[0];
                            yield true;
                        }
                        case "onPointAvailable" -> {
                            values.add(((DataPoint<?>) args[1]).value());
                            yield true;
                        }
                        case "onKeyFailure" -> failures.add((QualityCode) args[1]);
                        case "onError" -> throw new AssertionError("query error " + args[0]);
                        default -> null;
                    });
        }
    }

    @BeforeEach
    void start() throws Exception {
        server = NettyServerBuilder.forPort(0).addService(new FakeFactry()).build().start();
        client = new FactryGrpcClient("localhost", server.getPort(), "collector-uuid", "token", false, false, null);
        MetricRegistry registry = new MetricRegistry();
        GatewayContext context = (GatewayContext) Proxy.newProxyInstance(
                GatewayContext.class.getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> method.getName().equals("getMetricRegistry") ? registry : null);
        engine = new FactryQueryEngine(context, "Factry", new FactryHistorianSettings(), client,
                new MeasurementCache(), new HistorianMetrics());
    }

    @AfterEach
    void stop() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    private static QualifiedPath tag(String name) {
        return QualifiedPathUtils.toPathFromHistoricalString("histprov:Factry:/sys:gw:/prov:default:/tag:Test/" + name);
    }

    private Recorder raw(String tagName) {
        Recorder recorder = new Recorder();
        engine.query(RawQueryOptions.builder()
                .startTime(Instant.parse(START).toEpochMilli())
                .endTime(Instant.parse(START).plusSeconds(60).toEpochMilli())
                .addQueryKeys(List.of(new RawQueryKey(tag(tagName))))
                .build(), (RawPointProcessor) recorder.processor());
        return recorder;
    }

    private Recorder aggregated(String tagName, AggregationMode mode) {
        Recorder recorder = new Recorder();
        engine.query(AggregatedQueryOptions.builder()
                .startTime(Instant.parse(START).toEpochMilli())
                .endTime(Instant.parse(START).plusSeconds(60).toEpochMilli())
                .addQueryKeys(List.of(AggregatedQueryKey.of(tag(tagName), LegacyAggregateAdapter.of(mode))))
                .withTimeWindow(TimeWindow.ofMillis(60_000))
                .build(), (AggregatedPointProcessor) recorder.processor());
        return recorder;
    }

    /** The data type Ignition gives the result column of the query's only key. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Optional<DataType> columnType(Recorder recorder) {
        Map<?, ?> props = ((DefaultProcessingContext) recorder.context).properties();
        assertEquals(1, props.size());
        return ((ProcessingContext) recorder.context).getContextValue(
                (QueryKey) props.keySet().iterator().next(), HistorianProperties.DATA_TYPE);
    }

    @Test
    void rawHistoryOfADateTimeTagIsDates() {
        Recorder r = raw("Stamp");

        assertEquals(Optional.of(DataType.DateTime), columnType(r));
        assertEquals(List.of(new Date(T0), new Date(T0 + 1_000), new Date(T0 + 2_000)), r.values);
    }

    @Test
    void rawHistoryOfANumberTagStaysNumbers() {
        Recorder r = raw("Sine");

        assertEquals(Optional.of(DataType.Float8), columnType(r));
        assertEquals(List.of((double) T0, (double) T0 + 1_000, (double) T0 + 2_000), r.values);
    }

    @Test
    void lastValueAndAverageOfADateTimeTagAreDates() {
        for (AggregationMode mode : List.of(AggregationMode.LastValue, AggregationMode.Average,
                AggregationMode.Maximum)) {
            Recorder r = aggregated("Stamp", mode);

            assertEquals(Optional.of(DataType.DateTime), columnType(r), mode.name());
            assertEquals(List.of(new Date(T0 + 1_000)), r.values, mode.name());
        }
    }

    @Test
    void countOfADateTimeTagIsANumberInANumberColumn() {
        Recorder r = aggregated("Stamp", AggregationMode.Count);

        // Ignition coerces each value to the column type; a DateTime column would turn 3 into a Date.
        assertEquals(Optional.of(DataType.Float8), columnType(r));
        assertEquals(List.of(3.0), r.values);
    }

    @Test
    void countOfAStringTagIsANumberInANumberColumn() {
        Recorder r = aggregated("Name", AggregationMode.Count);

        // Ignition types a Count column after the tag, so a String column would turn 3 into "3".
        assertEquals(Optional.of(DataType.Float8), columnType(r));
        assertEquals(List.of(3.0), r.values);
    }

    @Test
    void sumOfADateTimeTagIsBadUnsupported() {
        Recorder r = aggregated("Stamp", AggregationMode.Sum);

        assertEquals(List.of(), r.values);
        assertEquals(List.of(QualityCode.Bad_Unsupported), r.failures);
    }
}
