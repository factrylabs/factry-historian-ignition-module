package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.config.BasicPropertySet;
import com.inductiveautomation.ignition.common.config.PropertySet;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import com.inductiveautomation.ignition.common.sqltags.history.InterpolationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.tags.config.properties.TagHistoryProps;
import com.inductiveautomation.ignition.common.tags.config.properties.WellKnownTagProps;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Buckets without samples, read the way a chart reads them (FillMode.DERIVED): an analog tag
 * gets the straight line between its neighbours, as Ignition's own historians draw it, and a
 * discrete tag holds its value.
 */
class AnalogFillTest {

    private static final long T0 = Instant.parse("2026-11-10T10:00:00Z").toEpochMilli();
    private static final String PATH = "sys:gw:/prov:default:/tag:out";

    private final TwaFakeFactry factry = new TwaFakeFactry();
    private final MeasurementCache cache = new MeasurementCache();

    private FactryQueryEngine engine() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        cache.refresh(factry);
        FactryQueryEngine engine = new FactryQueryEngine(TwaFakeFactry.gatewayContext(), "Factry", settings,
                factry, cache, new HistorianMetrics());
        engine.now = () -> T0 + 3_600_000;
        return engine;
    }

    /** 0 at 0 s and 60 at 60 s: a tag stored once a minute, read in 12 s buckets. */
    private void minuteTag(String datatype, String ignitionDataType) {
        String u = ignitionDataType == null ? factry.addMeasurement("default/out", datatype)
                : factry.addMeasurement("default/out", datatype, ignitionDataType);
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 60_000, 60, "Good");
    }

    /** Six 12 s buckets, one column, as Ignition builds the dataset. */
    private List<Object> column(AggregationMode mode) throws Exception {
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine(), T0, T0 + 72_000, 6, mode,
                FillMode.DERIVED, "out");
        assertNull(ds.error, ds::toString);
        List<Object> out = new ArrayList<>();
        for (int r = 0; r < ds.rows.size(); r++) {
            assertEquals(T0 + r * 12_000L, ds.time(r), ds::toString);
            assertTrue(ds.qualities.get(r)[1].isGood(), ds::toString);
            out.add(((Number) ds.rows.get(r)[1]).doubleValue());
        }
        return out;
    }

    /** MinMax as the engine hands it to Ignition: per bucket a minimum, then a maximum. */
    private List<Object> minMax() {
        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        engine().query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(T0 + 72_000)
                .withTimeWindow(TimeWindow.ofMillis(12_000))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe(PATH),
                        LegacyAggregateAdapter.of(AggregationMode.MinMax), FillMode.DERIVED))
                .build(), rec);
        assertNull(rec.error);
        List<Object> out = new ArrayList<>();
        for (var p : rec.points) {
            assertTrue(p.quality().isGood());
            out.add(((Number) p.value()).doubleValue());
        }
        return out;
    }

    /** The line at each empty bucket's end, stamped at its start, as Ignition draws it. */
    private static final List<Object> LINE = List.of(0.0, 24.0, 36.0, 48.0, 60.0, 60.0);
    /** The line averaged over each empty bucket: its value at the bucket middle. */
    private static final List<Object> LINE_AVERAGE = List.of(0.0, 18.0, 30.0, 42.0, 54.0, 60.0);
    private static final List<Object> HELD = List.of(0.0, 0.0, 0.0, 0.0, 0.0, 60.0);

    private void assertDrawn(List<Object> expected) throws Exception {
        assertEquals(expected, column(AggregationMode.LastValue), "LastValue");
        assertEquals(expected == LINE ? LINE_AVERAGE : expected, column(AggregationMode.Average), "Average");
        assertEquals(expected, column(AggregationMode.SimpleAverage), "SimpleAverage");
        List<Object> pairs = new ArrayList<>();
        expected.forEach(v -> pairs.addAll(List.of(v, v)));
        assertEquals(pairs, minMax(), "MinMax");
    }

    @Test
    void aFloatTag_isDrawnOnAStraightLineThroughEmptyBuckets() throws Exception {
        minuteTag("number", "Float8");
        // Before the fix every empty bucket held 0: a staircase that lags the SQL historian's line.
        assertDrawn(LINE);
    }

    @Test
    void anOwnTagWithAutoAndFloat8_isDrawnOnALine() throws Exception {
        // A measurement without settings (older than them): the metadata says what the tag is.
        minuteTag("number", null);
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Auto",
                MeasurementCache.IGNITION_DATA_TYPE, "Float8"));
        assertDrawn(LINE);
    }

    @Test
    void aNumberMeasurementNothingIsKnownAbout_holds() throws Exception {
        // Another collector's measurement, an asset property, or one older than the settings, read
        // on another gateway: not known to be analog, so held as before the line.
        minuteTag("number", null);
        assertDrawn(HELD);
    }

    @Test
    void anotherCollectorsMeasurement_ignoresThisGatewaysMetadata() throws Exception {
        String u = factry.addMeasurementOf("another-collector", "default/out", "number");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 60_000, 60, "Good");
        // This gateway's tag of the same name says Float8, Auto; the measurement is not its own.
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Auto",
                MeasurementCache.IGNITION_DATA_TYPE, "Float8"));
        assertDrawn(HELD);
    }

    @Test
    void aStoredPlainAuto_holds() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.setSetting(u, MeasurementCache.DEADBAND_STYLE_SETTING, "Auto");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 60_000, 60, "Good");
        assertDrawn(HELD);
    }

    @Test
    void aDiscreteTag_holdsItsValue() throws Exception {
        minuteTag("number", null);
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Discrete"));
        assertDrawn(HELD);
    }

    @Test
    void anAnalogStyle_drawsAnIntegerTagOnALine() throws Exception {
        minuteTag("number", "Int4");
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Analog_Compressed"));
        assertDrawn(LINE);
    }

    @Test
    void anIntegerTagWithAutoStyle_holdsItsValue() throws Exception {
        minuteTag("number", "Int4");
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Auto"));
        assertDrawn(HELD);
    }

    @Test
    void aBooleanTag_holdsItsValue() throws Exception {
        String u = factry.addMeasurement("default/out", "boolean");
        factry.add(u, T0, false, "Good");
        factry.add(u, T0 + 60_000, true, "Good");
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Analog"));
        assertEquals(List.of(0.0, 0.0, 0.0, 0.0, 0.0, 1.0), column(AggregationMode.LastValue));
    }

    @Test
    void theStyleOnTheMeasurement_countsWhenThisGatewayHasNoMetadata() throws Exception {
        // Another gateway's tag, or this one's before Ignition sends the metadata.
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.setSetting(u, MeasurementCache.DEADBAND_STYLE_SETTING, "Discrete");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 60_000, 60, "Good");
        assertDrawn(HELD);
    }

    @Test
    void theMetadata_winsOverTheStyleOnTheMeasurement() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.setSetting(u, MeasurementCache.DEADBAND_STYLE_SETTING, "Discrete");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 60_000, 60, "Good");
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Analog_Compressed"));
        assertDrawn(LINE);
    }

    @Test
    void ignitionIsToldTheStyle_forTheAggregatesItComputesItself() {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.setSetting(u, MeasurementCache.DEADBAND_STYLE_SETTING, "Discrete");
        FactryQueryEngine engine = engine();
        var node = engine.lookupNode(QualifiedPath.parseSafe(PATH)).orElseThrow();
        PropertySet props = engine.getProcessorProperties(node).orElseThrow();
        // HistorianPointQueryExecutor turns this into FillMode.PREV, so its own interpolator holds.
        assertEquals(InterpolationMode.Discrete, props.get(TagHistoryProps.HistoricalDeadbandStyle));
    }

    /** Six 12 s LastValue buckets as read, with "Bad" for a row that is not Good. */
    private List<Object> lastValues(long end, long now) throws Exception {
        FactryQueryEngine engine = engine();
        engine.now = () -> now;
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, T0, end, (int) ((end - T0) / 12_000),
                AggregationMode.LastValue, FillMode.DERIVED, "out");
        assertNull(ds.error, ds::toString);
        List<Object> out = new ArrayList<>();
        for (int r = 0; r < ds.rows.size(); r++) {
            out.add(ds.qualities.get(r)[1].isGood() ? ((Number) ds.rows.get(r)[1]).doubleValue() : "Bad");
        }
        return out;
    }

    @Test
    void anEmptyBucketAfterABucketWithASpread_getsOneValueForMinAndMax() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 5_000, 100, "Good");
        factry.add(u, T0 + 60_000, 100, "Good");
        // The line runs from 100 at 5 s, not from the first bucket's minimum: no band of 20/100,
        // 40/100, ... that never happened.
        assertEquals(List.of(0.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0),
                minMax());
        assertEquals(List.of(100.0, 100.0, 100.0, 100.0, 100.0, 100.0), column(AggregationMode.LastValue));
    }

    @Test
    void aRampSampledOffTheBucketStarts_isDrawnStraight() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 + 11_000, 11, "Good");
        factry.add(u, T0 + 71_000, 71, "Good");
        // Ignition's own historians read 11, 24, 36, 48, 60, 71: the line at each empty bucket's
        // end. The line at the start (11, 12, 24, ...) kinks at every sample.
        assertEquals(List.of(11.0, 24.0, 36.0, 48.0, 60.0, 71.0), column(AggregationMode.LastValue));
        assertEquals(List.of(11.0, 11.0, 24.0, 24.0, 36.0, 36.0, 48.0, 48.0, 60.0, 60.0, 71.0, 71.0), minMax());
    }

    @Test
    void badTimeBetweenTwoGoodBuckets_isNotBridged() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0, 10, "Good");
        factry.add(u, T0 + 30_000, 99, "Bad");
        factry.add(u, T0 + 60_000, 60, "Good");
        // Empty, Bad, empty: the first empty bucket holds (a Bad sample follows), the rest is Bad.
        assertEquals(Arrays.asList(10.0, 10.0, "Bad", "Bad", "Bad", 60.0), lastValues(T0 + 72_000, T0 + 3_600_000));
    }

    @Test
    void theRangeStart_isOnTheLineFromTheSampleBeforeIt() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 - 24_000, 0, "Good");
        factry.add(u, T0 + 36_000, 60, "Good");
        assertEquals(Arrays.asList(36.0, 48.0, 60.0, 60.0, 60.0, 60.0), lastValues(T0 + 72_000, T0 + 3_600_000));
    }

    @Test
    void aPastRangeEnd_isOnTheLineToTheSampleAfterIt() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 + 36_000, 0, "Good");
        factry.add(u, T0 + 96_000, 60, "Good");
        assertEquals(Arrays.asList("Bad", "Bad", "Bad", 0.0, 24.0, 36.0), lastValues(T0 + 72_000, T0 + 3_600_000));
    }

    @Test
    void towardNow_theLastValueHolds() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 24_000, 24, "Good");
        // No extrapolation past the last sample, and nothing after now.
        assertEquals(Arrays.asList(0.0, 24.0, 24.0, 24.0, 24.0, "Bad"), lastValues(T0 + 72_000, T0 + 60_000));
    }

    @Test
    void aBadStretch_isNotBridged() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 - 30_000, 5, "Good");
        factry.add(u, T0 + 13_000, 7, "Good");
        factry.add(u, T0 + 26_000, 99, "Bad");
        factry.add(u, T0 + 49_000, 9, "Good");
        // On the line from 5 at -30 s to 7 at 13 s (at 12 s, the first bucket's end), Bad through
        // Bad time, and held after the last sample, since nothing follows the range.
        assertEquals(Arrays.asList(5 + 2 * 42.0 / 43, 7.0, "Bad", "Bad", 9.0, 9.0),
                lastValues(T0 + 72_000, T0 + 3_600_000));
    }

    /** Hourly Average from T0 to {@code end}, as the engine hands it to Ignition. */
    private List<Object> hourlyAverage(long end) {
        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        FactryQueryEngine engine = engine();
        engine.now = () -> T0 + 24 * HOUR;
        engine.query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(end)
                .withTimeWindow(TimeWindow.ofMinutes(60))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe(PATH),
                        LegacyAggregateAdapter.of(AggregationMode.Average), FillMode.DERIVED))
                .build(), rec);
        assertNull(rec.error);
        List<Object> out = new ArrayList<>();
        for (var p : rec.points) {
            assertTrue(p.quality().isGood());
            out.add(((Number) p.value()).doubleValue());
        }
        return out;
    }

    private static final long HOUR = 3_600_000;

    @Test
    void theAverageOfAnEmptyBucket_isTheLineAtItsMiddle() {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 4 * HOUR, 400, "Good");
        // A ramp from 0 to 400 over 4 h averages 150 in its second hour, not the 100 at its start.
        assertEquals(List.of(0.0, 150.0, 250.0, 350.0), hourlyAverage(T0 + 4 * HOUR));
        // A last bucket cut short by the range end averages the line up to that end.
        assertEquals(List.of(0.0, 150.0, 250.0, 325.0), hourlyAverage(T0 + 3 * HOUR + HOUR / 2));
    }

    @Test
    void theSampleBeforeTheRange_isReadOnce() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 - 24_000, 0, "Good");
        factry.add(u, T0 + 36_000, 60, "Good");
        lastValues(T0 + 72_000, T0 + 3_600_000);
        assertEquals(1, factry.queries.stream().filter(q -> q.getDesc() && q.getStart().getSeconds() == 0).count(), factry.queries::toString);
        factry.queries.clear();
        hourlyAverage(T0 + 4 * HOUR);
        assertEquals(1, factry.queries.stream().filter(q -> q.getDesc() && q.getStart().getSeconds() == 0).count(), factry.queries::toString);
    }

    /** The reads of the samples around empty buckets: [1 for the last before, 0 for the first after, start, end) in s. */
    private List<List<Long>> edgeReads() {
        return factry.queries.stream()
                .filter(AnalogFillTest::isEdgeRead)
                .map(q -> List.of(q.getDesc() ? 1L : 0L,
                        (com.google.protobuf.util.Timestamps.toMillis(q.getStart()) - T0) / 1000,
                        (com.google.protobuf.util.Timestamps.toMillis(q.getEnd()) - T0) / 1000))
                .toList();
    }

    /** A read of one sample per status around a run of empty buckets (not the one before the range). */
    private static boolean isEdgeRead(io.factry.historian.proto.QueryTimeseriesRequest q) {
        return !q.hasAggregation() && q.getLimit() == 1 && com.google.protobuf.util.Timestamps.toMillis(q.getStart()) > 0;
    }

    @Test
    void aFailedReadAroundEmptyBuckets_holdsInstead() throws Exception {
        minuteTag("number", "Float8");
        factry.failWhen = AnalogFillTest::isEdgeRead;
        for (Status status : List.of(Status.UNKNOWN, Status.DEADLINE_EXCEEDED)) {
            factry.failWith = status;
            // The pen draws as before the line, not Bad and not a failed query.
            assertEquals(HELD, column(AggregationMode.LastValue), status.toString());
            assertEquals(HELD, column(AggregationMode.Average), status.toString());
        }
    }

    @Test
    void aLongRangeWithOneShortGap_readsOnlyOneSampleOnEachSide() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        for (int t = 0; t <= 300; t += 6) {
            if (t < 120 || t > 144) {
                factry.add(u, T0 + t * 1000L, t, "Good");
            }
        }
        FactryQueryEngine engine = engine();
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, T0, T0 + 300_000, 25,
                AggregationMode.LastValue, FillMode.DERIVED, "out");
        assertNull(ds.error, ds::toString);
        // Buckets 120 s and 132 s are empty: the last sample of the bucket before (108 s to 120 s)
        // and the first of the bucket after (144 s to 156 s), one per status each.
        assertEquals(List.of(List.of(1L, 108L, 120L), List.of(0L, 144L, 156L)), edgeReads());
        // On the line from 114 at 114 s to 150 at 150 s, at each bucket's end.
        assertEquals(132.0, ((Number) ds.rows.get(10)[1]).doubleValue(), ds::toString);
        assertEquals(144.0, ((Number) ds.rows.get(11)[1]).doubleValue(), ds::toString);
    }

    @Test
    void runsPastTheCap_hold() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        // Empty buckets at 12 s and 60 s: two runs.
        for (int t : new int[]{0, 24, 36, 48, 72, 84}) {
            factry.add(u, T0 + t * 1000L, t, "Good");
        }
        assertEquals(List.of(0.0, 24.0, 24.0, 36.0, 48.0, 72.0, 72.0, 84.0), eightBuckets(engine()));
        assertEquals(4, edgeReads().size());

        factry.queries.clear();
        FactryQueryEngine engine = engine();
        engine.linearRunsPerPen = 1;
        assertEquals(List.of(0.0, 24.0, 24.0, 36.0, 48.0, 48.0, 72.0, 84.0), eightBuckets(engine));
        assertEquals(2, edgeReads().size());
    }

    @Test
    void theLatestSampleOnEachSide_counts() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        // Before the run a Bad sample, then a Good one: the line starts at the Good one.
        factry.add(u, T0 + 2_000, 99, "Bad");
        factry.add(u, T0 + 6_000, 6, "Good");
        // After the run a Bad sample first: Bad time follows, so the run holds.
        factry.add(u, T0 + 60_000, 99, "Bad");
        factry.add(u, T0 + 61_000, 61, "Good");
        assertEquals(List.of(6.0, 6.0, 6.0, 6.0, 6.0, 61.0), column(AggregationMode.LastValue));
    }

    private List<Object> eightBuckets(FactryQueryEngine engine) throws Exception {
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, T0, T0 + 96_000, 8,
                AggregationMode.LastValue, FillMode.DERIVED, "out");
        assertNull(ds.error, ds::toString);
        return values(ds);
    }

    private static List<Object> values(AverageHistoryQuery.Dataset ds) {
        List<Object> out = new ArrayList<>();
        for (Object[] row : ds.rows) {
            out.add(((Number) row[1]).doubleValue());
        }
        return out;
    }

    @Test
    void metadataWithoutAStyle_meansAuto() throws Exception {
        // With Include Metadata on, Ignition leaves a style at its default out.
        minuteTag("number", "Float8");
        FactryStorageEngine storage = storage(factry, cache);
        storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
        assertEquals(HELD, column(AggregationMode.LastValue));
        BasicPropertySet ps = new BasicPropertySet();
        ps.set(WellKnownTagProps.EngUnit, "m3/h");
        storage.doStoreMetadata(List.of(MetadataPoint.from(ps, QualifiedPath.parseSafe(PATH))));
        assertEquals(LINE, column(AggregationMode.LastValue));
    }

    private static MetadataPoint metadata(String tag, InterpolationMode style) {
        BasicPropertySet ps = new BasicPropertySet();
        ps.set(TagHistoryProps.HistoricalDeadbandStyle, style);
        return MetadataPoint.from(ps, QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag));
    }

    private static FactryStorageEngine storage(FactryGrpcClient client, MeasurementCache cache) {
        return new FactryStorageEngine(TwaFakeFactry.gatewayContext(), "Factry", new FactryHistorianSettings(),
                client, cache, new HistorianMetrics());
    }

    /** LastValue in 12 s buckets from T0 to {@code end}, with the given fill, as handed to Ignition. */
    private List<Object> lastValuesWith(FillMode fill, AggregationMode mode, long end) {
        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        engine().query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(end)
                .withTimeWindow(TimeWindow.ofMillis(12_000))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe(PATH), LegacyAggregateAdapter.of(mode), fill))
                .build(), rec);
        assertNull(rec.error);
        List<Object> out = new ArrayList<>();
        for (var p : rec.points) {
            assertTrue(p.quality().isGood());
            out.add(((Number) p.value()).doubleValue());
        }
        return out;
    }

    @Test
    void anExplicitFillMode_wins() {
        minuteTag("number", null);
        // Asked LINEAR: a line, though nothing says the tag is analog.
        assertEquals(LINE, lastValuesWith(FillMode.LINEAR, AggregationMode.LastValue, T0 + 72_000));
        // Asked PREV: held, though the tag is analog.
        cache.storeMetadata("default/out", Map.of(MeasurementCache.DEADBAND_STYLE, "Analog_Compressed",
                MeasurementCache.IGNITION_DATA_TYPE, "Float8"));
        assertEquals(HELD, lastValuesWith(FillMode.PREV, AggregationMode.LastValue, T0 + 72_000));
    }

    @Test
    void aRangeThatIsNotAWholeNumberOfBuckets_endsOnTheLine() {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0, 0, "Good");
        factry.add(u, T0 + 120_000, 120, "Good");
        // The last bucket is 60 s to 66 s: its LastValue is the line at 66 s, its Average at 63 s.
        assertEquals(List.of(0.0, 24.0, 36.0, 48.0, 60.0, 66.0),
                lastValuesWith(FillMode.DERIVED, AggregationMode.LastValue, T0 + 66_000));
        assertEquals(List.of(0.0, 18.0, 30.0, 42.0, 54.0, 63.0),
                lastValuesWith(FillMode.DERIVED, AggregationMode.Average, T0 + 66_000));
    }

    /** A ramp stored once a minute for an hour (value = minutes), read as LastValue in 300 points. */
    private List<Double> oneHourOfAMinuteTag(FactryQueryEngine engine) throws Exception {
        engine.now = () -> T0 + 3_700_000;
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, T0, T0 + HOUR, 300,
                AggregationMode.LastValue, FillMode.DERIVED, "out");
        assertNull(ds.error, ds::toString);
        List<Double> out = new ArrayList<>();
        for (Object[] row : ds.rows) {
            out.add(((Number) row[1]).doubleValue());
        }
        return out;
    }

    private void minuteRamp() {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        for (int m = 0; m <= 60; m++) {
            factry.add(u, T0 + m * 60_000L, m, "Good");
        }
    }

    /** Rows of 12 s buckets that are not on the ramp: a bucket with a sample has that sample, an empty one the ramp at its end. */
    private static List<Integer> offTheLine(List<Double> values) {
        List<Integer> off = new ArrayList<>();
        for (int r = 0; r < values.size(); r++) {
            double expected = r % 5 == 0 ? r / 5 : (r + 1) / 5.0;
            if (Math.abs(values.get(r) - expected) > 1e-9) {
                off.add(r);
            }
        }
        return off;
    }

    @Test
    void anHourOfAMinuteTagIn300Points_isOnTheLineThroughout() throws Exception {
        minuteRamp();
        List<Double> values = oneHourOfAMinuteTag(engine());
        assertEquals(300, values.size());
        // 60 runs of four empty buckets: every one on the line, read in one raw request.
        assertEquals(List.of(), offTheLine(values), values::toString);
        assertEquals(List.of(0.4, 0.6, 0.8, 1.0, 1.0), values.subList(1, 6));
        assertEquals(List.of(10.4, 10.6, 10.8, 11.0, 11.0), values.subList(51, 56));
        assertEquals(1, factry.queries.stream().filter(q -> !q.hasAggregation() && q.getLimit() > 1).count());
        assertEquals(List.of(), edgeReads().stream().filter(r -> r.get(0) == 1L).toList());
    }

    @Test
    void aSpanOverTheLimit_fallsBackToOneSampleReads_capped() throws Exception {
        minuteRamp();
        FactryQueryEngine engine = engine();
        engine.linearRawLimit = 2;
        engine.linearRunsPerPen = 1000;
        assertEquals(List.of(), offTheLine(oneHourOfAMinuteTag(engine)));

        engine = engine();
        engine.linearRawLimit = 2;
        List<Double> values = oneHourOfAMinuteTag(engine);
        // The default cap: the first 10 runs on the line, the rest held.
        assertEquals(List.of(), offTheLine(values.subList(0, 50)));
        assertEquals(List.of(10.0, 10.0, 10.0, 10.0, 10.0, 11.0), values.subList(50, 56));
    }
}
