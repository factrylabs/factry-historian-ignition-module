package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Aggregates other than Average, read the way Ignition reads them (HistorianQueryInterface and
 * HistoryWriter), against a fake Factry. Every bucket must get a point: Ignition fills a bucket
 * without one with a neighbouring value marked Good.
 */
class FactryQueryEngineBucketTest {

    private static final long T0 = Instant.parse("2026-11-10T10:00:00Z").toEpochMilli();
    /** A value that reads as a Bad row. */
    private static final Object BAD = "Bad";

    private final TwaFakeFactry factry = new TwaFakeFactry();
    /** Whether the tag's Deadband Style is Discrete, as Ignition sends it in the metadata. */
    private boolean discrete;

    private FactryQueryEngine engine(long now) {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(factry);
        if (discrete) {
            cache.storeMetadata("default/out", java.util.Map.of(MeasurementCache.DEADBAND_STYLE, "Discrete"));
        }
        FactryQueryEngine engine = new FactryQueryEngine(TwaFakeFactry.gatewayContext(), "Factry", settings,
                factry, cache, new HistorianMetrics());
        engine.now = () -> now;
        return engine;
    }

    /** Six 10 s buckets from 10:00:00, read an hour later unless {@code now} says otherwise. */
    private AverageHistoryQuery.Dataset read(AggregationMode mode, FillMode fill, long now) throws Exception {
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine(now), T0, T0 + 60_000, 6, mode, fill, "out");
        assertNull(ds.error, ds::toString);
        return ds;
    }

    private AverageHistoryQuery.Dataset read(AggregationMode mode) throws Exception {
        return read(mode, FillMode.NONE, T0 + 3_600_000);
    }

    /** The column per row, with {@link #BAD} for a row whose quality is not Good. */
    private static List<Object> column(AverageHistoryQuery.Dataset ds) {
        List<Object> out = new ArrayList<>();
        for (int r = 0; r < ds.rows.size(); r++) {
            out.add(ds.qualities.get(r)[1].isGood() ? ds.rows.get(r)[1] : BAD);
        }
        return out;
    }

    private static void assertColumn(AverageHistoryQuery.Dataset ds, Object... expected) {
        assertEquals(Arrays.asList(expected), column(ds), ds::toString);
        for (int r = 0; r < ds.rows.size(); r++) {
            assertEquals(T0 + r * 10_000L, ds.time(r), ds::toString);
        }
    }

    @Test
    void aBadStretchAndTheTimeBeforeTheFirstSample_readAsBad() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 15_000, 7, "Good");
        factry.add(u, T0 + 27_000, 99, "Bad");
        factry.add(u, T0 + 47_000, 8, "Good");

        // Before the fix Ignition filled the missing buckets: 7, 7, 8, 8, 8, 8, all Good.
        for (AggregationMode mode : List.of(AggregationMode.LastValue, AggregationMode.Minimum,
                AggregationMode.Maximum, AggregationMode.SimpleAverage)) {
            assertColumn(read(mode), BAD, 7.0, BAD, BAD, 8.0, 8.0);
        }
        assertColumn(read(AggregationMode.LastValue, FillMode.DERIVED, T0 + 3_600_000), BAD, 7.0, BAD, BAD, 8.0, 8.0);
        // An empty bucket has no sum, but a held value has no spread.
        assertColumn(read(AggregationMode.Sum), BAD, 7.0, BAD, BAD, 8.0, BAD);
        assertColumn(read(AggregationMode.Range), BAD, 0.0, BAD, BAD, 0.0, 0.0);
    }

    @Test
    void aTagStoredOnChange_holdsItsValueThroughEmptyBuckets() throws Exception {
        String u = factry.addMeasurement("default/out", "number", "Float8");
        factry.add(u, T0 - 30_000, 5, "Good");
        factry.add(u, T0 + 5_000, 7, "Good");
        factry.add(u, T0 + 45_000, 8, "Good");

        // A Discrete tag holds its value. Before the fix: 7, 8, 8, 8, 8, 8 (Ignition filled the
        // empty buckets with the next value).
        discrete = true;
        for (AggregationMode mode : List.of(AggregationMode.LastValue, AggregationMode.Minimum,
                AggregationMode.Maximum, AggregationMode.SimpleAverage)) {
            assertColumn(read(mode), 7.0, 7.0, 7.0, 7.0, 8.0, 8.0);
        }
        assertColumn(read(AggregationMode.LastValue, FillMode.DERIVED, T0 + 3_600_000), 7.0, 7.0, 7.0, 7.0, 8.0, 8.0);
        // An analog one (a Float8 tag with Auto) is drawn on the line from 7 at 5 s to 8 at 45 s,
        // at each empty bucket's end, as Ignition's own historians do (#83). The last bucket
        // holds: nothing follows the range.
        discrete = false;
        assertColumn(read(AggregationMode.LastValue, FillMode.DERIVED, T0 + 3_600_000), 7.0, 7.375, 7.625, 7.875, 8.0, 8.0);
    }

    @Test
    void aValueFromBeforeTheRange_fillsTheFirstBuckets() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 - 30_000, 5, "Good");
        factry.add(u, T0 + 25_000, 6, "Good");

        assertColumn(read(AggregationMode.LastValue), 5.0, 5.0, 6.0, 6.0, 6.0, 6.0);
    }

    @Test
    void aBadValueFromBeforeTheRange_isBadUntilTheNextGoodSample() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 - 30_000, 5, "Bad");
        factry.add(u, T0 + 25_000, 6, "Good");

        assertColumn(read(AggregationMode.Maximum), BAD, BAD, 6.0, 6.0, 6.0, 6.0);
    }

    @Test
    void afterAMixedBucket_theSampleBeforeTheNextBucketSaysWhatIsHeld() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 2_000, 7, "Good");
        factry.add(u, T0 + 5_000, 99, "Bad");
        factry.add(u, T0 + 32_000, 4, "Good");
        factry.add(u, T0 + 33_000, 99, "Bad");
        factry.add(u, T0 + 35_000, 9, "Good");

        // A bucket with Good then Bad samples keeps its last Good value; the empty buckets after
        // it hold the status of its last sample.
        assertColumn(read(AggregationMode.LastValue), 7.0, BAD, BAD, 9.0, 9.0, 9.0);
    }

    @Test
    void bucketsAfterNow_areBad() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 5_000, 7, "Good");

        assertColumn(read(AggregationMode.LastValue, FillMode.NONE, T0 + 25_000), 7.0, 7.0, 7.0, BAD, BAD, BAD);
    }

    @Test
    void aStringTagHoldsItsText() throws Exception {
        String u = factry.addMeasurement("default/out", "string");
        factry.add(u, T0 + 5_000, "run", "Good");
        factry.add(u, T0 + 45_000, "stop", "Good");

        assertColumn(read(AggregationMode.LastValue), "run", "run", "run", "run", "stop", "stop");
    }

    @Test
    void aDateTimeTagHoldsADate() throws Exception {
        String u = factry.addDateTimeMeasurement("default/out");
        factry.add(u, T0 + 5_000, 1_000_000, "Good");
        factry.add(u, T0 + 45_000, 2_000_000, "Good");

        Date d1 = new Date(1_000_000);
        Date d2 = new Date(2_000_000);
        assertColumn(read(AggregationMode.LastValue), d1, d1, d1, d1, d2, d2);
    }

    @Test
    void minMax_givesEveryBucketAMinimumAndAMaximum() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 15_000, 7, "Good");
        factry.add(u, T0 + 16_000, 3, "Good");
        factry.add(u, T0 + 27_000, 99, "Bad");
        factry.add(u, T0 + 41_000, 5, "Good");

        FactryQueryEngineAverageTest.Recorder rec = aggregated(AggregationMode.MinMax, T0 + 50_000);

        // Per bucket a minimum, then a maximum: none yet, 3 and 7, Bad, Bad, 5 and 5 held, after now.
        assertEquals(Arrays.asList(BAD, BAD, 3.0, 7.0, BAD, BAD, BAD, BAD, 5.0, 5.0, BAD, BAD), values(rec));
        for (int i = 0; i < rec.points.size(); i++) {
            assertEquals(T0 + (i / 2) * 10_000L, rec.points.get(i).timestamp().toEpochMilli());
        }
    }

    /** One pen of {@code mode} over the six buckets, as the engine hands it to Ignition. */
    private FactryQueryEngineAverageTest.Recorder aggregated(AggregationMode mode, long now) {
        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        engine(now).query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(T0 + 60_000)
                .withTimeWindow(TimeWindow.ofMillis(10_000))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out"),
                        LegacyAggregateAdapter.of(mode)))
                .build(), rec);
        assertNull(rec.error);
        return rec;
    }

    /** Each point's value, with {@link #BAD} for a Bad point; a Bad point must have no value. */
    private static List<Object> values(FactryQueryEngineAverageTest.Recorder rec) {
        List<Object> out = new ArrayList<>();
        for (var p : rec.points) {
            if (p.quality().isGood()) {
                out.add(p.value());
            } else {
                assertNull(p.value());
                out.add(BAD);
            }
        }
        return out;
    }

    @Test
    void badBuckets_streamToThePowerChartsJson() throws Exception {
        // The Power Chart streams history as strict JSON, which throws at a NaN.
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 15_000, 7, "Good");
        factry.add(u, T0 + 27_000, 99, "Bad");
        for (AggregationMode mode : List.of(AggregationMode.LastValue, AggregationMode.MinMax, AggregationMode.Sum)) {
            java.io.StringWriter json = new java.io.StringWriter();
            List<Exception> errors = new ArrayList<>();
            com.inductiveautomation.ignition.common.StreamingDatasetJsonWriter sink =
                    new com.inductiveautomation.ignition.common.StreamingDatasetJsonWriter(
                            new com.inductiveautomation.ignition.common.gson.stream.JsonWriter(json), errors::add);

            AverageHistoryQuery.wide(sink, engine(T0 + 30_000), T0, T0 + 60_000, 6, mode, FillMode.NONE, "out");

            assertEquals(List.of(), errors, json::toString);
            assertTrue(json.toString().endsWith("}"), json::toString);
            assertTrue(!json.toString().contains("NaN"), json::toString);
        }
    }

    @Test
    void count_isZeroForABucketWithoutGoodSamples() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        for (int s = 0; s < 13; s++) {
            factry.add(u, T0 + s * 1000L, s, "Good");
        }
        factry.add(u, T0 + 25_000, 99, "Bad");
        for (int s = 40; s < 60; s++) {
            factry.add(u, T0 + s * 1000L, s, "Good");
        }

        // Before the fix Ignition copied a neighbour's count into the empty buckets: 10, 3, 10, 10, 10, 10.
        assertColumn(read(AggregationMode.Count), 10.0, 3.0, 0.0, 0.0, 10.0, 10.0);
        assertColumn(read(AggregationMode.Count, FillMode.DERIVED, T0 + 3_600_000), 10.0, 3.0, 0.0, 0.0, 10.0, 10.0);
    }

    @Test
    void count_withoutAnySamples_isZero_andBadFromNowOn() throws Exception {
        factry.addMeasurement("default/out", "number");

        assertColumn(read(AggregationMode.Count, FillMode.NONE, T0 + 30_000), 0.0, 0.0, 0.0, BAD, BAD, BAD);
    }

    @Test
    void count_addsUpSeveralGoodStatusesInOneBucket() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 3, "Good");
        factry.add(u, T0 + 2_000, 5, "GoodLocalOverride");
        factry.add(u, T0 + 3_000, 4, "Good");
        factry.add(u, T0 + 4_000, 4, "Bad");

        FactryQueryEngineAverageTest.Recorder rec = aggregated(AggregationMode.Count, T0 + 3_600_000);

        assertEquals(Arrays.asList(3.0, 0.0, 0.0, 0.0, 0.0, 0.0), values(rec));
    }

    @Test
    void severalGoodStatusesInOneBucket_giveOnePoint() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 3, "Good");
        factry.add(u, T0 + 2_000, 5, "GoodLocalOverride");
        factry.add(u, T0 + 3_000, 4, "Good");

        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        engine(T0 + 3_600_000).query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(T0 + 10_000)
                .withTimeWindow(TimeWindow.ofMillis(10_000))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out"),
                        LegacyAggregateAdapter.of(AggregationMode.Maximum)))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out"),
                        LegacyAggregateAdapter.of(AggregationMode.Minimum)))
                .build(), rec);

        assertNull(rec.error);
        assertEquals(List.of(5.0, 3.0), rec.points.stream().map(p -> ((Number) p.value()).doubleValue()).toList());
        assertTrue(rec.points.stream().allMatch(p -> p.quality() == QualityCode.Good));
    }

    @Test
    void lastValue_withSeveralGoodStatusesInOneBucket_isTheLatestGoodSample() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 3, "Good");
        factry.add(u, T0 + 2_000, 5, "GoodLocalOverride");
        factry.add(u, T0 + 3_000, 4, "Good");

        FactryQueryEngineAverageTest.Recorder rec = aggregated(AggregationMode.LastValue, T0 + 3_600_000);

        // Factry's last per status says 4 (Good) and 5 (GoodLocalOverride), not which came last.
        assertEquals(Arrays.asList(4.0, 4.0, 4.0, 4.0, 4.0, 4.0), values(rec));
    }

    @Test
    void anAggregateThatCannotBeCombinedOverSeveralGoodStatuses_isUncertain() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 3, "Good");
        factry.add(u, T0 + 2_000, 5, "GoodLocalOverride");
        factry.add(u, T0 + 3_000, 4, "Good");
        factry.add(u, T0 + 11_000, 6, "Good");

        for (AggregationMode mode : List.of(AggregationMode.SimpleAverage, AggregationMode.Range,
                AggregationMode.Variance, AggregationMode.StdDev)) {
            FactryQueryEngineAverageTest.Recorder rec = aggregated(mode, T0 + 3_600_000);

            // The mean of the means (3.5) and the largest spread (1) look right but are not.
            assertEquals(QualityCode.Uncertain, rec.points.get(0).quality(), mode.name());
            assertEquals(QualityCode.Good, rec.points.get(1).quality(), mode.name());
        }
    }

    @Test
    void pastTheReadCap_anEmptyBucketAfterABucketEndingBad_isBad() {
        String u = factry.addMeasurement("default/out", "number");
        // 12 buckets with Good then Bad, each followed by an empty one: 12 reads, cap 10.
        for (int i = 0; i < 12; i++) {
            factry.add(u, T0 + i * 20_000L + 1_000, i, "Good");
            factry.add(u, T0 + i * 20_000L + 5_000, 99, "Bad");
        }

        FactryQueryEngineAverageTest.Recorder rec = new FactryQueryEngineAverageTest.Recorder();
        engine(T0 + 3_600_000).query(AggregatedQueryOptions.builder()
                .startTime(T0).endTime(T0 + 240_000)
                .withTimeWindow(TimeWindow.ofMillis(10_000))
                .addQueryKey(AggregatedQueryKey.of(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out"),
                        LegacyAggregateAdapter.of(AggregationMode.LastValue)))
                .build(), rec);

        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            expected.add((double) i);
            expected.add(BAD);
        }
        assertEquals(expected, values(rec));
    }

    @Test
    void aStringTag_hasOnlyLastValueAndCount() throws Exception {
        // Factry 8.2.3 refuses every other aggregate on a string measurement (checked live).
        String u = factry.addMeasurement("default/out", "string");
        factry.add(u, T0 + 5_000, "run", "Good");

        for (AggregationMode mode : List.of(AggregationMode.Average, AggregationMode.SimpleAverage,
                AggregationMode.Sum, AggregationMode.Minimum, AggregationMode.MinMax, AggregationMode.Range)) {
            AverageHistoryQuery.Dataset ds = read(mode);
            for (QualityCode[] q : ds.qualities) {
                assertEquals(QualityCode.Bad_Unsupported.getCode(), q[1].getCode(), () -> mode + "\n" + ds);
            }
        }
        assertColumn(read(AggregationMode.LastValue), "run", "run", "run", "run", "run", "run");
    }

    @Test
    void lastValue_withSeveralGoodStatuses_ignoresSamplesAfterNow() {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 3, "Good");
        factry.add(u, T0 + 2_000, 5, "GoodLocalOverride");
        // Stamped in the future, as a collector with a clock ahead can store it.
        factry.add(u, T0 + 8_000, 7, "Good");

        FactryQueryEngineAverageTest.Recorder rec = aggregated(AggregationMode.LastValue, T0 + 5_000);

        assertEquals(Arrays.asList(5.0, BAD, BAD, BAD, BAD, BAD), values(rec));
    }

    @Test
    void varianceAndStdDev_divideByTheNumberOfSamples_asIgnitionDoes() throws Exception {
        // Ignition's VarianceAggregate is the population variance; Factry's stddev is the sample
        // one, and Factry has no variance at all (every Variance pen read Bad_Failure before).
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 1_000, 1, "Good");
        factry.add(u, T0 + 2_000, 2, "Good");
        factry.add(u, T0 + 3_000, 3, "Good");
        factry.add(u, T0 + 11_000, 5, "Good");

        for (AggregationMode mode : List.of(AggregationMode.Variance, AggregationMode.StdDev)) {
            AverageHistoryQuery.Dataset ds = read(mode);
            double expected = mode == AggregationMode.Variance ? 2.0 / 3 : Math.sqrt(2.0 / 3);

            assertEquals(expected, ((Number) ds.rows.get(0)[1]).doubleValue(), 1e-9, () -> mode + "\n" + ds);
            assertTrue(ds.qualities.get(0)[1].isGood(), ds::toString);
            // One sample: Factry has no value, Ignition gives 0.
            assertEquals(0.0, ((Number) ds.rows.get(1)[1]).doubleValue(), 1e-9, () -> mode + "\n" + ds);
            assertTrue(ds.qualities.get(1)[1].isGood(), ds::toString);
        }
    }
}
