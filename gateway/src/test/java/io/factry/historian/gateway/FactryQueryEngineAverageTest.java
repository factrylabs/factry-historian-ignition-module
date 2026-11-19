package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.data.AggregatedPoint;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ignition's "Average" through the engine's public query entry point, against a fake Factry. */
class FactryQueryEngineAverageTest {

    private static final long MIN = 60_000;

    private final TwaFakeFactry factry = new TwaFakeFactry();

    private static long t(String hhmm) {
        return Instant.parse("2026-11-10T" + hhmm + ":00Z").toEpochMilli();
    }

    private FactryQueryEngine engine() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(factry);
        return new FactryQueryEngine(TwaFakeFactry.gatewayContext(), "Factry", settings, factry, cache,
                new HistorianMetrics());
    }

    private static AggregatedQueryKey key(String tag, AggregationMode mode) {
        return AggregatedQueryKey.of(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag),
                LegacyAggregateAdapter.of(mode));
    }

    /** Records what the engine hands to Ignition. */
    static final class Recorder implements AggregatedPointProcessor {
        final List<AggregatedPoint<?, ?>> points = new ArrayList<>();
        final List<QualityCode> keyFailures = new ArrayList<>();
        Exception error;

        @Override
        public boolean onInitialize(ProcessingContext<AggregatedQueryKey, DataPointType> context) {
            return true;
        }

        @Override
        public boolean onPointAvailable(AggregatedQueryKey key, AggregatedPoint<?, ?> point) {
            points.add(point);
            return true;
        }

        @Override
        public void onKeyFailure(AggregatedQueryKey key, QualityCode quality) {
            keyFailures.add(quality);
        }

        @Override
        public void onError(QualityCode quality) {
            error = new RuntimeException(String.valueOf(quality));
        }

        @Override
        public void onError(Exception e) {
            error = e;
        }

        @Override
        public void onComplete() {
        }

        /** Value per bucket start, in order. */
        /** Value per bucket start, in order, for the buckets with Good time. */
        Map<Long, Double> values() {
            Map<Long, Double> out = new LinkedHashMap<>();
            for (AggregatedPoint<?, ?> p : points) {
                if (!p.quality().isBad()) {
                    out.put(p.timestamp().toEpochMilli(), ((Number) p.value()).doubleValue());
                }
            }
            return out;
        }

        /** The bucket starts returned as Bad (no Good time). */
        List<Long> bad() {
            return points.stream().filter(p -> p.quality().isBad()).map(p -> p.timestamp().toEpochMilli()).toList();
        }

        Map<Long, QualityCode> qualities() {
            Map<Long, QualityCode> out = new LinkedHashMap<>();
            for (AggregatedPoint<?, ?> p : points) {
                out.put(p.timestamp().toEpochMilli(), p.quality());
            }
            return out;
        }
    }

    private Recorder average(FactryQueryEngine engine, String tag, long start, long end, TimeWindow window) {
        Recorder rec = new Recorder();
        engine.query(AggregatedQueryOptions.builder()
                .startTime(start).endTime(end)
                .withTimeWindow(window)
                .addQueryKey(key(tag, AggregationMode.Average))
                .build(), rec);
        assertNull(rec.error, () -> "query failed: " + rec.error);
        assertEquals(List.of(), rec.keyFailures);
        return rec;
    }

    private static void assertValues(Map<Long, Double> expected, Map<Long, Double> actual) {
        assertEquals(expected.keySet(), actual.keySet(), "bucket starts");
        for (Map.Entry<Long, Double> e : expected.entrySet()) {
            assertEquals(e.getValue(), actual.get(e.getKey()), 1e-9, "bucket " + Instant.ofEpochMilli(e.getKey()));
        }
    }

    @Test
    void valueFromBeforeTheRangeSeedsTheAverage_andAnOutageIsAGap() {
        String u = factry.addMeasurement("default/a", "number");
        factry.add(u, t("07:30"), 100, "Good");
        factry.add(u, t("08:02"), 40, "Good");
        for (String bad : List.of("08:05", "08:15", "08:25", "08:35", "08:45")) {
            factry.add(u, t(bad), 1000, "Bad");
        }
        factry.add(u, t("08:55"), 50, "Good");
        factry.add(u, t("08:57"), 80, "Good");

        Recorder rec = average(engine(), "a", t("08:00"), t("09:00"), TimeWindow.ofMinutes(10));

        // 08:00: 2 min at 100 (held from 07:30) and 3 min at 40 over 5 Good minutes.
        // 08:50: 2 min at 50 and 3 min at 80. The buckets inside the outage are Bad.
        assertValues(Map.of(t("08:00"), 64.0, t("08:50"), 68.0), rec.values());
        assertEquals(List.of(t("08:10"), t("08:20"), t("08:30"), t("08:40")), rec.bad());
    }

    @Test
    void goodSubStatusIsGood_uncertainIsNot() {
        String u = factry.addMeasurement("default/a", "number");
        factry.add(u, t("07:00"), 4, "Good");
        factry.add(u, t("08:05"), 7, "UncertainInitialValue");
        factry.add(u, t("08:08"), 10, "GoodLocalOverride");

        Recorder rec = average(engine(), "a", t("08:00"), t("08:10"), TimeWindow.ofMinutes(10));

        // 5 min at 4, the Uncertain 3 minutes left out, 2 min at 10.
        assertValues(Map.of(t("08:00"), (5 * 4 + 2 * 10) / 7.0), rec.values());
    }

    @Test
    void lastBucketStopsAtTheRangeEnd() {
        String u = factry.addMeasurement("default/a", "number");
        factry.add(u, t("07:00"), 100, "Good");
        factry.add(u, t("08:21"), 50, "Good");
        factry.add(u, t("08:23"), 0, "Bad");

        Recorder rec = average(engine(), "a", t("08:00"), t("08:25"), TimeWindow.ofMinutes(10));

        // The last bucket is 08:20 to 08:25: Good from 08:20 to 08:23 only.
        assertValues(Map.of(t("08:00"), 100.0, t("08:10"), 100.0, t("08:20"), (100 + 2 * 50) / 3.0),
                rec.values());
    }

    @Test
    void bucketsAlternatingBetweenGoodAndBad_areReadInOneRequest() {
        // 1000 one-minute buckets, each holding one sample half way: Good (value k) in even
        // buckets, Bad in odd ones. Every bucket starts in the other status, so each needs raw
        // samples; one request per bucket would be 1000 sequential calls.
        String u = factry.addMeasurement("default/a", "number");
        long start = t("00:00");
        for (int k = 0; k < 1000; k++) {
            factry.add(u, start + k * MIN + 30_000, k, k % 2 == 0 ? "Good" : "Bad");
        }

        Recorder rec = average(engine(), "a", start, start + 1000 * MIN, TimeWindow.ofMinutes(1));

        assertEquals(1, factry.rawQueries().size());
        Map<Long, Double> expected = new LinkedHashMap<>();
        for (int k = 0; k < 1000; k++) {
            // Even: Good k for its second half. Odd: Good k-1 for its first half.
            expected.put(start + k * MIN, (double) (k % 2 == 0 ? k : k - 1));
        }
        assertValues(expected, rec.values());
        assertTrue(rec.qualities().values().stream().allMatch(QualityCode::isGood));
    }

    @Test
    void aBadBlipInEveryBucket_staysWithinTheRawBudget() {
        // A day of 4 Hz data (345,600 samples, about 8 MB raw) with one Bad sample in every
        // 10-minute bucket. Reading all of those buckets in one request breaks the 4 MiB
        // message limit and the pen fails; the module must stay within its budget, read the
        // most recent buckets exactly and give the older ones the Good-only twa, as Good
        // values (the Power Chart drops values that are not Good).
        String u = factry.addMeasurement("default/a", "number");
        long start = t("00:00");
        long day = 24 * 60 * MIN;
        for (long ts = start; ts < start + day; ts += 250) {
            long inBucket = (ts - start) % (10 * MIN);
            boolean blip = inBucket == 5 * MIN;
            factry.add(u, ts, blip ? 1000 : inBucket == 5 * MIN - 250 ? 40 : 10, blip ? "Bad" : "Good");
        }
        factry.maxAnswerBytes = 4 * 1024 * 1024;

        Recorder rec = average(engine(), "a", start, start + day, TimeWindow.ofMinutes(10));

        GoodTimeAverage.Budget budget = FactryQueryEngine.RAW_BUDGET;
        assertTrue(factry.rawQueries().size() <= budget.requestsPerPen(), "raw requests");
        assertTrue(factry.rawAnswerPoints.stream().allMatch(n -> n <= budget.pointsPerRequest()),
                () -> "points per raw request: " + factry.rawAnswerPoints);
        long rawPoints = factry.rawAnswerPoints.stream().mapToLong(Integer::longValue).sum();
        assertTrue(rawPoints <= budget.pointsPerPen());

        // Read raw: the 40 counts for 250 ms of 599.75 Good seconds. Fallback: the twa over
        // Good samples holds the 40 through the Bad 250 ms as well.
        double exact = (10 * 599.5 + 40 * 0.25) / 599.75;
        double fallback = (10 * 599.5 + 40 * 0.5) / 600;
        int read = (int) (rawPoints / 2400); // samples per bucket
        assertTrue(read > 0 && read < 144, "some buckets read raw, the rest fall back: " + read);
        List<Double> values = new ArrayList<>(rec.values().values());
        assertEquals(144, values.size());
        for (int k = 0; k < 144; k++) {
            assertEquals(k >= 144 - read ? exact : fallback, values.get(k), 1e-9, "bucket " + k);
        }
        assertTrue(rec.qualities().values().stream().allMatch(QualityCode::isGood));
    }

    @Test
    void rangeEndingInTheFuture_stopsAtNow() {
        String u = factry.addMeasurement("default/a", "number");
        factry.add(u, t("07:00"), 100, "Good");
        factry.add(u, t("08:22"), 0, "Good");
        FactryQueryEngine engine = engine();
        engine.now = () -> t("08:25");

        Recorder rec = average(engine, "a", t("08:00"), t("09:00"), TimeWindow.ofMinutes(10));

        // 08:20 holds 100 for 2 minutes and 0 for 3 until now (not 8 until the bucket end).
        // No buckets after now, like Ignition's own Average.
        assertValues(Map.of(t("08:00"), 100.0, t("08:10"), 100.0, t("08:20"), 40.0), rec.values());
    }

    @Test
    void rangeStartingAfterNow_hasNoValues() {
        String u = factry.addMeasurement("default/a", "number");
        factry.add(u, t("07:00"), 100, "Good");
        FactryQueryEngine engine = engine();
        engine.now = () -> t("07:30");

        Recorder rec = average(engine, "a", t("08:00"), t("09:00"), TimeWindow.ofMinutes(10));

        assertEquals(Map.of(), rec.values());
        assertEquals(6, rec.bad().size());
    }

    @Test
    void booleanAverage_isTheFractionOfGoodTimeTrue() {
        // A run bit on for 1 minute of the hour. Ignition holds a Boolean until the next sample,
        // so its Average is 1/60; the mean of the two samples in the hour would be 0.5.
        String u = factry.addMeasurement("default/run", "boolean");
        factry.add(u, t("07:00"), false, "Good");
        factry.add(u, t("08:10"), true, "Good");
        factry.add(u, t("08:11"), false, "Good");
        factry.add(u, t("08:40"), true, "Good");
        factry.add(u, t("08:45"), false, "Bad");

        Recorder rec = average(engine(), "run", t("08:00"), t("09:00"), TimeWindow.ofMinutes(30));

        // 08:00-08:30: true 1 of 30 minutes. 08:30-09:00: true 5 of the 15 Good minutes.
        assertValues(Map.of(t("08:00"), 1 / 30.0, t("08:30"), 5 / 15.0), rec.values());
        assertTrue(rec.qualities().values().stream().allMatch(QualityCode::isGood));
    }

    @Test
    void booleanWithoutSamplesInTheRange_holdsItsValueWithoutRawReads() {
        String u = factry.addMeasurement("default/run", "boolean");
        factry.add(u, t("07:00"), true, "Good");

        Recorder rec = average(engine(), "run", t("08:00"), t("08:30"), TimeWindow.ofMinutes(10));

        assertValues(Map.of(t("08:00"), 1.0, t("08:10"), 1.0, t("08:20"), 1.0), rec.values());
        assertEquals(0, factry.rawQueries().size());
    }

    @Test
    void healthyBoolean_onlyTheBucketWhereItChangesIsReadRaw() {
        // A day of a Boolean stored every 10 s that never changes, then turns false at 23:55.
        // Only the bucket where it changes needs raw samples.
        String u = factry.addMeasurement("default/run", "boolean");
        long start = t("00:00");
        for (long ts = start - MIN; ts < start + 24 * 60 * MIN; ts += 10_000) {
            factry.add(u, ts, ts < start + (23 * 60 + 55) * MIN, "Good");
        }

        Recorder rec = average(engine(), "run", start, start + 24 * 60 * MIN, TimeWindow.ofMinutes(10));

        Map<Long, Double> expected = new LinkedHashMap<>();
        for (int k = 0; k < 144; k++) {
            expected.put(start + k * 10 * MIN, k < 143 ? 1.0 : 0.5);
        }
        assertValues(expected, rec.values());
        assertEquals(1, factry.rawQueries().size());
        assertEquals(60, factry.rawAnswerPoints.get(0)); // only the last bucket, at 10 s
    }

    @Test
    void booleanBucketBeyondTheBudget_isUncertain() {
        // 08:00-09:00 toggles every 50 ms (72,000 samples, more than one raw request may hold);
        // 09:00-10:00 is on for 6 minutes. The first falls back to the sample mean, which is
        // not a time-weighted average, so it must not pass as Good.
        String u = factry.addMeasurement("default/run", "boolean");
        for (long ts = t("08:00"); ts < t("09:00"); ts += 50) {
            factry.add(u, ts, (ts / 50) % 2 == 0, "Good");
        }
        factry.add(u, t("09:00"), false, "Good");
        factry.add(u, t("09:30"), true, "Good");
        factry.add(u, t("09:36"), false, "Good");

        Recorder rec = average(engine(), "run", t("08:00"), t("10:00"), TimeWindow.ofHours(1));

        assertValues(Map.of(t("08:00"), 0.5, t("09:00"), 0.1), rec.values());
        assertEquals(Map.of(t("08:00"), QualityCode.Uncertain, t("09:00"), QualityCode.Good), rec.qualities());
    }

    @Test
    void dateTimeAverage_isTimeWeightedAndADate() {
        // A DateTime tag is a number measurement of epoch milliseconds. Its Average goes through
        // the time-weighted path like any number and comes back as a Date.
        String u = factry.addDateTimeMeasurement("default/stamp");
        factry.add(u, t("07:00"), t("01:00"), "Good");
        factry.add(u, t("08:30"), t("03:00"), "Good");

        Recorder rec = average(engine(), "stamp", t("08:00"), t("09:00"), TimeWindow.ofHours(1));

        assertEquals(1, rec.points.size());
        assertEquals(new java.util.Date(t("02:00")), rec.points.get(0).value());
        assertEquals(1, factry.aggregateQueries("twa").size());
    }

    @Test
    void aBadBucket_readsAsBadThroughIgnitionsQueryPath_notAsTheHeldValue() throws Exception {
        // Live on 8.3.6: Ignition fills a bucket without a point with the previous value, marked
        // Good. A bucket that is Bad the whole time must read as Bad instead.
        long t0 = t("10:38") + 5_911;
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, t0, 100, "Good");
        factry.add(u, t0 + 15_000, 0, "Bad");
        factry.add(u, t0 + 35_000, 60, "Good");
        FactryQueryEngine engine = engine();
        engine.now = () -> t0 + 60_000;

        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, t0, t0 + 60_000, 6,
                AggregationMode.Average, "out");

        assertNull(ds.error, ds::toString);
        assertEquals(6, ds.rows.size(), ds::toString);
        // 05-15 Good 100; 15-25 Bad from 20; 25-35 Bad throughout; 35-45 Bad until 40.
        assertEquals(100.0, ((Number) ds.rows.get(0)[1]).doubleValue(), 1e-9, ds::toString);
        assertEquals(100.0, ((Number) ds.rows.get(1)[1]).doubleValue(), 1e-9, ds::toString);
        assertTrue(ds.qualities.get(2)[1].isBad(), ds::toString);
        assertEquals(60.0, ((Number) ds.rows.get(3)[1]).doubleValue(), 1e-9, ds::toString);
        assertTrue(ds.qualities.get(3)[1].isGood(), ds::toString);
    }

    @Test
    void beforeTheFirstSampleAndAfterNow_readAsBadThroughIgnitionsQueryPath() throws Exception {
        // Ignition fills a bucket without a point from its neighbour, marked Good: here the
        // value 100 would show before it was ever stored and after now.
        long t0 = t("10:38");
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, t0 + 20_000, 100, "Good");
        FactryQueryEngine engine = engine();
        engine.now = () -> t0 + 40_000;

        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine, t0, t0 + 60_000, 6,
                AggregationMode.Average, "out");

        assertNull(ds.error, ds::toString);
        assertEquals(6, ds.rows.size(), ds::toString);
        for (int r = 0; r < 6; r++) {
            boolean good = r == 2 || r == 3; // 10:38:20 to now (10:38:40)
            assertEquals(good, ds.qualities.get(r)[1].isGood(), "row " + r + "\n" + ds);
            Object v = ds.rows.get(r)[1];
            if (good) {
                assertEquals(100.0, ((Number) v).doubleValue(), 1e-9, ds::toString);
            }
        }
    }

    @Test
    void badBuckets_streamToThePowerChartsJson() throws Exception {
        // The Power Chart streams history through StreamingDatasetJsonWriter on a strict
        // JsonWriter, which throws at a NaN and cuts the response off.
        long t0 = t("10:38") + 5_911;
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, t0, 100, "Good");
        factry.add(u, t0 + 15_000, 0, "Bad");
        factry.add(u, t0 + 35_000, 60, "Good");
        FactryQueryEngine engine = engine();
        engine.now = () -> t0 + 40_000;
        java.io.StringWriter json = new java.io.StringWriter();
        List<Exception> errors = new ArrayList<>();
        com.inductiveautomation.ignition.common.StreamingDatasetJsonWriter sink =
                new com.inductiveautomation.ignition.common.StreamingDatasetJsonWriter(
                        new com.inductiveautomation.ignition.common.gson.stream.JsonWriter(json), errors::add);

        AverageHistoryQuery.wide(sink, engine, t0, t0 + 60_000, 6, AggregationMode.Average,
                com.inductiveautomation.ignition.common.FillMode.NONE, "out");

        assertEquals(List.of(), errors, json::toString);
        assertTrue(json.toString().endsWith("}"), json::toString);
        assertTrue(!json.toString().contains("NaN"), json::toString);
    }
}
