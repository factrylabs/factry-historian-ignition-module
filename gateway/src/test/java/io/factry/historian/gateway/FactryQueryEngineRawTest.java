package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import io.factry.historian.proto.QueryTimeseriesRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Raw reads through the public query entry point, against an in-process Factry. */
class FactryQueryEngineRawTest {

    private static final long START = 1_794_268_800_000L; // 2026-11-10T00:00:00Z
    private static final long DAY = 86_400_000L;

    private FakeFactryServer factry;
    private FactryGrpcClient client;
    private FactryQueryEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        factry = new FakeFactryServer();
        client = factry.client();
    }

    @AfterEach
    void tearDown() throws Exception {
        client.shutdown();
        factry.close();
    }

    private void startEngine() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(client);
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, client, cache,
                new HistorianMetrics());
    }

    private static RawQueryKey key(String tag) {
        return new RawQueryKey(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag));
    }

    /** Records what the engine hands to Ignition. */
    static final class Recorder implements RawPointProcessor {
        final Map<RawQueryKey, List<AtomicPoint<?>>> points = new LinkedHashMap<>();
        Exception error;
        QualityCode errorCode;
        boolean completed;

        @Override
        public boolean onInitialize(ProcessingContext<RawQueryKey, DataPointType> context) {
            return true;
        }

        @Override
        public boolean onPointAvailable(RawQueryKey key, AtomicPoint<?> point) {
            points.computeIfAbsent(key, k -> new ArrayList<>()).add(point);
            return true;
        }

        @Override
        public void onKeyFailure(RawQueryKey key, QualityCode quality) {
        }

        @Override
        public void onError(QualityCode quality) {
            errorCode = quality;
        }

        @Override
        public void onError(Exception e) {
            error = e;
        }

        @Override
        public void onComplete() {
            completed = true;
        }

        int count(RawQueryKey key) {
            return points.getOrDefault(key, List.of()).size();
        }
    }

    private Recorder query(Integer returnSize, RawQueryKey... keys) {
        RawQueryOptions.Builder b = RawQueryOptions.builder().startTime(START).endTime(START + DAY);
        for (RawQueryKey k : keys) {
            b.addQueryKey(k);
        }
        if (returnSize != null) {
            b.returnSize(returnSize);
        }
        Recorder rec = new Recorder();
        try {
            engine.query(b.build(), rec);
        } catch (IllegalStateException e) {
            // A failed read also throws, so Ignition shows the error; rec.error has what onError got.
        }
        return rec;
    }

    @Test
    void dayOfSecondDataForThreePensComesBackComplete() {
        // About 5 MB in one answer: more than grpc-java's default 4 MiB message limit.
        for (String tag : List.of("a", "b", "c")) {
            String uuid = factry.addMeasurement("default/" + tag, "number");
            for (int s = 0; s < 86_400; s++) {
                factry.addPoint(uuid, START + s * 1000L, s);
            }
        }
        startEngine();

        Recorder rec = query(null, key("a"), key("b"), key("c"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        assertTrue(rec.completed);
        for (String tag : List.of("a", "b", "c")) {
            List<AtomicPoint<?>> pts = rec.points.get(key(tag));
            assertEquals(86_400, pts.size(), tag);
            assertEquals(START, pts.get(0).timestamp().toEpochMilli());
            assertEquals(START + 86_399_000L, pts.get(pts.size() - 1).timestamp().toEpochMilli());
        }
        assertTrue(factry.queries.stream().allMatch(QueryTimeseriesRequest::hasLimit),
                "every request should be paged");
    }

    @Test
    void pagesKeepEveryStatusSeries() {
        // A Bad series much shorter than the Good one: it must not stop the Good one from paging.
        String uuid = factry.addMeasurement("default/a", "number");
        for (int s = 0; s < 25_000; s++) {
            factry.addPoint(uuid, START + s * 1000L, Value.newBuilder().setNumberValue(s).build(),
                    s % 1000 == 7 ? "Bad" : "Good");
        }
        startEngine();

        Recorder rec = query(null, key("a"));

        List<AtomicPoint<?>> pts = rec.points.get(key("a"));
        assertEquals(25_000, pts.size());
        assertEquals(25, pts.stream().filter(p -> p.quality().isNotGood()).count());
        for (int i = 0; i < pts.size(); i++) {
            assertEquals(START + i * 1000L, pts.get(i).timestamp().toEpochMilli());
        }
    }

    @Test
    void oneLargePageFitsUnderFactrysMessageLimit() {
        // 200 strings of 100 kB: 20 MB in one page, more than 4 MiB, less than Factry's 64 MiB.
        String uuid = factry.addMeasurement("default/s", "string");
        String big = "x".repeat(100_000);
        for (int s = 0; s < 200; s++) {
            factry.addPoint(uuid, START + s * 1000L, Value.newBuilder().setStringValue(s + big).build(), "Good");
        }
        startEngine();

        Recorder rec = query(null, key("s"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        assertEquals(200, rec.count(key("s")));
    }

    @Test
    void pageTooLargeForFactryIsRetriedSmaller() {
        String uuid = factry.addMeasurement("default/s", "string");
        String big = "x".repeat(10_000);
        for (int s = 0; s < 500; s++) {
            factry.addPoint(uuid, START + s * 1000L, Value.newBuilder().setStringValue(s + big).build(), "Good");
        }
        factry.maxAnswerBytes = 1_000_000;
        startEngine();

        Recorder rec = query(null, key("s"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        List<AtomicPoint<?>> pts = rec.points.get(key("s"));
        assertEquals(500, pts.size());
        for (int i = 0; i < pts.size(); i++) {
            assertEquals(i + big, pts.get(i).value());
        }
    }

    @Test
    void readOverTheMaxRawPointsCapEndsWithOnErrorAndNoPoints() {
        // A dense pen and a sparse one. Cut at the cap, the dense pen would stop after a few
        // minutes while the sparse one runs on, and Ignition would repeat the dense pen's last
        // value with Good quality to the end of the range.
        // This checks what the engine hands the processor. Ignition's reader ignores onError, so
        // on its own this shows as an empty result; the error reaches the user once read failures
        // also throw (#50).
        String dense = factry.addMeasurement("default/a", "number");
        String sparse = factry.addMeasurement("default/b", "number");
        for (int s = 0; s < 600; s++) {
            factry.addPoint(dense, START + s * 1000L, s);
            if (s % 60 == 0) {
                factry.addPoint(sparse, START + s * 1000L, s);
            }
        }
        startEngine();
        engine.maxRawPoints = 200;

        Recorder rec = query(null, key("a"), key("b"));

        assertNotNull(rec.error, "a read over the cap must end with onError, not return part of the range");
        assertTrue(rec.error.getMessage().contains("query.raw.max.points"), rec.error.getMessage());
        assertTrue(rec.error.getMessage().contains("200"), rec.error.getMessage());
        assertTrue(rec.points.isEmpty(), "no partial result");
        assertFalse(rec.completed);
    }

    @Test
    void readOfExactlyTheMaxRawPointsComesBackComplete() {
        for (String tag : List.of("a", "b", "c")) {
            String uuid = factry.addMeasurement("default/" + tag, "number");
            for (int s = 0; s < 10_000; s++) {
                factry.addPoint(uuid, START + s * 1000L, s);
            }
        }
        startEngine();
        engine.maxRawPoints = 30_000;

        Recorder rec = query(null, key("a"), key("b"), key("c"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        assertTrue(rec.completed);
        for (String tag : List.of("a", "b", "c")) {
            assertEquals(10_000, rec.count(key(tag)), tag);
        }
    }

    @Test
    void factryThatIgnoresOffsetFailsTheRead() {
        String uuid = factry.addMeasurement("default/a", "number");
        for (int s = 0; s < 25_000; s++) {
            factry.addPoint(uuid, START + s * 1000L, s);
        }
        factry.ignoreOffset = true;
        startEngine();

        Recorder rec = assertTimeoutPreemptively(Duration.ofSeconds(30), () -> query(null, key("a")));

        // Only the first page is real. Returning it alone would show the pen ending early.
        // As above, this checks onError on the processor, not what Ignition shows.
        assertNotNull(rec.error, "a Factry that repeats pages must end the read with onError");
        assertTrue(rec.error.getMessage().contains("offset"), rec.error.getMessage());
        assertTrue(rec.points.isEmpty(), "no partial result");
        assertEquals(2, factry.queries.size(), "should stop at the first page that does not move on");
    }

    @Test
    void returnSizeBelowTheCapReadsManyPens() {
        // Only returnSize points over all pens are wanted, so the cap must not count every pen's
        // first returnSize points.
        for (int pen = 0; pen < 5; pen++) {
            String uuid = factry.addMeasurement("default/p" + pen, "number");
            for (int s = 0; s < 600; s++) {
                factry.addPoint(uuid, START + (s * 5L + pen) * 1000L, s);
            }
        }
        startEngine();
        engine.maxRawPoints = 300;

        Recorder rec = query(100, key("p0"), key("p1"), key("p2"), key("p3"), key("p4"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        List<Long> times = new ArrayList<>();
        rec.points.values().forEach(pts -> pts.forEach(p -> times.add(p.timestamp().toEpochMilli())));
        times.sort(null);
        assertEquals(100, times.size());
        for (int i = 0; i < times.size(); i++) {
            assertEquals(START + i * 1000L, times.get(i));
        }
        // The range itself; the other two read the values before and after it.
        assertEquals(1, factry.queries.stream()
                        .filter(q -> com.google.protobuf.util.Timestamps.toMillis(q.getStart()) == START).count(),
                "pens past the first 100 points should stop paging");
    }

    @Test
    void penRefusedAfterTheReturnSizeCutDoesNotLosePointsOfOtherPens() {
        // Pen a sets where the first returnSize points end, so b stops paging; then Factry refuses
        // a on its second page. Without a's points the first 100 reach into b's.
        String a = factry.addMeasurement("default/a", "number");
        String b = factry.addMeasurement("default/b", "number");
        String c = factry.addMeasurement("default/c", "number");
        for (int s = 1; s <= 200; s++) {
            factry.addPoint(a, START + s * 1000L, s);
        }
        for (int s = 0; s < 100; s++) {
            factry.addPoint(b, START + (1000 + s) * 1000L, s);
        }
        for (int s = 100; s < 160; s++) {
            factry.addPoint(c, START + s * 1000L, s);
        }
        factry.failingPastFirstPage.add(a);
        startEngine();
        engine.maxRawPoints = 150;

        Recorder rec = query(100, key("a"), key("b"), key("c"));

        assertNull(rec.error, () -> "read failed: " + rec.error);
        assertEquals(0, rec.count(key("a")));
        assertEquals(60, rec.count(key("c")));
        assertEquals(40, rec.count(key("b")));
    }

    @Test
    void returnSizeKeepsTheFirstPointsInTime() {
        for (String tag : List.of("a", "b")) {
            String uuid = factry.addMeasurement("default/" + tag, "number");
            for (int s = 0; s < 5_000; s++) {
                // A sparse Bad series that runs on long after the first 100 Good points.
                factry.addPoint(uuid, START + s * 1000L, Value.newBuilder().setNumberValue(s).build(),
                        s % 1000 == 7 ? "Bad" : "Good");
            }
        }
        startEngine();

        Recorder rec = query(100, key("a"), key("b"));

        // 100 points across both pens, the earliest ones: 50 seconds of each.
        for (String tag : List.of("a", "b")) {
            List<AtomicPoint<?>> pts = rec.points.get(key(tag));
            assertEquals(50, pts.size(), tag);
            assertEquals(START + 49_000L, pts.get(49).timestamp().toEpochMilli(), tag);
        }
        for (QueryTimeseriesRequest q : factry.queries) {
            assertTrue(q.getOffset() + q.getLimit() <= 100, () -> "fetched past returnSize: " + q);
        }
    }
}
