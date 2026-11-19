package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import io.factry.historian.gateway.IgnitionHistoryQuery.Dataset;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failed reads through Ignition's own query path. A read that fails for every pen must end the
 * dataset with an error (what the Power Chart and clients show), not come back as an empty or
 * Bad_NotFound result that looks like a quiet or unknown tag. A pen that fails on its own is
 * Bad_Failure, and the other pens still show their data.
 */
class ReadErrorTest {

    private static final long START = 1_794_268_800_000L; // 2026-11-10T00:00:00Z
    private static final long FIVE_SECONDS = 5_000L;
    private static final long FIVE_MINUTES = 300_000L;

    private FakeFactryServer factry;
    private FactryGrpcClient client;
    private FactryQueryEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        factry = new FakeFactryServer();
        client = factry.client();
        String a = factry.addMeasurement("default/a", "number");
        String b = factry.addMeasurement("default/b", "number");
        for (int s = 0; s < 600; s++) {
            factry.addPoint(a, START + s * 1000L, s);
            factry.addPoint(b, START + s * 1000L, s);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        client.shutdown();
        factry.close();
    }

    private void startEngine(boolean measurementsKnown) {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        if (measurementsKnown) {
            cache.refresh(client);
        }
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, client, cache,
                new HistorianMetrics());
    }

    private Dataset raw(String... tags) throws Exception {
        return IgnitionHistoryQuery.wide(engine, START, START + FIVE_SECONDS, -1, null, tags);
    }

    private Dataset average(String... tags) throws Exception {
        return IgnitionHistoryQuery.wide(engine, START, START + FIVE_MINUTES, 5, AggregationMode.Average, tags);
    }

    private static void assertReadFailed(Dataset ds, String detail) {
        assertNotNull(ds.error, () -> "expected an error, got:\n" + ds);
        assertTrue(ds.error.getMessage().contains("Factry Historian read failed"), ds.error::getMessage);
        assertTrue(ds.error.getMessage().contains(detail), ds.error::getMessage);
    }

    @Test
    void rawReadWithFactryUnreachableIsAnError() throws Exception {
        startEngine(true);
        factry.close();

        assertReadFailed(raw("a", "b"), "UNAVAILABLE");
    }

    @Test
    void aggregatedReadWithFactryUnreachableIsAnError() throws Exception {
        startEngine(true);
        factry.close();

        assertReadFailed(average("a", "b"), "UNAVAILABLE");
    }

    @Test
    void factryUnreachableBeforeTheFirstLookupIsAnErrorNotNotFound() throws Exception {
        // Nothing cached yet: resolving the tags needs Factry, and that fails.
        startEngine(false);
        factry.close();

        assertReadFailed(raw("a", "b"), "UNAVAILABLE");
        assertReadFailed(average("a", "b"), "UNAVAILABLE");
    }

    /**
     * Pen a reads fine on every row; pen b reads null with Bad_Failure on every row, like an
     * unknown tag reads null with Bad_NotFound; no error for the read.
     */
    private static void assertOnlySecondPenFailed(Dataset ds) {
        assertNull(ds.error, ds::toString);
        assertTrue(ds.rows.size() > 1, ds::toString);
        for (int i = 0; i < ds.rows.size(); i++) {
            QualityCode[] q = ds.qualities.get(i);
            assertTrue(q[1].isGood(), ds::toString);
            assertNotNull(ds.rows.get(i)[1], ds::toString);
            assertTrue(q[2].is(QualityCode.Bad_Failure), ds::toString);
            // Not 0.0: a script or chart that ignores quality would take that for real data.
            assertNull(ds.rows.get(i)[2], ds::toString);
        }
    }

    @Test
    void aggregatedReadThatFactryRejectsForOnePenMarksOnlyThatPen() throws Exception {
        startEngine(true);
        factry.failingMeasurements.add("uuid-2");

        Dataset ds = average("a", "b");

        assertOnlySecondPenFailed(ds);
        assertEquals(5, ds.rows.size(), ds::toString);
        assertEquals(29.5, ((Number) ds.rows.get(0)[1]).doubleValue(), ds::toString);
    }

    @Test
    void rawReadThatFactryRejectsForOnePenMarksOnlyThatPen() throws Exception {
        startEngine(true);
        factry.failingMeasurements.add("uuid-2");

        Dataset ds = raw("a", "b");

        assertOnlySecondPenFailed(ds);
        // The same rows as pen a alone: the failed pen adds none of its own.
        assertEquals(6, ds.rows.size(), ds::toString);
        assertEquals(START, ds.time(0), ds::toString);
        assertEquals(5.0, ((Number) ds.rows.get(ds.rows.size() - 1)[1]).doubleValue(), ds::toString);
    }

    @Test
    void averageWithOnePenRejectedKeepsTheTimeWeightedPathForTheOther() throws Exception {
        // Average has its own path (Good-time twa, a point for every bucket, a Bad bucket without
        // a value). A pen Factry refuses there must still be a null Bad_Failure column, and no
        // cell may be NaN: the Power Chart writes history as strict JSON, which throws at a NaN.
        // Pen c is Bad for the whole third minute, so that bucket has no Good time.
        String c = factry.addMeasurement("default/c", "number");
        for (int s = 0; s < 300; s++) {
            factry.addPoint(c, START + s * 1000L, Value.newBuilder().setNumberValue(s).build(),
                    s >= 120 && s < 180 ? "Bad" : "Good");
        }
        startEngine(true);
        factry.failingMeasurements.add("uuid-2");

        Dataset ds = average("c", "b");

        assertTrue(factry.queries.stream().anyMatch(q -> q.hasAggregation()
                        && FactryQueryEngine.TIME_WEIGHTED_AVERAGE.equals(q.getAggregation().getName())),
                "Average should read Factry's twa");
        assertNull(ds.error, ds::toString);
        assertEquals(5, ds.rows.size(), ds::toString);
        for (int i = 0; i < ds.rows.size(); i++) {
            QualityCode[] q = ds.qualities.get(i);
            assertTrue(q[2].is(QualityCode.Bad_Failure), ds::toString);
            assertNull(ds.rows.get(i)[2], ds::toString);
            assertEquals(i != 2, q[1].isGood(), ds::toString);
            for (Object cell : ds.rows.get(i)) {
                assertFalse(cell instanceof Double d && d.isNaN(), ds::toString);
            }
        }
    }

    @Test
    void derivedFillWithOnePenRejectedMarksOnlyThatPen() throws Exception {
        // FillMode.DERIVED gives each pen its own query, so the failing pen is alone in its query.
        startEngine(true);
        factry.failingMeasurements.add("uuid-2");

        Dataset ds = IgnitionHistoryQuery.wide(engine, START, START + FIVE_MINUTES, 5, AggregationMode.Average,
                FillMode.DERIVED, "a", "b");

        assertOnlySecondPenFailed(ds);
        assertEquals(29.5, ((Number) ds.rows.get(0)[1]).doubleValue(), ds::toString);
    }

    /** Every pen reads null with Bad_Failure on every interval; no error for the read. */
    private static void assertEveryPenFailed(Dataset ds) {
        assertNull(ds.error, ds::toString);
        assertEquals(5, ds.rows.size(), ds::toString);
        for (int i = 0; i < ds.rows.size(); i++) {
            for (int c = 1; c < ds.columns.length; c++) {
                assertTrue(ds.qualities.get(i)[c].is(QualityCode.Bad_Failure), ds::toString);
                assertNull(ds.rows.get(i)[c], ds::toString);
            }
        }
    }

    @Test
    void aggregatedReadWithItsOnlyPenRejectedIsBadFailureNotAnError() throws Exception {
        // A chart with FillMode.DERIVED runs a query like this for each of its pens. An error here
        // would end the whole chart, not just this pen.
        startEngine(true);
        factry.failingMeasurements.add("uuid-1");

        assertEveryPenFailed(average("a"));
    }

    @Test
    void aggregatedReadThatFactryRejectsForEveryPenMarksEveryPen() throws Exception {
        startEngine(true);
        factry.failingMeasurements.add("uuid-1");
        factry.failingMeasurements.add("uuid-2");

        assertEveryPenFailed(average("a", "b"));
    }

    @Test
    void rawReadThatFactryRejectsForEveryPenIsAnError() throws Exception {
        // Raw rows come only from points, so Bad_Failure columns alone would be an empty, Good
        // result that looks like no data.
        startEngine(true);
        factry.failingMeasurements.add("uuid-1");
        factry.failingMeasurements.add("uuid-2");

        assertReadFailed(raw("a", "b"), "INTERNAL");
        assertReadFailed(raw("a"), "INTERNAL");
    }

    @Test
    void factryBackendDownOnEveryCallIsAnErrorForRaw() throws Exception {
        // Factry answers UNKNOWN when its own database is down: not a code that says Factry cannot
        // be reached, but every pen fails with it.
        startEngine(true);
        factry.queryFailure = Status.UNKNOWN.withDescription("influx: connection refused");

        assertReadFailed(raw("a", "b"), "influx: connection refused");
        assertReadFailed(raw("a"), "influx: connection refused");
    }

    @Test
    void factryBackendDownOnEveryCallMarksEveryAggregatedPen() throws Exception {
        // Not an error: with FillMode.DERIVED each pen is its own query, and an error would end
        // the whole chart. Interval rows carry Bad_Failure on every pen, so it does not look
        // like no data.
        startEngine(true);
        factry.queryFailure = Status.UNKNOWN.withDescription("influx: connection refused");

        assertEveryPenFailed(average("a", "b"));
    }

    @Test
    void factryTimingOutStopsAfterTheFirstCall() throws Exception {
        // A Factry that hangs answers every call with DEADLINE_EXCEEDED after the read deadline.
        // Each call more would add a full deadline to the wait.
        String[] tags = new String[10];
        for (int i = 0; i < tags.length; i++) {
            tags[i] = "p" + i;
            factry.addMeasurement("default/p" + i, "number");
        }
        startEngine(true);
        factry.queryFailure = Status.DEADLINE_EXCEEDED;

        assertReadFailed(IgnitionHistoryQuery.wide(engine, START, START + FIVE_MINUTES, 5, AggregationMode.MinMax,
                tags), "DEADLINE_EXCEEDED");
        assertEquals(1, factry.queries.size(), "calls after Factry timed out");

        factry.queries.clear();
        assertReadFailed(IgnitionHistoryQuery.wide(engine, START, START + FIVE_MINUTES, 5, AggregationMode.MinMax,
                FillMode.DERIVED, tags), "DEADLINE_EXCEEDED");
        assertEquals(1, factry.queries.size(), "calls after Factry timed out, one query per pen");

        factry.queries.clear();
        assertReadFailed(raw(tags), "DEADLINE_EXCEEDED");
        assertEquals(1, factry.queries.size(), "raw calls after Factry timed out");
    }

    @Test
    void rawReadOverThePointCapIsAnError() throws Exception {
        // Part of the range would show a cut-short pen as a flat line; the chart shows the reason.
        startEngine(true);
        engine.maxRawPoints = 5;

        assertReadFailed(raw("a", "b"), "query.raw.max.points");
    }

    @Test
    void aggregatedReadOverThePointCapIsAnError() throws Exception {
        // Every pen's points are held until all pens are read, and Average gives one per interval.
        startEngine(true);
        engine.maxRawPoints = 10;
        assertNull(average("a", "b").error, "2 pens x 5 intervals is within a cap of 10");

        engine.maxRawPoints = 9;
        factry.queries.clear();
        assertReadFailed(average("a", "b"), "query.raw.max.points");
        assertEquals(0, factry.queries.size(), "the cap should be checked before asking Factry");
    }

    @Test
    void unknownTagIsStillNotFoundAndNotAnError() throws Exception {
        startEngine(true);

        Dataset ds = raw("a", "c");

        assertNull(ds.error);
        assertEquals(6, ds.rows.size(), ds::toString);
        for (QualityCode[] q : ds.qualities) {
            assertTrue(q[1].isGood(), ds::toString);
            assertTrue(q[2].is(QualityCode.Bad_NotFound), ds::toString);
        }
    }

    @Test
    void everyTagUnknownAnswersAtOnce() throws Exception {
        // Without columns, Ignition waits 5 s for them before it gives up.
        startEngine(true);

        for (boolean aggregated : new boolean[]{false, true}) {
            long t0 = System.nanoTime();
            Dataset ds = aggregated ? average("x", "y") : raw("x", "y");
            long ms = (System.nanoTime() - t0) / 1_000_000;

            assertTrue(ms < 2_000, () -> "took " + ms + " ms:\n" + ds);
            assertNull(ds.error, ds::toString);
            for (QualityCode[] q : ds.qualities) {
                assertTrue(q[1].is(QualityCode.Bad_NotFound), ds::toString);
                assertTrue(q[2].is(QualityCode.Bad_NotFound), ds::toString);
            }
        }
    }

    @Test
    void rangeWithoutDataIsEmptyAndNotAnError() throws Exception {
        startEngine(true);

        Dataset ds = IgnitionHistoryQuery.wide(engine, START - 60_000L, START - 1L, -1, null, "a", "b");

        assertNull(ds.error);
        assertEquals(0, ds.rows.size(), ds::toString);
    }
}
