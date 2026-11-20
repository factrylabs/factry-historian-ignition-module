package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ignition historizes an array tag as one path per element ({@code Arr[0]}, {@code Arr[1]}, ...);
 * PointWriter stores them as one list per row in the measurement {@code Arr}. Read back the way
 * Ignition reads (HistorianQueryInterface and HistoryWriter), each element path must give its own
 * element, and the paths Factry cannot answer must say so in their quality instead of reading 0.0
 * marked Good.
 */
class ArrayElementReadTest {

    private static final long T0 = Instant.parse("2026-11-10T10:00:00Z").toEpochMilli();
    private static final long END = T0 + 60_000;
    /** A value that reads as a Bad row. */
    private static final Object BAD = "Bad";

    private final TwaFakeFactry factry = new TwaFakeFactry();
    private String arr;
    private String plain;

    /** Arr = [1,10,100] at 10 s, [2,20,200] at 25 s, [3,30,300] at 45 s; plain = 7 at 5 s. */
    private void storeArray() {
        arr = factry.addMeasurement("default/Arr", "[]number");
        factry.add(arr, T0 + 10_000, List.of(1, 10, 100), "Good");
        factry.add(arr, T0 + 25_000, List.of(2, 20, 200), "Good");
        factry.add(arr, T0 + 45_000, List.of(3, 30, 300), "Good");
        plain = factry.addMeasurement("default/Plain", "number");
        factry.add(plain, T0 + 5_000, 7, "Good");
    }

    private FactryQueryEngine engine() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(factry);
        FactryQueryEngine engine = new FactryQueryEngine(TwaFakeFactry.gatewayContext(), "Factry", settings,
                factry, cache, new HistorianMetrics());
        engine.now = () -> T0 + 3_600_000;
        return engine;
    }

    private IgnitionHistoryQuery.Dataset raw(int returnSize, String... tags) throws Exception {
        return raw(returnSize, 0, tags);
    }

    private IgnitionHistoryQuery.Dataset raw(int returnSize, int flags, String... tags) throws Exception {
        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.raw(engine(), T0, END, returnSize, flags, tags);
        assertNull(ds.error, ds::toString);
        return ds;
    }

    /**
     * Column {@code c} of a raw read, one value per timestamp. Ignition's wide writer starts a row
     * for every point, also for two tags stored at the same time (as on main for any two tags),
     * so a stored array row gives one row per element; the last one has every element.
     */
    private static List<Object> column(IgnitionHistoryQuery.Dataset ds, int c) {
        List<Object[]> rows = new ArrayList<>();
        List<QualityCode[]> qualities = new ArrayList<>();
        for (int r = 0; r < ds.rows.size(); r++) {
            if (r + 1 == ds.rows.size() || ds.time(r + 1) != ds.time(r)) {
                rows.add(ds.rows.get(r));
                qualities.add(ds.qualities.get(r));
            }
        }
        return column(rows, qualities, c);
    }

    /** Six 10 s buckets from T0. */
    private AverageHistoryQuery.Dataset aggregated(AggregationMode mode, String... tags) throws Exception {
        AverageHistoryQuery.Dataset ds = AverageHistoryQuery.wide(engine(), T0, END, 6, mode, FillMode.NONE, tags);
        assertNull(ds.error, ds::toString);
        return ds;
    }

    /** Column {@code c} (1 is the first tag) per row, with {@link #BAD} where the quality is not Good. */
    private static List<Object> column(List<Object[]> rows, List<QualityCode[]> qualities, int c) {
        List<Object> out = new ArrayList<>();
        for (int r = 0; r < rows.size(); r++) {
            out.add(qualities.get(r)[c].isGood() ? rows.get(r)[c] : BAD);
        }
        return out;
    }

    @Test
    void rawElementPaths_readTheirOwnElement() throws Exception {
        storeArray();

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Arr[1]", "Arr[2]", "Plain");

        // Before the fix every Arr[i] column was Bad_NotFound.
        assertEquals(List.of(BAD, 10.0, 20.0, 30.0), column(ds, 1), ds::toString);
        assertEquals(List.of(BAD, 100.0, 200.0, 300.0), column(ds, 2), ds::toString);
        assertEquals(List.of(7.0, 7.0, 7.0, 7.0), column(ds, 3), ds::toString);
    }

    @Test
    void rawElementPaths_getTheirValueBeforeTheStart() throws Exception {
        storeArray();
        factry.add(arr, T0 - 30_000, List.of(5, 50, 500), "Good");

        // includeBoundingValues=True; Ignition 8.3.6 asks for no bounds otherwise.
        IgnitionHistoryQuery.Dataset ds = raw(-1, 16, "Arr[0]", "Arr[1]");

        assertEquals(T0 - 30_000, ds.time(0), ds::toString);
        assertEquals(List.of(5.0, 1.0, 2.0, 3.0), column(ds, 1), ds::toString);
        assertEquals(List.of(50.0, 10.0, 20.0, 30.0), column(ds, 2), ds::toString);
    }

    @Test
    void theBasePathOfAnArray_isUnsupported_notZeroMarkedGood() throws Exception {
        storeArray();

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Arr", "Arr[0]");

        // Before the fix the base path read 0.0, Good, on every row.
        assertEquals(3, ds.rows.size(), ds::toString);
        for (int r = 0; r < ds.rows.size(); r++) {
            assertTrue(ds.qualities.get(r)[1].is(QualityCode.Bad_Unsupported), ds::toString);
            assertNull(ds.rows.get(r)[1], ds::toString);
        }
        assertEquals(List.of(1.0, 2.0, 3.0), column(ds, 2), ds::toString);

        AverageHistoryQuery.Dataset agg = aggregated(AggregationMode.LastValue, "Arr");
        for (QualityCode[] q : agg.qualities) {
            assertTrue(q[1].is(QualityCode.Bad_Unsupported), agg::toString);
        }
    }

    @Test
    void returnSize_countsRowsOfEveryElement() throws Exception {
        storeArray();

        // Two elements of one stored row are two points, as two tags would be.
        IgnitionHistoryQuery.Dataset ds = raw(4, "Arr[0]", "Arr[2]");

        assertEquals(4, ds.rows.size(), ds::toString);
        assertEquals(List.of(1.0, 2.0), column(ds, 1), ds::toString);
        assertEquals(List.of(100.0, 200.0), column(ds, 2), ds::toString);
    }

    @Test
    void aRefusedArray_failsEachOfItsElementColumns_andNotTheOtherPens() throws Exception {
        storeArray();
        factry.failingMeasurements.add(arr);

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Plain", "Arr[0]", "Arr[1]");

        assertEquals(List.of(7.0), column(ds, 1), ds::toString);
        for (QualityCode[] q : ds.qualities) {
            assertTrue(q[2].is(QualityCode.Bad_Failure), ds::toString);
            assertTrue(q[3].is(QualityCode.Bad_Failure), ds::toString);
        }
    }

    @Test
    void lastValueAndCount_ofAnElement() throws Exception {
        storeArray();

        AverageHistoryQuery.Dataset last = aggregated(AggregationMode.LastValue, "Arr[2]", "Arr[0]");
        assertEquals(Arrays.asList(BAD, 100.0, 200.0, 200.0, 300.0, 300.0),
                column(last.rows, last.qualities, 1), last::toString);
        assertEquals(Arrays.asList(BAD, 1.0, 2.0, 2.0, 3.0, 3.0),
                column(last.rows, last.qualities, 2), last::toString);

        // A count of stored rows: an array row is written whenever any element changes.
        AverageHistoryQuery.Dataset count = aggregated(AggregationMode.Count, "Arr[1]");
        assertEquals(Arrays.asList(0.0, 1.0, 1.0, 0.0, 1.0, 0.0),
                column(count.rows, count.qualities, 1), count::toString);
    }

    @Test
    void aggregatesFactryCannotComputeForAnArray_areUnsupportedPerColumn() throws Exception {
        storeArray();

        for (AggregationMode mode : List.of(AggregationMode.Average, AggregationMode.SimpleAverage,
                AggregationMode.Minimum, AggregationMode.Maximum, AggregationMode.MinMax, AggregationMode.Sum,
                AggregationMode.Range, AggregationMode.Variance, AggregationMode.StdDev)) {
            factry.queries.clear();
            AverageHistoryQuery.Dataset ds = aggregated(mode, "Plain", "Arr[0]");

            for (QualityCode[] q : ds.qualities) {
                // Not Bad_NotFound: the path exists, Factry just cannot aggregate an array.
                assertTrue(q[2].is(QualityCode.Bad_Unsupported), () -> mode + "\n" + ds);
            }
            assertTrue(ds.qualities.stream().anyMatch(q -> q[1].isGood()), () -> mode + "\n" + ds);
            // Refused before asking Factry, so no request fails and no time is lost on it.
            assertTrue(factry.queries.stream().noneMatch(q -> q.getMeasurementUUIDsList().contains(arr)),
                    () -> mode + ": " + factry.queries);
        }
    }

    @Test
    void aStringArrayElement_readsAsText() throws Exception {
        String names = factry.addMeasurement("default/Names", "[]string");
        factry.add(names, T0 + 10_000, List.of("a", "b"), "Good");
        factry.add(names, T0 + 30_000, List.of("c", "b"), "Good");

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Names[0]", "Names[1]");

        assertEquals(List.of("a", "c"), column(ds, 1), ds::toString);
        assertEquals(List.of("b", "b"), column(ds, 2), ds::toString);
    }

    @Test
    void aDateTimeArrayElement_readsAsADate() throws Exception {
        // PointWriter records the array tag's Ignition type on the measurement, e.g. DateTimeArray.
        String stamps = factry.addMeasurement("default/Stamps", "[]number", "DateTimeArray");
        factry.add(stamps, T0 + 10_000, List.of(T0 - 5_000, T0 - 1_000), "Good");

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Stamps[1]");

        assertEquals(List.of(new java.util.Date(T0 - 1_000)), column(ds, 1), ds::toString);
        AverageHistoryQuery.Dataset last = aggregated(AggregationMode.LastValue, "Stamps[0]");
        assertEquals(new java.util.Date(T0 - 5_000), last.rows.get(1)[1], last::toString);
    }

    @Test
    void anIndexOnAPlainMeasurement_isNotFound() throws Exception {
        storeArray();

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Arr[0]", "Plain[0]");

        assertEquals(List.of(1.0, 2.0, 3.0), column(ds, 1), ds::toString);
        for (QualityCode[] q : ds.qualities) {
            assertTrue(q[2].is(QualityCode.Bad_NotFound), ds::toString);
        }
    }

    @Test
    void anIndexTooLargeForAnInt_isNotFound_andTheOtherPensStillRead() throws Exception {
        storeArray();

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Plain", "Arr[99999999999]");

        assertNull(ds.error, ds::toString);
        assertEquals(List.of(7.0), column(ds, 1), ds::toString);
        for (QualityCode[] q : ds.qualities) {
            assertTrue(q[2].is(QualityCode.Bad_NotFound), ds::toString);
        }
    }

    @Test
    void anElementPastTheEndOfAStoredRow_isBad_notZeroMarkedGood() throws Exception {
        storeArray();

        IgnitionHistoryQuery.Dataset ds = raw(-1, "Arr[0]", "Arr[5]");

        assertEquals(List.of(1.0, 2.0, 3.0), column(ds, 1), ds::toString);
        assertEquals(List.of(BAD, BAD, BAD), column(ds, 2), ds::toString);
    }
}
