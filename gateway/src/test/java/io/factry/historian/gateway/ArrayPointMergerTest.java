package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the pure array-merge logic that turns partial, possibly multi-timestamp
 * array element updates into full-array snapshot rows. This is the logic behind the
 * store-and-forward "collapsed rows" fix: a batch draining a backlog after an outage can
 * carry the same array at several timestamps, and each must become its own correctly-dated
 * row rather than collapsing into one.
 */
class ArrayPointMergerTest {

    private static final int GOOD = 192; // arbitrary quality code, echoed straight through

    // --- parseArrayPath ---

    @Test
    void parseArrayPath_arrayIndexed() {
        ArrayPointMerger.ArrayRef ref = ArrayPointMerger.parseArrayPath("line/temps[3]");
        assertNotNull(ref);
        assertEquals("line/temps", ref.basePath);
        assertEquals(3, ref.index);
    }

    @Test
    void parseArrayPath_multiDigitIndex() {
        ArrayPointMerger.ArrayRef ref = ArrayPointMerger.parseArrayPath("a/b/c[123]");
        assertNotNull(ref);
        assertEquals("a/b/c", ref.basePath);
        assertEquals(123, ref.index);
    }

    @Test
    void parseArrayPath_notAnArray() {
        assertNull(ArrayPointMerger.parseArrayPath("line/speed"));
    }

    @Test
    void parseArrayPath_bracketsInBasePathKeepLastIndexOnly() {
        // Base path itself may legitimately contain brackets; only the trailing [n] is the index.
        ArrayPointMerger.ArrayRef ref = ArrayPointMerger.parseArrayPath("group[7]/temps[2]");
        assertNotNull(ref);
        assertEquals("group[7]/temps", ref.basePath);
        assertEquals(2, ref.index);
    }

    @Test
    void parseArrayPath_nonNumericIndexIsNotArray() {
        assertNull(ArrayPointMerger.parseArrayPath("line/temps[x]"));
    }

    // --- groupByTimestamp ---

    @Test
    void groupByTimestamp_sortsByTimestampThenIndex() {
        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(3, 200L, 5.0, GOOD),
                new ArrayPointMerger.ArrayElement(1, 100L, 9.0, GOOD),
                new ArrayPointMerger.ArrayElement(2, 100L, 4.0, GOOD));

        TreeMap<Long, TreeMap<Integer, ArrayPointMerger.ArrayElement>> grouped =
                ArrayPointMerger.groupByTimestamp(elements);

        assertEquals(Arrays.asList(100L, 200L), new ArrayList<>(grouped.keySet()));
        // Within timestamp 100 the indices are sorted ascending.
        assertEquals(Arrays.asList(1, 2), new ArrayList<>(grouped.get(100L).keySet()));
        assertEquals(Arrays.asList(3), new ArrayList<>(grouped.get(200L).keySet()));
    }

    @Test
    void groupByTimestamp_lastWriteWinsForSameTimestampAndIndex() {
        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(0, 100L, 1.0, GOOD),
                new ArrayPointMerger.ArrayElement(0, 100L, 2.0, GOOD));

        TreeMap<Long, TreeMap<Integer, ArrayPointMerger.ArrayElement>> grouped =
                ArrayPointMerger.groupByTimestamp(elements);

        assertEquals(2.0, grouped.get(100L).get(0).value);
    }

    // --- mergeUpdates ---

    /**
     * The core regression test. A single batch carries the same array at two timestamps.
     * Starting from [1,2,3,4]: at T1 indices 2 and 3 change to 4 and 5; at T2 index 1 changes
     * to 9. The old "group by index only" logic collapsed this into one row; we now expect two
     * correctly-dated rows, each a full point-in-time snapshot.
     */
    @Test
    void mergeUpdates_multipleTimestampsProduceOneRowEach() {
        TreeMap<Integer, Object> state = seededState(1.0, 2.0, 3.0, 4.0);

        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(2, 100L, 4.0, GOOD),
                new ArrayPointMerger.ArrayElement(3, 100L, 5.0, GOOD),
                new ArrayPointMerger.ArrayElement(1, 200L, 9.0, GOOD));

        List<ArrayPointMerger.ArraySnapshot> snaps =
                ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(2, snaps.size(), "one row per timestamp");

        assertEquals(100L, snaps.get(0).timestampMillis);
        assertEquals(Arrays.asList(1.0, 2.0, 4.0, 5.0), snaps.get(0).values);

        assertEquals(200L, snaps.get(1).timestampMillis);
        assertEquals(Arrays.asList(1.0, 9.0, 4.0, 5.0), snaps.get(1).values,
                "second row builds cumulatively on the first");
    }

    @Test
    void mergeUpdates_singleTimestampProducesOneMergedRow() {
        TreeMap<Integer, Object> state = seededState(1.0, 2.0, 3.0, 4.0);

        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(2, 100L, 4.0, GOOD),
                new ArrayPointMerger.ArrayElement(3, 100L, 5.0, GOOD));

        List<ArrayPointMerger.ArraySnapshot> snaps =
                ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(1, snaps.size());
        assertEquals(Arrays.asList(1.0, 2.0, 4.0, 5.0), snaps.get(0).values,
                "unchanged indices preserved from seeded state");
    }

    @Test
    void mergeUpdates_emptyStateStoresOnlyChangedIndices() {
        // No seed (cold start with nothing in Factry): only the indices that arrive are stored.
        TreeMap<Integer, Object> state = new TreeMap<>();

        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(0, 100L, 7.0, GOOD),
                new ArrayPointMerger.ArrayElement(1, 100L, 8.0, GOOD));

        List<ArrayPointMerger.ArraySnapshot> snaps =
                ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(1, snaps.size());
        assertEquals(Arrays.asList(7.0, 8.0), snaps.get(0).values);
    }

    @Test
    void mergeUpdates_snapshotsAreIndependentCopies() {
        // A later timestamp must not mutate an already-emitted earlier snapshot.
        TreeMap<Integer, Object> state = seededState(0.0);

        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(0, 100L, 1.0, GOOD),
                new ArrayPointMerger.ArrayElement(0, 200L, 2.0, GOOD));

        List<ArrayPointMerger.ArraySnapshot> snaps =
                ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(Arrays.asList(1.0), snaps.get(0).values);
        assertEquals(Arrays.asList(2.0), snaps.get(1).values);
    }

    @Test
    void mergeUpdates_leavesStateAsFinalMergedArray() {
        // Callers reuse state as the last-known value cache, so it must reflect the newest values.
        TreeMap<Integer, Object> state = seededState(1.0, 2.0, 3.0);

        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(0, 100L, 10.0, GOOD),
                new ArrayPointMerger.ArrayElement(2, 200L, 30.0, GOOD));

        ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(Arrays.asList(10.0, 2.0, 30.0), new ArrayList<>(state.values()));
    }

    @Test
    void mergeUpdates_rowQualityTakenFromLowestChangedIndex() {
        TreeMap<Integer, Object> state = seededState(1.0, 2.0);

        int bad = 24;
        List<ArrayPointMerger.ArrayElement> elements = Arrays.asList(
                new ArrayPointMerger.ArrayElement(1, 100L, 5.0, GOOD),
                new ArrayPointMerger.ArrayElement(0, 100L, 6.0, bad));

        List<ArrayPointMerger.ArraySnapshot> snaps =
                ArrayPointMerger.mergeUpdates(state, ArrayPointMerger.groupByTimestamp(elements));

        assertEquals(bad, snaps.get(0).qualityCode, "quality comes from the lowest-indexed change");
    }

    /** Build a state map seeded with the given values at indices 0..n-1. */
    private static TreeMap<Integer, Object> seededState(Object... values) {
        TreeMap<Integer, Object> state = new TreeMap<>();
        for (int i = 0; i < values.length; i++) {
            state.put(i, values[i]);
        }
        return state;
    }
}
