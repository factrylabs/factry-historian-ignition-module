package io.factry.historian.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pure, side-effect-free helpers for turning per-element array tag updates into
 * time-ordered, full-array snapshots.
 *
 * <p>Ignition sends array tags one element at a time and, thanks to per-element
 * deadbands, only the indices that actually changed arrive in a given update. A single
 * store-and-forward batch can also carry the same array at several timestamps (e.g. when
 * draining a backlog after an outage). This class decides how those partial, multi-timestamp
 * updates map to stored rows: group by timestamp, then apply changes cumulatively in time
 * order, emitting one full-array snapshot per timestamp so intermediate states are preserved
 * and correctly dated.
 *
 * <p>Deliberately free of Ignition SDK, gRPC and protobuf types so this core logic can be
 * unit-tested without Factry, a live gateway, or the measurement cache.
 */
final class ArrayPointMerger {
    private ArrayPointMerger() {}

    /** Pattern matching array-indexed tag paths like "some/path[0]", "some/path[123]". */
    private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("^(.+)\\[(\\d+)]$");

    /** A tag path split into its array base path and element index. */
    static final class ArrayRef {
        final String basePath;
        final int index;

        ArrayRef(String basePath, int index) {
            this.basePath = basePath;
            this.index = index;
        }
    }

    /**
     * Parse an array-indexed tag path into its base path and index, or return {@code null}
     * if the path is not array-indexed. E.g. {@code "line/temps[3]"} → {@code (line/temps, 3)};
     * {@code "line/speed"} → {@code null}.
     */
    static ArrayRef parseArrayPath(String tagPath) {
        Matcher m = ARRAY_INDEX_PATTERN.matcher(tagPath);
        if (!m.matches()) {
            return null;
        }
        return new ArrayRef(m.group(1), Integer.parseInt(m.group(2)));
    }

    /** One element change within an array update. */
    static final class ArrayElement {
        final int index;
        final long timestampMillis;
        final Object value;
        final int qualityCode;

        ArrayElement(int index, long timestampMillis, Object value, int qualityCode) {
            this.index = index;
            this.timestampMillis = timestampMillis;
            this.value = value;
            this.qualityCode = qualityCode;
        }
    }

    /** A full-array snapshot to be stored as a single row. */
    static final class ArraySnapshot {
        final long timestampMillis;
        /** The full array in ascending index order. */
        final List<Object> values;
        final int qualityCode;

        ArraySnapshot(long timestampMillis, List<Object> values, int qualityCode) {
            this.timestampMillis = timestampMillis;
            this.values = values;
            this.qualityCode = qualityCode;
        }
    }

    /**
     * Group element changes for one array by timestamp, then by index. Both levels are
     * sorted ascending so downstream merging is deterministic and in time order. When two
     * changes share the same timestamp and index, the later one in {@code elements} wins.
     */
    static TreeMap<Long, TreeMap<Integer, ArrayElement>> groupByTimestamp(List<ArrayElement> elements) {
        TreeMap<Long, TreeMap<Integer, ArrayElement>> byTimestamp = new TreeMap<>();
        for (ArrayElement e : elements) {
            byTimestamp
                    .computeIfAbsent(e.timestampMillis, k -> new TreeMap<>())
                    .put(e.index, e);
        }
        return byTimestamp;
    }

    /**
     * Apply per-timestamp element changes onto a running array state, in ascending time
     * order, emitting one snapshot per timestamp. Each snapshot is an independent copy of
     * the full array as it stood at that timestamp, so earlier states are not overwritten
     * by later ones.
     *
     * <p>{@code state} is the last-known full array (index → value); callers seed it from
     * the cache or from Factry so unchanged indices are preserved. It is mutated in place to
     * the final merged array so callers can reuse it as the updated last-known value.
     *
     * <p>Row quality is taken from the lowest-indexed element changed at each timestamp,
     * matching the pre-refactor behaviour.
     */
    static List<ArraySnapshot> mergeUpdates(
            TreeMap<Integer, Object> state,
            TreeMap<Long, TreeMap<Integer, ArrayElement>> byTimestamp) {
        List<ArraySnapshot> snapshots = new ArrayList<>();
        for (Map.Entry<Long, TreeMap<Integer, ArrayElement>> tsEntry : byTimestamp.entrySet()) {
            long ts = tsEntry.getKey();
            TreeMap<Integer, ArrayElement> elements = tsEntry.getValue();
            int qualityCode = elements.firstEntry().getValue().qualityCode;

            // Apply this timestamp's changes onto the running state.
            for (Map.Entry<Integer, ArrayElement> e : elements.entrySet()) {
                state.put(e.getKey(), e.getValue().value);
            }

            // Snapshot the full merged array at this timestamp, in index order.
            snapshots.add(new ArraySnapshot(ts, new ArrayList<>(state.values()), qualityCode));
        }
        return snapshots;
    }
}
