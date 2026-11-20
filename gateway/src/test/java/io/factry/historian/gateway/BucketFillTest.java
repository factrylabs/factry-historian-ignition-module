package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BucketFillTest {

    private final Map<Long, BucketFill.Status> statuses = new HashMap<>();
    private final List<Long> reads = new ArrayList<>();

    private void bucket(long start, boolean good, Object last) {
        statuses.computeIfAbsent(start, t -> new BucketFill.Status()).add(good, last);
    }

    private List<String> fill(long buckets, long now, int maxReads, BucketFill.Sample before) {
        List<String> out = new ArrayList<>();
        for (BucketFill.Bucket b : BucketFill.fill(0, buckets * 10, 10, now, statuses, ts -> {
            reads.add(ts);
            return before == null ? List.of() : List.of(before);
        }, maxReads, false)) {
            out.add(b.kind() == BucketFill.Kind.HELD ? "held " + b.value() : b.kind().toString());
        }
        return out;
    }

    @Test
    void theSampleBeforeTheRange_isReadOnlyWhenTheFirstBucketsAreEmpty() {
        bucket(0, true, 1.0);

        assertEquals(List.of("GOOD_SAMPLES", "held 1.0", "held 1.0"),
                fill(3, 100, 10, new BucketFill.Sample(-5, 9.0, true)));
        assertEquals(List.of(), reads);

        statuses.clear();
        bucket(20, true, 2.0);
        assertEquals(List.of("held 9.0", "held 9.0", "GOOD_SAMPLES"),
                fill(3, 100, 10, new BucketFill.Sample(-5, 9.0, true)));
        assertEquals(List.of(0L), reads);
    }

    @Test
    void readsAfterMixedBuckets_areCapped_thenAnEmptyBucketIsBad() {
        // Mixed, empty, mixed, empty: the second empty bucket is past the cap of one read. The
        // sample read says Good, so the first empty bucket holds it; the second is a gap.
        bucket(0, true, 1.0);
        bucket(0, false, null);
        bucket(20, true, 2.0);
        bucket(20, false, null);

        assertEquals(List.of("GOOD_SAMPLES", "held 1.0", "GOOD_SAMPLES", "BAD"),
                fill(4, 100, 1, new BucketFill.Sample(5, 1.0, true)));
        assertEquals(List.of(10L), reads);
    }

    @Test
    void bucketsFromNowOn_areBad_withoutReads() {
        assertEquals(List.of("BAD", "BAD"), fill(2, 0, 10, new BucketFill.Sample(-5, 9.0, true)));
        assertEquals(List.of(), reads);
    }
}
