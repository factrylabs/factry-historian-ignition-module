package io.factry.historian.gateway;

import io.factry.historian.gateway.GoodTimeAverage.BucketStatus;
import io.factry.historian.gateway.GoodTimeAverage.Budget;
import io.factry.historian.gateway.GoodTimeAverage.Result;
import io.factry.historian.gateway.GoodTimeAverage.Sample;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The inputs mirror what Factry 8.2.3 returned for the same samples (twa fill "previous"
 * without a status filter, and {@code last} grouped by status).
 */
class GoodTimeAverageTest {

    private static final long MIN = 60_000;

    private static long t(String hhmm) {
        return Instant.parse("2026-11-10T" + hhmm + ":00Z").toEpochMilli();
    }

    private static final Budget UNLIMITED = new Budget(Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    /**
     * Raw samples on a fake Factry; records each [from, to) it is asked for. Counts per bucket
     * use {@code periodMs}; the tests' buckets start on multiples of it.
     */
    private static class Raw implements GoodTimeAverage.Source {
        final List<Sample> all;
        final List<String> calls = new ArrayList<>();
        long periodMs = 10 * MIN;
        Map<Long, Double> fallback = Map.of();
        int fallbackCalls = 0;

        Raw(Sample... all) {
            this.all = List.of(all);
        }

        @Override
        public Map<Long, Long> counts() {
            return all.stream().collect(Collectors.groupingBy(s -> s.ts() - s.ts() % periodMs, Collectors.counting()));
        }

        @Override
        public List<Sample> raw(long from, long to) {
            calls.add(Instant.ofEpochMilli(from) + ".." + Instant.ofEpochMilli(to));
            return all.stream().filter(s -> s.ts() >= from && s.ts() < to).toList();
        }

        @Override
        public Map<Long, Double> fallback() {
            fallbackCalls++;
            return fallback;
        }

        @Override
        public Sample before(long ts) {
            calls.add("before " + Instant.ofEpochMilli(ts));
            Sample last = null;
            for (Sample s : all) {
                if (s.ts() < ts) {
                    last = s;
                }
            }
            return last;
        }
    }

    /**
     * The values of the buckets with Good time, checking that all of them are exact (read raw or
     * from twa). Bad buckets are left out; {@link #bad} lists them.
     */
    private static Map<Long, Double> exact(Map<Long, Result> out) {
        Map<Long, Double> values = new LinkedHashMap<>();
        out.forEach((k, r) -> {
            assertTrue(r.exact(), () -> "not exact at " + Instant.ofEpochMilli(k));
            if (!r.isBad()) {
                values.put(k, r.value());
            }
        });
        return values;
    }

    private static List<Long> bad(Map<Long, Result> out) {
        return out.entrySet().stream().filter(e -> e.getValue().isBad()).map(Map.Entry::getKey).sorted().toList();
    }

    private static Sample good(String hhmm, double v) {
        return new Sample(t(hhmm), v, true);
    }

    private static Sample bad(String hhmm) {
        return new Sample(t(hhmm), -1, false);
    }

    private static BucketStatus status(Double lastGood, boolean notGood) {
        BucketStatus st = new BucketStatus();
        if (lastGood != null) {
            st.add(true, lastGood);
        }
        if (notGood) {
            st.add(false, -1);
        }
        return st;
    }

    private static Map<Long, Double> buckets(String... hhmmValue) {
        Map<Long, Double> m = new LinkedHashMap<>();
        for (String kv : hhmmValue) {
            String[] p = kv.split("=");
            m.put(t(p[0]), Double.parseDouble(p[1]));
        }
        return m;
    }

    @Test
    void outage_isAGapAndEdgeBucketsAverageOnlyTheirGoodTime() {
        // 100 since 07:30, 40 at 08:02, Bad from 08:05 (a Bad sample every 10 minutes),
        // Good again with 50 at 08:55 and 80 at 08:57. Factry's twa with a Good filter gives
        // 52, 40, 40, 40, 40, 54: the outage reads as a flat Good 40.
        Raw raw = new Raw(good("08:02", 40), bad("08:05"), bad("08:15"), bad("08:25"),
                bad("08:35"), bad("08:45"), good("08:55", 50), good("08:57", 80));
        Map<Long, BucketStatus> statuses = Map.of(
                t("08:00"), status(40.0, true),
                t("08:10"), status(null, true),
                t("08:20"), status(null, true),
                t("08:30"), status(null, true),
                t("08:40"), status(null, true),
                t("08:50"), status(80.0, false));

        Map<Long, Result> res = GoodTimeAverage.compute(
                buckets("08:00=31.5", "08:10=-1", "08:20=-1", "08:30=-1", "08:40=-1", "08:50=33.5"),
                statuses, good("07:30", 100), 10 * MIN, t("09:00"), UNLIMITED, raw);
        Map<Long, Double> out = exact(res);

        // 08:00: 2 min at 100 and 3 min at 40 over 5 Good minutes. 08:50: 2 min at 50 and
        // 3 min at 80 over 5 Good minutes. The four buckets inside the outage are Bad.
        assertEquals(Map.of(t("08:00"), 64.0, t("08:50"), 68.0), out);
        assertEquals(List.of(t("08:10"), t("08:20"), t("08:30"), t("08:40")), bad(res));
        // The two buckets where the status changes share one raw request; the outage between
        // them adds only its five Bad samples.
        assertEquals(List.of("2026-11-10T08:00:00Z..2026-11-10T09:00:00Z"), raw.calls);
    }

    @Test
    void outage_inOneBucket_averagesOnlyItsGoodTime() {
        Raw raw = new Raw(good("08:02", 40), bad("08:05"), bad("08:15"), bad("08:25"),
                bad("08:35"), bad("08:45"), good("08:55", 50), good("08:57", 80));
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(80.0, true));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=31.5"), statuses,
                good("07:30", 100), 60 * MIN, t("09:00"), UNLIMITED, raw));

        // (2*100 + 3*40 + 2*50 + 3*80) / 10 Good minutes, not 31.5 (Bad values in) or 52 (held).
        assertEquals(Map.of(t("08:00"), 66.0), out);
    }

    @Test
    void goodThroughout_usesFactryTwaWithoutRawSamples() {
        Raw raw = new Raw();
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(0.0, false));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=10", "08:10=0", "08:20=0"),
                statuses, good("07:00", 100), 10 * MIN, t("08:30"), UNLIMITED, raw));

        assertEquals(buckets("08:00=10", "08:10=0", "08:20=0"), out);
        assertEquals(List.of(), raw.calls);
    }

    @Test
    void rangeStartingInsideAnOutage_isAGapUntilGoodReturns() {
        Raw raw = new Raw(good("08:25", 20));
        Map<Long, BucketStatus> statuses = Map.of(t("08:20"), status(20.0, false));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=-1", "08:10=-1", "08:20=14", "08:30=20"),
                statuses, bad("07:50"), 10 * MIN, t("08:40"), UNLIMITED, raw));

        // 08:20 holds Good only from 08:25; 08:30 is Good throughout.
        assertEquals(buckets("08:20=20", "08:30=20"), out);
        assertEquals(List.of("2026-11-10T08:20:00Z..2026-11-10T08:30:00Z"), raw.calls);
    }

    @Test
    void lastSampleBad_leavesTheRestOfTheRangeEmpty() {
        Raw raw = new Raw(bad("08:05"));
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(null, true));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=50", "08:10=-1", "08:20=-1"),
                statuses, good("07:00", 100), 10 * MIN, t("08:30"), UNLIMITED, raw));

        assertEquals(buckets("08:00=100"), out);
    }

    @Test
    void uncertainSamples_areNotGoodTime() {
        // statuses are classified by the engine; an Uncertain sample is passed as not good.
        Raw raw = new Raw(new Sample(t("08:05"), 7, false), good("08:08", 10));
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(10.0, true));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=99"), statuses,
                good("07:00", 4), 10 * MIN, t("08:10"), UNLIMITED, raw));

        assertEquals(Map.of(t("08:00"), (5 * 4 + 2 * 10) / 7.0), out);
    }

    @Test
    void twoGoodStatusesInABucket_areFetchedRaw() {
        // "Good" and "GoodLocalOverride" are both Good, but per-status last values cannot say
        // which came last, so the bucket is computed from its samples.
        Raw raw = new Raw(good("08:00", 10), good("08:30", 20));
        BucketStatus st = status(10.0, false);
        st.add(true, 20);

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=15"), Map.of(t("08:00"), st),
                null, 60 * MIN, t("09:00"), UNLIMITED, raw));

        assertEquals(Map.of(t("08:00"), 15.0), out);
        assertEquals(1, raw.calls.size());
    }

    @Test
    void alternatingGoodAndBadBuckets_shareOneRequest() {
        // Each bucket holds one status, but it differs from the one held at its start, so every
        // bucket needs raw samples. They are read in one request, not one per bucket.
        Raw raw = new Raw(good("08:05", 10), bad("08:15"), good("08:25", 30), bad("08:35"));
        Map<Long, BucketStatus> statuses = Map.of(
                t("08:00"), status(10.0, false), t("08:10"), status(null, true),
                t("08:20"), status(30.0, false), t("08:30"), status(null, true));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(
                buckets("08:00=0", "08:10=0", "08:20=0", "08:30=0"),
                statuses, bad("07:00"), 10 * MIN, t("08:40"), UNLIMITED, raw));

        assertEquals(buckets("08:00=10", "08:10=10", "08:20=30", "08:30=30"), out);
        assertEquals(List.of("2026-11-10T08:00:00Z..2026-11-10T08:40:00Z"), raw.calls);
    }

    @Test
    void requestsStayWithinThePointsPerRequest() {
        // Two samples per bucket, at most four per request: two buckets per request, grouped
        // from the range end.
        Raw raw = new Raw(good("08:01", 1), bad("08:02"), good("08:11", 2), bad("08:12"),
                good("08:21", 3), bad("08:22"));
        Map<Long, BucketStatus> statuses = Map.of(
                t("08:00"), status(1.0, true), t("08:10"), status(2.0, true), t("08:20"), status(3.0, true));

        Map<Long, Double> out = exact(GoodTimeAverage.compute(buckets("08:00=0", "08:10=0", "08:20=0"),
                statuses, null, 10 * MIN, t("08:30"), new Budget(4, 100, 10, 10), raw));

        assertEquals(buckets("08:00=1", "08:10=2", "08:20=3"), out);
        assertEquals(List.of("2026-11-10T08:00:00Z..2026-11-10T08:10:00Z",
                "2026-11-10T08:10:00Z..2026-11-10T08:30:00Z"), raw.calls);
    }

    @Test
    void bucketsBeyondThePenBudget_areTheOldestAndGetTheFallback() {
        Raw raw = new Raw(good("08:01", 1), bad("08:02"), good("08:11", 2), bad("08:12"),
                good("08:21", 3), bad("08:22"));
        raw.fallback = buckets("08:00=7", "08:10=8", "08:20=9");
        Map<Long, BucketStatus> statuses = Map.of(
                t("08:00"), status(1.0, true), t("08:10"), status(2.0, true), t("08:20"), status(3.0, true));

        Map<Long, Result> out = GoodTimeAverage.compute(buckets("08:00=0", "08:10=0", "08:20=0"),
                statuses, null, 10 * MIN, t("08:30"), new Budget(100, 2, 10, 10), raw);

        // The budget goes to the most recent bucket, so a trend's newest part is exact.
        assertEquals(Map.of(t("08:00"), new Result(7, false), t("08:10"), new Result(8, false),
                t("08:20"), new Result(3, true)), out);
        assertEquals(List.of("before 2026-11-10T08:20:00Z", "2026-11-10T08:20:00Z..2026-11-10T08:30:00Z"),
                raw.calls);
        assertEquals(1, raw.fallbackCalls);
    }

    @Test
    void aBucketTooBigToRead_doesNotKeepTheBucketsAfterItFromBeingRead() {
        // 08:00 is too big to read, so the status at 08:10's start is unknown. One read of the
        // sample before 08:10 settles it, and 08:10 is averaged from its own samples.
        Raw raw = new Raw(good("08:01", 1), bad("08:02"), good("08:03", 1), bad("08:12"), good("08:15", 2));
        raw.fallback = buckets("08:00=7", "08:10=8", "08:20=9");
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(1.0, true), t("08:10"), status(2.0, true));

        Map<Long, Result> out = GoodTimeAverage.compute(buckets("08:00=0", "08:10=0", "08:20=2"),
                statuses, null, 10 * MIN, t("08:30"), new Budget(2, 100, 10, 10), raw);

        // 08:10: 2 min at 1 (held from 08:03), Bad 08:12 to 08:15, 5 min at 2.
        assertEquals(Map.of(t("08:00"), new Result(7, false), t("08:10"), new Result((2 * 1 + 5 * 2) / 7.0, true),
                t("08:20"), new Result(2, true)), out);
        assertEquals(List.of("before 2026-11-10T08:10:00Z", "2026-11-10T08:10:00Z..2026-11-10T08:20:00Z"),
                raw.calls);
    }

    @Test
    void aBadBucketAfterABucketTooBigToRead_isBad() {
        // 08:00 ends Bad but is not read; 08:10 holds only a Bad sample. Without raw requests
        // left, the sample before 08:10 still shows it starts Bad: a gap, not a held Good value.
        Raw raw = new Raw(good("08:01", 1), good("08:02", 1), bad("08:03"), bad("08:15"));
        raw.fallback = buckets("08:00=1", "08:10=1");
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(1.0, true), t("08:10"), status(null, true));

        Map<Long, Result> out = GoodTimeAverage.compute(buckets("08:00=0", "08:10=0"),
                statuses, null, 10 * MIN, t("08:20"), new Budget(100, 100, 0, 10), raw);

        assertEquals(Map.of(t("08:00"), new Result(1, false), t("08:10"), Result.BAD), out);
        assertEquals(List.of("before 2026-11-10T08:10:00Z"), raw.calls);
    }

    @Test
    void healthyTag_needsNoCountsAndNoFallback() {
        Raw raw = new Raw() {
            @Override
            public Map<Long, Long> counts() {
                throw new AssertionError("counts are only needed when a bucket needs raw samples");
            }
        };
        Map<Long, BucketStatus> statuses = Map.of(t("08:00"), status(5.0, false));

        exact(GoodTimeAverage.compute(buckets("08:00=5", "08:10=5"), statuses, good("07:00", 5),
                10 * MIN, t("08:20"), UNLIMITED, raw));

        assertEquals(0, raw.fallbackCalls);
    }

    @Test
    void oldBucketsCannotUseUpTheReadANewRawRequestNeeds() {
        // One read outside raw requests, and budget for only the newest bucket. Walking from
        // 08:00, 08:10 spends that read; 08:30 is the raw request and still needs the status at
        // its start (08:20 held both statuses and was not read): Good 30 since 08:25.
        Raw raw = new Raw(good("08:02", 10), bad("08:05"), good("08:12", 20), bad("08:21"),
                good("08:25", 30), bad("08:35"));
        raw.fallback = buckets("08:00=10", "08:10=20", "08:20=25");
        Map<Long, BucketStatus> statuses = Map.of(
                t("08:00"), status(10.0, true), t("08:10"), status(20.0, false),
                t("08:20"), status(30.0, true), t("08:30"), status(null, true));

        Map<Long, Result> out = GoodTimeAverage.compute(buckets("08:00=0", "08:10=0", "08:20=0", "08:30=0"),
                statuses, null, 10 * MIN, t("08:40"), new Budget(100, 1, 10, 1), raw);

        // 08:30: 5 minutes at 30 (Good), then Bad.
        assertEquals(new Result(30, true), out.get(t("08:30")));
        assertEquals(List.of("before 2026-11-10T08:10:00Z", "before 2026-11-10T08:30:00Z",
                "2026-11-10T08:30:00Z..2026-11-10T08:40:00Z"), raw.calls);
    }
}
