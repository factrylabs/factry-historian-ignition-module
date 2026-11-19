package io.factry.historian.gateway;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Ignition's time-weighted "Average", counted over Good time only.
 *
 * <p>Each stored sample holds until the next one, whatever its status. A Bad or Uncertain sample
 * therefore ends the Good time of the value before it. Factry's {@code twa} (fill "previous")
 * cannot do this: it refuses group by, and a status filter makes it hold the last Good value
 * straight through an outage stored as Bad samples.
 *
 * <p>So per bucket this class decides from cheap aggregates which case applies:
 * <ul>
 *   <li>Good the whole bucket (held Good, no other status inside): Factry's {@code twa} is right.
 *   <li>Not Good the whole bucket (held not Good, no Good sample inside): {@link Result#BAD}.
 *       Not leaving the bucket out: Ignition fills a bucket without a point with a
 *       neighbouring value marked Good, so an outage would read as a held Good value.
 *   <li>Otherwise the status changes inside the bucket: its raw samples are fetched and averaged
 *       over the Good time only.
 * </ul>
 *
 * <p>Raw reads are bounded by a {@link Budget}. Neighbouring buckets that need raw samples share
 * one request, also across buckets in between that do not need them, as long as the request
 * stays within the budget. The budget is spent from the range end backwards, so the most recent
 * part of a trend is the exact part. A bucket the budget does not cover gets the
 * {@link Source#fallback()} value instead: an approximate number for an old bucket is better
 * than a pen that fails because one request grew past the gRPC message limit.
 *
 * <p>Free of Ignition SDK and gRPC types so it can be unit-tested without Factry.
 */
final class GoodTimeAverage {
    private GoodTimeAverage() {}

    /** One stored sample; {@code good} is its status level. A non-numeric value is NaN. */
    record Sample(long ts, double value, boolean good) {}

    /** One bucket's average. {@code exact} is false for a fallback value (see the class doc). */
    record Result(double value, boolean exact) {
        /** No Good time in the bucket: returned as a Bad point without a value. */
        static final Result BAD = new Result(Double.NaN, true);

        /** Also true for an infinite value, which no client can show (strict JSON refuses it). */
        boolean isBad() {
            return !Double.isFinite(value);
        }
    }

    /** Limits on raw reads for one pen. */
    record Budget(long pointsPerRequest, long pointsPerPen, int requestsPerPen, int seedReadsPerPen) {}

    /** What Factry is asked for beyond the aggregates passed in. Each call is one request. */
    interface Source {
        /** Number of samples (all statuses) per bucket start; buckets without samples absent. */
        Map<Long, Long> counts();

        /** The raw samples in [from, to), in time order. */
        List<Sample> raw(long from, long to);

        /** A value per bucket start for buckets the budget cannot read raw. */
        Map<Long, Double> fallback();

        /** The last sample before {@code ts}, or null when there is none. */
        Sample before(long ts);
    }

    /** What one bucket holds, from Factry's {@code last} aggregate grouped by status. */
    static final class BucketStatus {
        boolean hasGood;
        boolean hasNotGood;
        /** Number of Good-level status series ("Good", "GoodLocalOverride", ...) in the bucket. */
        int goodSeries;
        /** The last Good value; only meaningful when exactly one Good series is present. */
        double lastGood = Double.NaN;
        /**
         * The value every Good sample in the bucket holds, or NaN when not known. Set for
         * Booleans, where a Good mean of exactly 0 or 1 says so; it spares the raw read of a
         * bucket that cannot change value.
         */
        double sameGood = Double.NaN;

        void add(boolean good, double last) {
            if (good) {
                hasGood = true;
                goodSeries++;
                lastGood = last;
            } else {
                hasNotGood = true;
            }
        }

        boolean isEmpty() {
            return !hasGood && !hasNotGood;
        }

        /** Holds more than one status, so the order of its samples is unknown without raw. */
        boolean isMixed() {
            return (hasGood && hasNotGood) || goodSeries > 1;
        }

        /** Whether this bucket holds only Good samples that all equal {@code value}. */
        boolean onlyGood(double value) {
            return !isMixed() && hasGood && sameGood == value;
        }
    }

    private static final BucketStatus EMPTY = new BucketStatus();

    /** The status in force at a bucket start, as far as the plan can tell before reading raw. */
    private enum Held {
        /** No sample yet. */
        NONE, GOOD, NOT_GOOD,
        /** After a bucket holding more than one status: known only once its samples are read. */
        UNKNOWN
    }

    /** Buckets {@code first..last} (indexes) read in one raw request. */
    private record Span(int first, int last) {}

    /**
     * @param twa        Factry's twa per bucket start, in time order (fill "previous", no filter);
     *                   a null value where Factry has none (Booleans: every bucket), so that
     *                   bucket needs raw samples unless no sample falls inside it
     * @param statuses   per bucket start, which statuses it holds (buckets without samples absent)
     * @param seed       the last sample before the range, or null when there is none
     * @param periodMs   bucket length
     * @param rangeEndMs the query end; the last bucket stops here
     * @return the value per bucket start; {@link Result#BAD} for a bucket without Good time.
     *         A bucket before the first sample ever may be absent; the caller treats a missing
     *         bucket as Bad too
     */
    static Map<Long, Result> compute(
            Map<Long, Double> twa, Map<Long, BucketStatus> statuses, Sample seed,
            long periodMs, long rangeEndMs, Budget budget, Source source) {

        List<Long> buckets = new ArrayList<>(twa.keySet());
        List<Span> spans = plan(buckets, twa, statuses, seed, budget, source);

        Map<Long, Result> out = new LinkedHashMap<>();
        Map<Long, Double> fallback = null;
        Sample held = seed;
        // True after a bucket holding more than one status that was not read raw: the status
        // at the next bucket start is then unknown until the sample before it is read.
        boolean unknown = false;
        int seedReads = 0;
        int nextSpan = 0;

        int i = 0;
        while (i < buckets.size()) {
            long start = buckets.get(i);
            BucketStatus st = statuses.getOrDefault(start, EMPTY);
            boolean inSpan = nextSpan < spans.size() && spans.get(nextSpan).first() == i;
            // A raw request always gets its read: there are at most requestsPerPen of them, and
            // without it its first bucket would drop Good time yet still count as exact. Only
            // the reads for buckets outside a request are capped.
            if (unknown && (inSpan || (!st.isMixed() && seedReads < budget.seedReadsPerPen()))) {
                held = source.before(start);
                unknown = false;
                if (!inSpan) {
                    seedReads++;
                }
            }
            if (inSpan) {
                Span span = spans.get(nextSpan++);
                held = averageRaw(buckets, span, held, periodMs, rangeEndMs, source, out);
                unknown = false;
                i = span.last() + 1;
                continue;
            }

            boolean heldGood = !unknown && held != null && held.good();
            boolean heldNotGood = !unknown && held != null && !held.good();
            boolean consistent = !unknown && !st.isMixed()
                    && !(heldGood && st.hasNotGood) && !(heldNotGood && st.hasGood);
            // Without a twa a Good bucket is known only when nothing changes inside it: no
            // samples, or only samples equal to the value held (or the first ever).
            // Boxed on purpose: a primitive branch would unbox the null one and throw.
            Double good = twa.get(start) != null ? twa.get(start)
                    : st.isEmpty() && heldGood ? Double.valueOf(held.value())
                    : heldGood && st.onlyGood(held.value()) ? Double.valueOf(held.value())
                    : held == null && !unknown && st.onlyGood(st.sameGood) ? Double.valueOf(st.sameGood)
                    : null;
            if (consistent && !heldGood && !st.hasGood) {
                // Not Good the whole bucket. Before the first sample ever it is not added here;
                // the engine makes every missing bucket Bad.
                if (held != null || !st.isEmpty()) {
                    out.put(start, Result.BAD);
                }
            } else if (consistent && good != null) {
                out.put(start, new Result(good, true));
            } else if (unknown && !st.hasGood && !st.isEmpty()) {
                // Only not-Good samples and an unknown start: treat it as not Good.
                out.put(start, Result.BAD);
            } else {
                if (fallback == null) {
                    fallback = source.fallback();
                }
                Double v = fallback.get(start);
                if (v != null && !Double.isNaN(v)) {
                    out.put(start, new Result(v, false));
                }
            }

            if (st.isMixed()) {
                unknown = true;
            } else if (st.hasGood) {
                held = new Sample(start, st.lastGood, true);
                unknown = false;
            } else if (st.hasNotGood) {
                held = new Sample(start, Double.NaN, false);
                unknown = false;
            }
            i++;
        }
        return out;
    }

    /**
     * Picks the raw requests: predicts which buckets need raw samples and groups them under the
     * budget. Each request is self-contained (the sample before it is read when needed), so a
     * bucket too big to read does not keep later buckets from being read.
     */
    private static List<Span> plan(List<Long> buckets, Map<Long, Double> twa,
                                   Map<Long, BucketStatus> statuses, Sample seed, Budget budget, Source source) {
        List<Integer> wanted = new ArrayList<>();
        Held held = seed == null ? Held.NONE : seed.good() ? Held.GOOD : Held.NOT_GOOD;
        double heldValue = seed == null ? Double.NaN : seed.value();
        for (int i = 0; i < buckets.size(); i++) {
            BucketStatus st = statuses.getOrDefault(buckets.get(i), EMPTY);
            if (needsRaw(held, heldValue, st, twa.get(buckets.get(i)) != null)) {
                wanted.add(i);
            }
            if (st.isMixed()) {
                held = Held.UNKNOWN;
            } else if (st.hasGood) {
                held = Held.GOOD;
                heldValue = st.lastGood;
            } else if (st.hasNotGood) {
                held = Held.NOT_GOOD;
            }
        }
        if (wanted.isEmpty()) {
            return List.of();
        }

        long[] before = prefixCounts(buckets, source.counts()); // before[i] = samples in buckets 0..i-1
        List<Span> spans = new ArrayList<>();
        long penPoints = 0;
        int first = -1;
        int last = -1;
        // From the end backwards: what the budget leaves out is the oldest part of the range.
        for (int w = wanted.size() - 1; w >= 0; w--) {
            int i = wanted.get(w);
            if (first >= 0 && before[last + 1] - before[i] <= budget.pointsPerRequest()
                    && penPoints + before[first] - before[i] <= budget.pointsPerPen()) {
                // Extend the open request; the buckets in between come along.
                penPoints += before[first] - before[i];
                first = i;
                continue;
            }
            if (first >= 0) {
                spans.add(new Span(first, last));
                first = -1;
            }
            long own = before[i + 1] - before[i];
            if (own <= budget.pointsPerRequest() && penPoints + own <= budget.pointsPerPen()
                    && spans.size() < budget.requestsPerPen()) {
                first = i;
                last = i;
                penPoints += own;
            }
        }
        if (first >= 0) {
            spans.add(new Span(first, last));
        }
        Collections.reverse(spans);
        return spans;
    }

    /** Whether the bucket's status may change inside it, or its value may and it has no twa. */
    private static boolean needsRaw(Held held, double heldValue, BucketStatus st, boolean hasTwa) {
        if (st.isEmpty()) {
            return false;
        }
        if (st.isMixed() || held == Held.UNKNOWN) {
            return true;
        }
        if (st.hasNotGood) {
            return held == Held.GOOD;
        }
        if (held == Held.NOT_GOOD) {
            return true;
        }
        return !hasTwa && !(held == Held.GOOD ? st.onlyGood(heldValue) : st.onlyGood(st.sameGood));
    }

    private static long[] prefixCounts(List<Long> buckets, Map<Long, Long> counts) {
        long[] before = new long[buckets.size() + 1];
        for (int i = 0; i < buckets.size(); i++) {
            before[i + 1] = before[i] + counts.getOrDefault(buckets.get(i), 0L);
        }
        return before;
    }

    /** Averages the buckets of one span over their Good time; returns the sample held after it. */
    private static Sample averageRaw(List<Long> buckets, Span span, Sample held, long periodMs,
                                     long rangeEndMs, Source source, Map<Long, Result> out) {
        List<Sample> samples = source.raw(buckets.get(span.first()),
                bucketEnd(buckets.get(span.last()), periodMs, rangeEndMs));
        int s = 0;
        for (int b = span.first(); b <= span.last(); b++) {
            long from = buckets.get(b);
            long to = bucketEnd(from, periodMs, rangeEndMs);
            double sum = 0;
            long goodMs = 0;
            long t = from;
            while (s < samples.size() && samples.get(s).ts() < to) {
                Sample next = samples.get(s++);
                if (next.ts() < from) {
                    held = next;
                    continue;
                }
                if (isGoodNumber(held)) {
                    sum += held.value() * (next.ts() - t);
                    goodMs += next.ts() - t;
                }
                held = next;
                t = next.ts();
            }
            if (isGoodNumber(held)) {
                sum += held.value() * (to - t);
                goodMs += to - t;
            }
            if (goodMs > 0) {
                out.put(from, new Result(sum / goodMs, true));
            } else if (held != null) {
                out.put(from, Result.BAD); // only not-Good time (none before the first sample)
            }
        }
        return held;
    }

    private static boolean isGoodNumber(Sample s) {
        return s != null && s.good() && !Double.isNaN(s.value());
    }

    private static long bucketEnd(long start, long periodMs, long rangeEndMs) {
        return Math.min(start + periodMs, rangeEndMs);
    }
}
