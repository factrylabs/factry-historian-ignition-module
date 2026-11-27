package io.factry.historian.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * Which kind of point each bucket of an aggregate other than Average gets.
 *
 * <p>Every bucket needs a point. Ignition fills a bucket without one with a neighbouring value
 * marked Good, so a Bad stretch, the time before the first sample and an empty bucket of a tag
 * stored on change would all read as a value that was never there. Per bucket:
 * <ul>
 *   <li>Good samples in it: Factry's aggregate over those samples ({@link Kind#GOOD_SAMPLES}).
 *   <li>No samples, and the status held from before is Good: the value held ({@link Kind#HELD}),
 *       as Ignition's own aggregates use the value at the block start for an empty block.
 *   <li>Otherwise (only Bad or Uncertain samples, a status held that is not Good, before the
 *       first sample, or after now): {@link Kind#BAD}.
 * </ul>
 *
 * <p>The status held at an empty bucket comes from the bucket before it. After a bucket that
 * holds more than one status the order of its samples is unknown, so the last sample before the
 * empty bucket is read. For LastValue a bucket with several Good statuses is read the same way,
 * since Factry's {@code last} per status does not say which Good sample came last. Those reads
 * are capped per pen. Past the cap the empty bucket is Bad (a gap is safer than a guess) and such
 * a LastValue is not exact. Before the first bucket with samples, the sample before the range
 * says what is held; that read is not capped and happens at most once.
 *
 * <p>Free of Ignition SDK and gRPC types so it can be unit-tested without Factry.
 */
final class BucketFill {
    private BucketFill() {}

    /** One stored sample; {@code good} is its status level. */
    record Sample(long ts, Object value, boolean good) {}

    /** What one bucket holds, from Factry's {@code last} aggregate grouped by status. */
    static final class Status {
        boolean hasGood;
        boolean hasNotGood;
        /** Number of Good-level status series ("Good", "GoodLocalOverride", ...) in the bucket. */
        int goodSeries;
        /** The last Good value; with several Good series, the last of one of them. */
        Object lastGood;

        void add(boolean good, Object last) {
            if (good) {
                hasGood = true;
                goodSeries++;
                lastGood = last;
            } else {
                hasNotGood = true;
            }
        }

        /** Holds more than one status, so what it ends with is unknown. */
        boolean isMixed() {
            return (hasGood && hasNotGood) || goodSeries > 1;
        }
    }

    enum Kind { GOOD_SAMPLES, HELD, BAD }

    /**
     * One bucket. {@code value} is the value held for {@link Kind#HELD}, and the last Good value
     * for {@link Kind#GOOD_SAMPLES}. {@code exact} is false when that last Good value is a guess
     * between several Good statuses.
     */
    record Bucket(long start, Kind kind, Object value, boolean exact) {}

    /**
     * @param statuses       per bucket start, which statuses it holds (buckets without samples absent)
     * @param before         the last sample of each status before a time; empty when there is none
     * @param maxReads       most reads of {@code before} after the first bucket with samples
     * @param resolveLastGood read which Good sample of a bucket with several Good statuses came
     *                       last (LastValue needs it)
     * @return every bucket from {@code startMs} to {@code endMs}, in time order
     */
    static List<Bucket> fill(long startMs, long endMs, long periodMs, long nowMs,
                             Map<Long, Status> statuses, LongFunction<List<Sample>> before, int maxReads,
                             boolean resolveLastGood) {
        List<Bucket> out = new ArrayList<>();
        Sample held = null;
        // False until the status held is known: before the first bucket with samples, and after
        // a bucket that holds several statuses.
        boolean known = false;
        boolean started = false;
        int reads = 0;
        for (long b = startMs; b < endMs; b += periodMs) {
            if (b >= nowMs) {
                // Nothing is stored yet; Ignition's own aggregates have no value after now.
                out.add(new Bucket(b, Kind.BAD, null, true));
                continue;
            }
            Status st = statuses.get(b);
            if (st == null || (!st.hasGood && !st.hasNotGood)) {
                if (!known) {
                    if (!started) {
                        held = latest(before.apply(b), false);
                    } else if (reads < maxReads) {
                        reads++;
                        held = latest(before.apply(b), false);
                    } else {
                        held = null;
                    }
                    known = true;
                }
                out.add(held != null && held.good()
                        ? new Bucket(b, Kind.HELD, held.value(), true)
                        : new Bucket(b, Kind.BAD, null, true));
                continue;
            }
            started = true;
            Object lastGood = st.lastGood;
            boolean exact = st.goodSeries <= 1;
            if (!exact && resolveLastGood && reads < maxReads) {
                reads++;
                // Up to now, as the statuses were read: a sample stamped later is not there yet.
                List<Sample> last = before.apply(Math.min(b + periodMs, Math.min(endMs, nowMs)));
                Sample good = latest(last, true);
                if (good != null) {
                    lastGood = good.value();
                    exact = true;
                }
                held = latest(last, false);
                known = held != null;
            } else if (st.isMixed()) {
                known = false;
            } else {
                held = new Sample(b, st.hasGood ? st.lastGood : null, st.hasGood);
                known = true;
            }
            out.add(st.hasGood
                    ? new Bucket(b, Kind.GOOD_SAMPLES, lastGood, exact)
                    : new Bucket(b, Kind.BAD, null, true));
        }
        return out;
    }

    /** The latest of {@code samples} (only Good ones when {@code good}), or null. */
    private static Sample latest(List<Sample> samples, boolean good) {
        Sample out = null;
        for (Sample s : samples) {
            if ((!good || s.good()) && (out == null || s.ts() > out.ts())) {
                out = s;
            }
        }
        return out;
    }
}
