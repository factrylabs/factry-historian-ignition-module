package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.HistorianProperties;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.historian.gateway.util.HistorianUtilities;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.util.Flags;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Raw reads with the value before the start and the value after the end, as Ignition's own
 * historians return them, read the way Ignition reads them (HistorianQueryInterface and
 * HistoryWriter).
 */
class FactryQueryEngineBoundsTest {

    private static final long T0 = Instant.parse("2026-11-10T10:00:00Z").toEpochMilli();
    private static final long END = T0 + 60_000;
    /** queryTagHistory's includeBoundingValues=True and False, and "no seed values". */
    private static final int YES_BOUNDS = 16;
    private static final int NO_BOUNDS = 32;
    private static final int NO_SEED = 1;

    /** Refuses the reads of the values around the range with this status when set. */
    private io.grpc.Status refuseBounds;
    /** Answers the read of the range with its end included, unlike Factry. */
    private boolean endInclusive;

    private final TwaFakeFactry factry = new TwaFakeFactry() {
        @Override
        public io.factry.historian.proto.QueryTimeseriesResponse queryTimeseries(
                io.factry.historian.proto.QueryTimeseriesRequest request) {
            if (refuseBounds != null && request.getLimit() == 1) {
                throw refuseBounds.asRuntimeException();
            }
            if (endInclusive && request.getLimit() != 1 && request.hasEnd()) {
                request = request.toBuilder().setEnd(com.google.protobuf.util.Timestamps.add(request.getEnd(),
                        com.google.protobuf.util.Durations.fromMillis(1))).build();
            }
            return super.queryTimeseries(request);
        }
    };

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

    private IgnitionHistoryQuery.Dataset raw(int returnSize, int flags, String... tags) throws Exception {
        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.raw(engine(), T0, END, returnSize, flags, tags);
        assertNull(ds.error, ds::toString);
        return ds;
    }

    /** "seconds from T0 = value" per row, for the first data column. */
    private static List<String> rows(IgnitionHistoryQuery.Dataset ds) {
        List<String> out = new ArrayList<>();
        for (int r = 0; r < ds.rows.size(); r++) {
            out.add((ds.time(r) - T0) / 1000 + "=" + ds.rows.get(r)[1]);
        }
        return out;
    }

    /** A tag stored on change: 5 ten minutes before the range, 6 inside, 7 a minute after it. */
    private String onChangeTag(String name) {
        String u = factry.addMeasurement("default/" + name, "number");
        factry.add(u, T0 - 600_000, 5, "Good");
        factry.add(u, T0 + 30_000, 6, "Good");
        factry.add(u, T0 + 120_000, 7, "Good");
        return u;
    }

    @Test
    void queryTagHistory_returnsTheValuesAroundTheRangeWhenAskedFor() throws Exception {
        onChangeTag("out");

        // Before the fix: only the 30 s row. Ignition 8.3.6 sets "no seed values" on every raw
        // read through this path, so includeBoundingValues=True must win over it.
        assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"), rows(raw(-1, YES_BOUNDS, "out")));
        // Left out or False: none, as Ignition's Core Historian answers the same call.
        for (int flags : new int[]{0, NO_BOUNDS, NO_SEED}) {
            assertEquals(List.of("30=6.0"), rows(raw(-1, flags, "out")), "flags " + flags);
        }
    }

    @Test
    void queryRawPoints_inIgnition836_returnsTheValuesAroundTheRangeOnlyWhenTheOptionsAskForThem() throws Exception {
        onChangeTag("out");
        QualifiedPath path = QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out");

        // queryRawPoints(paths, startTime, endTime): Ignition turns its options into legacy query
        // flags and runs the legacy path, which sets "no seed values". includeBounds=True only
        // makes the range inclusive, and that is dropped on the way. The Core Historian gives no
        // bounds there either.
        assertEquals(List.of("30=6.0"), rows(IgnitionHistoryQuery.rawPoints(engine(),
                RawQueryOptions.builder().startTime(T0).endTime(END).addQueryKey(new RawQueryKey(path)).build())));
        assertEquals(List.of("30=6.0"), rows(IgnitionHistoryQuery.rawPoints(engine(),
                RawQueryOptions.builder().startTime(T0).endTime(END).addQueryKey(new RawQueryKey(path))
                        .includeBounds().build())));
        // A RawQueryOptions passed as its first argument can ask for them (YES_BOUND_VALUES).
        assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"), rows(IgnitionHistoryQuery.rawPoints(engine(),
                RawQueryOptions.builder().startTime(T0).endTime(END).addQueryKey(new RawQueryKey(path))
                        .addPropertyValue(HistorianProperties.YES_BOUND_VALUES, true).build())));
    }

    @Test
    void theQueryEngineApi_returnsTheValuesAroundTheRangeUnlessTheOptionsSayNo() {
        onChangeTag("out");

        // A caller of the query engine itself (another module) that sets no option gets the bounds.
        assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"),
                stream(RawQueryOptions.builder().startTime(T0).endTime(END).addQueryKey(key("out")).build()));
        assertEquals(List.of("30=6.0"), stream(RawQueryOptions.builder().startTime(T0).endTime(END)
                .addQueryKey(key("out")).addPropertyValue(HistorianProperties.NO_SEED_VALUES, true).build()));
        assertEquals(List.of("30=6.0"), stream(RawQueryOptions.builder().startTime(T0).endTime(END)
                .addQueryKey(key("out")).addPropertyValue(HistorianProperties.NO_BOUND_VALUES, true).build()));
    }

    @Test
    void aRangeWithoutChanges_stillHasItsValue() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 - 600_000, 5, "Good");
        factry.add(u, T0 + 120_000, 7, "Good");

        assertEquals(List.of("-600=5.0", "120=7.0"), rows(raw(-1, YES_BOUNDS, "out")));
    }

    @Test
    void theValueBeforeTheStart_keepsItsQuality() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 - 700_000, 4, "Good");
        factry.add(u, T0 - 600_000, 5, "Bad");
        factry.add(u, T0 + 30_000, 6, "Good");

        IgnitionHistoryQuery.Dataset ds = raw(-1, YES_BOUNDS, "out");

        assertEquals(List.of("-600=5.0", "30=6.0"), rows(ds));
        assertTrue(ds.qualities.get(0)[1].isBad(), ds::toString);
    }

    @Test
    void returnSize_countsTheValueBeforeTheStartFirst_andTheValueAfterTheEndLast() throws Exception {
        onChangeTag("out");

        assertEquals(List.of("-600=5.0"), rows(raw(1, YES_BOUNDS, "out")));
        assertEquals(List.of("-600=5.0", "30=6.0"), rows(raw(2, YES_BOUNDS, "out")));
        assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"), rows(raw(10, YES_BOUNDS, "out")));
    }

    @Test
    void severalPens_areBoundedInTwoRequests_inOneTimeOrder() throws Exception {
        onChangeTag("a");
        String b = factry.addMeasurement("default/b", "number");
        factry.add(b, T0 - 300_000, 1, "Good");
        factry.add(b, T0 + 10_000, 2, "Good");
        factry.add(b, T0 + 90_000, 3, "Good");
        String c = factry.addMeasurement("default/c", "number");
        factry.add(c, T0 + 20_000, 9, "Good");

        // The stream the engine hands to Ignition: Ignition's wide writer keeps only the first
        // row past the end, as it does for its own Core Historian.
        List<String> stream = stream(RawQueryOptions.builder().startTime(T0).endTime(END)
                .addQueryKey(key("a")).addQueryKey(key("b")).addQueryKey(key("c")).build());

        assertEquals(List.of("-600=5.0", "-300=1.0", "10=2.0", "20=9.0", "30=6.0", "90=3.0", "120=7.0"), stream);
        // One read of the range and one each for before and after, whatever the number of pens.
        assertEquals(3, factry.queries.size(), () -> factry.queries.toString());
    }

    /** "seconds from T0 = value" per point, in the order the engine hands them to Ignition. */
    private List<String> stream(RawQueryOptions options) {
        List<String> out = new ArrayList<>();
        engine().query(options, new RawPointProcessor() {
            @Override
            public boolean onInitialize(ProcessingContext<RawQueryKey, DataPointType> context) {
                return true;
            }

            @Override
            public boolean onPointAvailable(RawQueryKey key, AtomicPoint<?> point) {
                out.add((point.timestamp().toEpochMilli() - T0) / 1000 + "=" + point.value());
                return true;
            }

            @Override
            public void onKeyFailure(RawQueryKey key, QualityCode quality) {
            }

            @Override
            public void onError(QualityCode quality) {
            }

            @Override
            public void onError(Exception e) {
            }

            @Override
            public void onComplete() {
            }
        });
        return out;
    }

    private static RawQueryKey key(String tag) {
        return new RawQueryKey(QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag));
    }

    @Test
    void aPenFactryRefuses_isBadFailure_andTheOtherPensKeepTheirBounds() throws Exception {
        onChangeTag("a");
        String b = onChangeTag("b");
        factry.failingMeasurements.add(b);

        IgnitionHistoryQuery.Dataset ds = raw(-1, YES_BOUNDS, "a", "b");

        assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"), rows(ds));
        for (QualityCode[] q : ds.qualities) {
            assertEquals(QualityCode.Bad_Failure.getCode(), q[2].getCode(), ds::toString);
        }
    }

    @Test
    void whenFactryRefusesTheValuesAroundTheRange_theRangeIsStillReturned() throws Exception {
        onChangeTag("out");
        refuseBounds = io.grpc.Status.INTERNAL.withDescription("query failed");

        assertEquals(List.of("30=6.0"), rows(raw(-1, YES_BOUNDS, "out")));
    }

    @Test
    void whenFactryIsDownForTheValuesAroundTheRange_theReadFails() throws Exception {
        onChangeTag("out");
        refuseBounds = io.grpc.Status.UNAVAILABLE.withDescription("connection refused");

        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.raw(engine(), T0, END, -1, YES_BOUNDS, "out");

        assertTrue(ds.error != null && ds.error.getMessage().contains("connection refused"), ds::toString);
    }

    @Test
    void aSampleExactlyAtTheEnd_comesBackOnce() throws Exception {
        String u = factry.addMeasurement("default/out", "number");
        factry.add(u, T0 + 30_000, 6, "Good");
        factry.add(u, END, 7, "Good");

        // Factry's end is exclusive (checked live), so it is the value after the end.
        assertEquals(List.of("30=6.0", "60=7.0"), rows(raw(-1, YES_BOUNDS, "out")));
        // Also when the range read includes the end.
        endInclusive = true;
        assertEquals(List.of("30=6.0", "60=7.0"), rows(raw(-1, YES_BOUNDS, "out")));
    }

    @Test
    void beforeIgnition836_aRawReadCarriesOnlyTheCallersFlags() {
        // Historian 1.3.3 (Ignition 8.3.0 to 8.3.5) builds a plain raw read from the caller's
        // flags only (HistorianUtilities.fromLegacyQueryFlags), without "no seed values": so
        // queryTagHistory without includeBoundingValues, queryRawPoints and raw bindings get the
        // bounds there, as from the Core Historian on those versions.
        onChangeTag("out");

        for (int flags : new int[]{0, YES_BOUNDS}) {
            assertEquals(List.of("-600=5.0", "30=6.0", "120=7.0"), stream(RawQueryOptions.builder()
                    .startTime(T0).endTime(END).addQueryKey(key("out"))
                    .addPropertyValues(HistorianUtilities.fromLegacyQueryFlags(new Flags(flags))).build()),
                    "flags " + flags);
        }
        assertEquals(List.of("30=6.0"), stream(RawQueryOptions.builder().startTime(T0).endTime(END)
                .addQueryKey(key("out")).addPropertyValues(HistorianUtilities.fromLegacyQueryFlags(new Flags(NO_BOUNDS)))
                .build()));
    }
}
