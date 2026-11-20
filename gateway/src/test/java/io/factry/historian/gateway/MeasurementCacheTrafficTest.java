package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.gateway.api.query.browsing.RootBrowsePublisher;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.browsing.Result;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What browsing and reading cost in calls to Factry (#34). Browse and reads use the measurement
 * cache; a tag the cache does not know costs one exact-name lookup, not a download of every
 * measurement, and a tag Factry does not have either is not asked for again within a minute.
 */
class MeasurementCacheTrafficTest {

    private static final long T0 = 1_794_480_000_000L;

    private final FakeFactry factry = new FakeFactry();
    private final MeasurementCache cache = new MeasurementCache(new long[0]);
    private FactryQueryEngine engine;
    private long now = T0;

    @BeforeEach
    void start() {
        factry.add("default/Line/Flow", "uuid-flow");
        cache.now = () -> now;
        cache.refresh(factry);
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, factry, cache,
                new HistorianMetrics());
        factry.fullLoads = 0;
        factry.lookups = 0;
    }

    @AfterEach
    void stop() {
        factry.shutdown();
    }

    private List<Result> browse(String path) {
        QualifiedPath root = QualifiedPath.parseSafe(path);
        RootBrowsePublisher publisher = new RootBrowsePublisher(root);
        engine.doBrowse(root, null, publisher);
        return List.copyOf(publisher.getResults().getResults());
    }

    private IgnitionHistoryQuery.Dataset read(String tag) throws Exception {
        return IgnitionHistoryQuery.wide(engine, T0 - 60_000, T0, -1, null, tag);
    }

    @Test
    void browsingAsksFactryNothing() {
        browse("histprov:Factry");
        browse("histprov:Factry:/folder:Measurements");
        // Grouped under the collector, whose name Factry gives as its UUID here.
        List<Result> leaves = browse("histprov:Factry:/folder:Measurements:/folder:collector:/folder:default:/folder:Line");

        assertEquals(List.of("Flow"), leaves.stream().map(r -> r.getDisplayPath().toString()).toList());
        assertEquals(0, factry.fullLoads, "a browse step must not reload every measurement");
        assertEquals(0, factry.lookups);
    }

    @Test
    void unknownTagIsLookedUpOnceAMinute() throws Exception {
        IgnitionHistoryQuery.Dataset first = read("Does/Not/Exist");
        IgnitionHistoryQuery.Dataset second = read("Does/Not/Exist");

        assertEquals(0, factry.fullLoads, "a read of an unknown tag must not reload every measurement");
        assertEquals(1, factry.lookups, "two reads within a minute make one exact-name lookup");
        for (IgnitionHistoryQuery.Dataset ds : List.of(first, second)) {
            assertTrue(ds.rows.stream().allMatch(row -> row[1] == null), ds.toString());
        }

        // Created meanwhile, by hand or by another collector: a minute later it reads.
        factry.add("default/Does/Not/Exist", "uuid-late");
        factry.queryResponse = onePoint("uuid-late", T0 - 30_000, 7.0);
        now += 61_000;
        IgnitionHistoryQuery.Dataset later = read("Does/Not/Exist");

        assertEquals(2, factry.lookups);
        assertEquals(0, factry.fullLoads);
        assertEquals(7.0, later.rows.get(0)[1], later.toString());
    }

    @Test
    void measurementMadeAfterTheRefreshReadsAtOnce() throws Exception {
        factry.add("default/Line/Level", "uuid-level");
        factry.queryResponse = onePoint("uuid-level", T0 - 30_000, 3.0);

        IgnitionHistoryQuery.Dataset ds = read("Line/Level");

        assertEquals(3.0, ds.rows.get(0)[1], ds.toString());
        assertEquals(QualityCode.Good, ds.qualities.get(0)[1]);
        assertEquals(0, factry.fullLoads, "a read of a new tag must not reload every measurement");
        assertEquals(1, factry.lookups, "one exact-name lookup per read, not one per step of the read");
    }

    @Test
    void elementsOfACachedArrayAskFactryNothing() throws Exception {
        factry.measurements.add(io.factry.historian.proto.Measurement.newBuilder().setName("default/Line/Arr")
                .setUuid("uuid-arr").setCollectorUUID(FakeFactry.OWN).setStatus("Active").setDatatype("[]number")
                .build());
        cache.refresh(factry);
        factry.fullLoads = 0;

        read("Line/Arr[0]");
        read("Line/Arr[1]");

        assertEquals(0, factry.lookups, "each element resolves to the cached array, no lookup of 'Arr[i]'");
        assertEquals(0, factry.fullLoads);
    }

    @Test
    void anElementOfANewArrayReadsAtOnce() throws Exception {
        factry.measurements.add(io.factry.historian.proto.Measurement.newBuilder().setName("default/Line/New")
                .setUuid("uuid-new").setCollectorUUID(FakeFactry.OWN).setStatus("Active").setDatatype("[]number")
                .build());

        read("Line/New[0]");

        assertEquals(0, factry.fullLoads);
        assertEquals(2, factry.lookups, "one for 'New[0]', one for its array 'New'");
        assertEquals("uuid-new", cache.cachedForRead("default/Line/New").getUuid());
    }

    private static QueryTimeseriesResponse onePoint(String uuid, long ts, double value) {
        return QueryTimeseriesResponse.newBuilder().addSeries(Series.newBuilder()
                .setMeasurementUUID(uuid)
                .setTags(Struct.newBuilder().putFields("status", Value.newBuilder().setStringValue("Good").build()))
                .addDataPoints(SeriesPoint.newBuilder().setTimestamp(ts)
                        .setValue(Value.newBuilder().setNumberValue(value)))).build();
    }
}
