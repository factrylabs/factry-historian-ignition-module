package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.gateway.api.query.browsing.RootBrowsePublisher;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.browsing.Result;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several Sites in one Factry organisation: each Site's gateway has its own collector writing to
 * its own time-series database, and the same tag has the same measurement name at every Site
 * (#23, #52). A tag's own path reads only its own Site. Other Sites stay browsable and readable,
 * under their own collector's folder, by a node id that names their database.
 */
class SitesTest {

    private static final long T0 = 1_794_480_000_000L;
    private static final String SITE_B = "collector-b";
    private static final String OPC = "opc-collector";

    private final FakeFactry factry = new FakeFactry();
    private final MeasurementCache cache = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
    private FactryQueryEngine engine;

    @BeforeEach
    void start() {
        factry.databases.put("db-b", "site_b");
        // Site A, this gateway: its own tag, and an OPC UA collector writing to the same database.
        factry.add("default/Pump1/Flow", "a-flow");
        factry.add("default/Opc/Temp", "opc-temp", OPC, "Active");
        // Site B: the same tag, and one only Site B has.
        factry.database = "db-b";
        factry.add("default/Pump1/Flow", "b-flow", SITE_B, "Active");
        factry.add("default/OnlyB/Flow", "b-only", SITE_B, "Active");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, factry, cache,
                new HistorianMetrics());
        factry.respond = this::answer;
    }

    @AfterEach
    void stop() {
        factry.shutdown();
    }

    /** Every read answers one point per measurement it asks for, with that measurement's value. */
    private static final Map<String, Double> VALUES = Map.of(
            "a-flow", 1.0, "a-own", 3.0, "opc-temp", 5.0, "b-flow", 2.0, "b-only", 22.0, "b-hidden", 22.0, "b-new", 33.0, "x-only", 9.0);

    private QueryTimeseriesResponse answer(QueryTimeseriesRequest request) {
        QueryTimeseriesResponse.Builder reply = QueryTimeseriesResponse.newBuilder();
        for (String uuid : request.getMeasurementUUIDsList()) {
            reply.addSeries(Series.newBuilder().setMeasurementUUID(uuid)
                    .setTags(Struct.newBuilder().putFields("status", Value.newBuilder().setStringValue("Good").build()))
                    .addDataPoints(SeriesPoint.newBuilder().setTimestamp(T0 - 30_000)
                            .setValue(Value.newBuilder().setNumberValue(VALUES.getOrDefault(uuid, -1.0)))));
        }
        return reply.build();
    }

    /** The value a raw read gives a path, or null when it gives none. */
    private Object read(String path) throws Exception {
        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.rawPaths(engine, T0 - 60_000, T0, path);
        return ds.rows.stream().map(row -> row[1]).filter(v -> v != null).findFirst().orElse(null);
    }

    private List<String> uuidsRead() {
        return factry.queries.stream().flatMap(q -> q.getMeasurementUUIDsList().stream()).distinct().toList();
    }

    private Map<String, String> browse(String path) {
        QualifiedPath root = QualifiedPath.parseSafe(path);
        RootBrowsePublisher publisher = new RootBrowsePublisher(root);
        engine.doBrowse(root, null, publisher);
        return publisher.getResults().getResults().stream().collect(Collectors.toMap(
                (Result r) -> r.getPath().getLastPathComponent(),
                (Result r) -> String.valueOf(r.getPath().getPathComponent("tag"))));
    }

    @Test
    void aTagReadsItsOwnSitesMeasurement() throws Exception {
        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Pump1/Flow"));
        assertEquals(List.of("a-flow"), uuidsRead());
    }

    @Test
    void aTagOnlyAnotherSiteHasReadsNothing() throws Exception {
        assertNull(read("sys:siteA:/prov:default:/tag:OnlyB/Flow"), "not Site B's data marked Good");
        assertEquals(List.of(), uuidsRead());
    }

    @Test
    void aNewTagIsNotLookedUpInAnotherSite() throws Exception {
        factry.database = "db-b";
        factry.add("default/New/Flow", "b-new", SITE_B, "Active");

        assertNull(read("sys:siteA:/prov:default:/tag:New/Flow"));
        assertEquals(List.of(), uuidsRead());
        GetMeasurementsByFilterRequest lookup = factry.filterRequests.get(factry.filterRequests.size() - 1);
        assertEquals(List.of(FakeFactry.DB), lookup.getDatabaseUUIDsList(), "the lookup is scoped to the own Site");
    }

    @Test
    void anotherCollectorAtTheSameSiteReadsByItsPath() throws Exception {
        assertEquals(5.0, read("sys:siteA:/prov:default:/tag:Opc/Temp"));
    }

    @Test
    void everyBrowsedTagReadsTheMeasurementOfItsCollectorFolder() throws Exception {
        Map<String, String> own = browse(
                "histprov:Factry:/folder:Measurements:/folder:collector:/folder:default:/folder:Pump1");
        Map<String, String> siteB = browse(
                "histprov:Factry:/folder:Measurements:/folder:collector-b:/folder:default:/folder:Pump1");

        assertEquals(Map.of("Flow", "default/Pump1/Flow"), own);
        assertEquals(Map.of("Flow", "[site_b]default/Pump1/Flow"), siteB);
        assertEquals(1.0, read("histprov:Factry:/tag:" + own.get("Flow")));
        assertEquals(2.0, read("histprov:Factry:/tag:" + siteB.get("Flow")), "Site B's value under Site B's folder");
    }

    @Test
    void anotherSitesNewMeasurementReadsAtOnceByItsDatabaseAndName() throws Exception {
        factry.database = "db-b";
        factry.add("default/New/Flow", "b-new", SITE_B, "Active");

        assertEquals(33.0, read("histprov:Factry:/tag:[site_b]default/New/Flow"));
        GetMeasurementsByFilterRequest lookup = factry.filterRequests.get(factry.filterRequests.size() - 1);
        assertEquals(List.of("db-b"), lookup.getDatabaseUUIDsList());
    }

    @Test
    void withOneDatabaseEveryCollectorsTagReadsByItsPathBeforeTheFirstWrite() throws Exception {
        FakeFactry single = new FakeFactry();
        try {
            single.add("default/Line/Speed", "x-only", OPC, "Active");
            MeasurementCache fresh = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
            fresh.refresh(single);
            single.respond = this::answer;
            FactryQueryEngine singleEngine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry",
                    new FactryHistorianSettings(), single, fresh, new HistorianMetrics());

            IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.wide(singleEngine, T0 - 60_000, T0, -1, null,
                    "Line/Speed");

            assertEquals(9.0, ds.rows.get(0)[1], ds.toString());
        } finally {
            single.shutdown();
        }
    }

    @Test
    void aMeasurementWithoutACollectorDoesNotMakeItsDatabaseTheOwnSite() throws Exception {
        factry.database = "db-b";
        factry.add("default/Calc/Total", "x-only", "", "Active");
        cache.refresh(factry);

        assertNull(read("sys:siteA:/prov:default:/tag:OnlyB/Flow"));
        assertEquals(9.0, read("histprov:Factry:/tag:[site_b]default/Calc/Total"));
    }

    @Test
    void theDelimiterLeavesTheDatabaseNameAlone() throws Exception {
        factry.databases.put("db-b", "site.b");
        cache.refresh(factry);
        FactryHistorianSettings dots = new FactryHistorianSettings();
        dots.setDelimiter(".");
        engine.updateSettings(dots);

        Map<String, String> siteB = browse("histprov:Factry:/folder:Measurements:/folder:collector-b");

        // Flat: the name's '/' are escaped, the database name keeps its '.'.
        String id = "[site.b]" + TagPathUtil.toBrowseName("default/Pump1/Flow", ".");
        assertTrue(siteB.containsValue(id), siteB.toString());
        assertEquals(2.0, read("histprov:Factry:/tag:" + id));
    }

    @Test
    void aBrowsedTagOfAnotherSiteIsNotShadowedByTheOwnSitesName() {
        // The own Site's tag and Site B's have the same name; both are listed, each once.
        Map<String, String> siteB = browse(
                "histprov:Factry:/folder:Measurements:/folder:collector-b:/folder:default:/folder:OnlyB");
        assertEquals(Map.of("Flow", "[site_b]default/OnlyB/Flow"), siteB);
        assertTrue(browse("histprov:Factry:/folder:Measurements").keySet()
                .containsAll(List.of("collector", "collector-b", "opc-collector")));
    }

    /** One measurement of this collector in Site B's database, say made there by hand. */
    private void strayInSiteB() {
        factry.database = "db-b";
        factry.add("default/Stray/Level", "x-only");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);
    }

    @Test
    void aStrayMeasurementInAnotherDatabaseDoesNotMakeItsTagsReadByPath() throws Exception {
        strayInSiteB();

        assertNull(read("sys:siteA:/prov:default:/tag:OnlyB/Flow"), "not Site B's data marked Good");
        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Pump1/Flow"));
        assertEquals(9.0, read("sys:siteA:/prov:default:/tag:Stray/Level"), "this collector's own still reads");
        assertEquals(List.of("a-flow", "x-only"), uuidsRead());
    }

    @Test
    void withSeveralOwnDatabasesOtherCollectorsReadOnlyByDatabaseAndName() throws Exception {
        strayInSiteB();

        assertNull(read("sys:siteA:/prov:default:/tag:Opc/Temp"));
        assertEquals(5.0, read("histprov:Factry:/tag:[site_a]default/Opc/Temp"));
        assertEquals(22.0, read("histprov:Factry:/tag:[site_b]default/OnlyB/Flow"));
        assertEquals(Map.of("Temp", "[site_a]default/Opc/Temp"),
                browse("histprov:Factry:/folder:Measurements:/folder:opc-collector:/folder:default:/folder:Opc"));
    }

    @Test
    void anEmptyDatabaseListKeepsEveryMeasurement() throws Exception {
        factry.databases.clear();
        cache.refresh(factry);

        assertEquals(4, cache.getAllMeasurements().size(), "browse still lists every measurement");
        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Pump1/Flow"));
    }

    @Test
    void aMeasurementInADatabaseFactryDoesNotListIsKept() throws Exception {
        factry.database = "db-hidden";
        factry.add("default/Hidden/Flow", "x-only");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);

        assertEquals(9.0, read("sys:siteA:/prov:default:/tag:Hidden/Flow"));
        assertEquals("x-only", cache.writeUUID("default/Hidden/Flow", FakeFactry.OWN));
    }

    @Test
    void theSameTagOfThisCollectorInTwoDatabasesIsBrowsableAndReadableTwice() throws Exception {
        factry.add("default/Own/Flow", "a-own");
        factry.database = "db-b";
        factry.add("default/Own/Flow", "x-only");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);

        QualifiedPath root = QualifiedPath.parseSafe(
                "histprov:Factry:/folder:Measurements:/folder:collector:/folder:default:/folder:Own");
        RootBrowsePublisher publisher = new RootBrowsePublisher(root);
        engine.doBrowse(root, null, publisher);
        List<String> ids = publisher.getResults().getResults().stream()
                .map(r -> String.valueOf(r.getPath().getPathComponent("tag"))).sorted().toList();
        assertEquals(List.of("[site_b]default/Own/Flow", "default/Own/Flow"), ids);
        assertEquals(9.0, read("histprov:Factry:/tag:[site_b]default/Own/Flow"), "the one in site_b, not site_a's");
        assertEquals(3.0, read("histprov:Factry:/tag:[site_a]default/Own/Flow"));
        assertEquals(3.0, read("sys:siteA:/prov:default:/tag:Own/Flow"), "the first database's");
    }

    @Test
    void aLookupForAnotherSitesMeasurementStopsAtTheFirstPageThatHasIt() throws Exception {
        factry.database = "db-b";
        factry.add("default/Late/Flow", "b-new", SITE_B, "Active");
        for (int i = 0; i < 60; i++) {
            factry.add("default/Late/Flow" + i, "late-" + i, SITE_B, "Active");
        }
        factry.lookups = 0;

        assertEquals(33.0, read("histprov:Factry:/tag:[site_b]default/Late/Flow"));
        long sitePages = factry.filterRequests.stream()
                .filter(r -> r.hasKeyword() && r.getDatabaseUUIDsList().equals(List.of("db-b"))).count();
        assertEquals(1, sitePages, "one page in Site B's database, not every page up to the bound");
    }

    /** A cache and engine over {@code other}, with this test's answers to reads. */
    private FactryQueryEngine engineOver(FakeFactry other) {
        MeasurementCache fresh = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
        fresh.refresh(other);
        other.respond = this::answer;
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        return new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, other, fresh,
                new HistorianMetrics());
    }

    private static Object readWith(FactryQueryEngine e, String path) throws Exception {
        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.rawPaths(e, T0 - 60_000, T0, path);
        return ds.rows.stream().map(row -> row[1]).filter(v -> v != null).findFirst().orElse(null);
    }

    private static List<String> browseIds(FactryQueryEngine e, String path) {
        QualifiedPath root = QualifiedPath.parseSafe(path);
        RootBrowsePublisher publisher = new RootBrowsePublisher(root);
        e.doBrowse(root, null, publisher);
        return publisher.getResults().getResults().stream()
                .map(r -> String.valueOf(r.getPath().getPathComponent("tag"))).sorted().toList();
    }

    @Test
    void theDefaultDatabaseIsTheOwnSiteEvenWhenTheOnlyMeasurementIsElsewhere() throws Exception {
        // This collector's only measurement was made by hand in Site B's database.
        FakeFactry other = new FakeFactry();
        try {
            other.databases.put("db-b", "site_b");
            other.add("default/Opc/Temp", "opc-temp", OPC, "Active");
            other.database = "db-b";
            other.add("default/Stray/Level", "x-only");
            other.add("default/OnlyB/Flow", "b-only", SITE_B, "Active");
            other.defaultDatabase = FakeFactry.DB;
            FactryQueryEngine e = engineOver(other);

            assertNull(readWith(e, "sys:siteA:/prov:default:/tag:OnlyB/Flow"), "not Site B's data marked Good");
            assertEquals(5.0, readWith(e, "sys:siteA:/prov:default:/tag:Opc/Temp"));
            assertEquals(9.0, readWith(e, "sys:siteA:/prov:default:/tag:Stray/Level"));
        } finally {
            other.shutdown();
        }
    }

    @Test
    void withOneDatabaseAMeasurementInNoListedDatabaseStillReadsAndBrowses() throws Exception {
        FakeFactry other = new FakeFactry();
        try {
            other.add("default/Line/Own", "a-own");
            other.database = "db-unlisted";
            other.add("default/Line/Calc", "x-only", OPC, "Active");
            FactryQueryEngine e = engineOver(other);

            assertEquals(9.0, readWith(e, "sys:siteA:/prov:default:/tag:Line/Calc"));
            assertEquals(List.of("default/Line/Calc"), browseIds(e,
                    "histprov:Factry:/folder:Measurements:/folder:opc-collector:/folder:default:/folder:Line"));
        } finally {
            other.shutdown();
        }
    }

    @Test
    void anotherCollectorsSameNameInTwoDatabasesIsBrowsableTwice() {
        factry.databases.put("db-c", "site_c");
        factry.database = "db-c";
        factry.add("default/OnlyB/Flow", "x-only", SITE_B, "Active");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);

        assertEquals(List.of("[site_b]default/OnlyB/Flow", "[site_c]default/OnlyB/Flow"), browseIds(engine,
                "histprov:Factry:/folder:Measurements:/folder:collector-b:/folder:default:/folder:OnlyB"));
    }

    @Test
    void aRealNameThatLooksQualifiedWinsOverAnotherSitesKey() throws Exception {
        // Another collector's, in the own database: it reads by its name, so the name is its.
        factry.add("[site_b]default/OnlyB/Flow", "a-own", OPC, "Active");
        cache.refresh(factry);

        assertEquals(3.0, read("histprov:Factry:/tag:[site_b]default/OnlyB/Flow"));
    }

    @Test
    void aMissedRealNameThatLooksQualifiedIsLookedUpLiterallyFirst() throws Exception {
        // Made after the last refresh, and Site B's measurement is not cached either: both miss.
        factry.add("[site_b]default/OnlyB/Flow", "a-own");
        cache.evictByUUID("b-only");

        assertEquals(3.0, read("histprov:Factry:/tag:[site_b]default/OnlyB/Flow"));
    }

    @Test
    void measurementsInAnUnlistedDatabaseSurviveATotalOfZero() throws Exception {
        factry.database = "db-hidden";
        factry.add("default/Hidden/Flow", "x-only");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);
        factry.totalOverride = 0L;
        cache.refresh(factry);

        assertEquals(9.0, read("sys:siteA:/prov:default:/tag:Hidden/Flow"));
        assertEquals("x-only", cache.writeUUID("default/Hidden/Flow", FakeFactry.OWN));
    }

    @Test
    void aFactryThatDoesNotListItsDatabasesIsReportedOnce() {
        factry.databasesFailure = io.grpc.Status.UNIMPLEMENTED;
        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), ch.qos.logback.classic.Level.INFO)) {
            cache.refresh(factry);
            cache.refresh(factry);
            cache.refresh(factry);

            assertEquals(1, log.events().stream()
                    .filter(ev -> ev.getLevel() == ch.qos.logback.classic.Level.WARN
                            && ev.getFormattedMessage().contains("time-series databases")).count());
        }
    }

    @Test
    void withoutADatabaseListingAReadPrefersThisCollectorsMeasurementOnALaterPage() throws Exception {
        factry.databasesFailure = io.grpc.Status.UNIMPLEMENTED;
        cache.refresh(factry);
        factry.add("default/Pref/X", "b-only", SITE_B, "Active");
        for (int i = 0; i < 55; i++) {
            factry.add("default/Pref/X" + i, "pref-" + i, SITE_B, "Active");
        }
        factry.add("default/Pref/X", "x-only");

        assertEquals(9.0, read("sys:siteA:/prov:default:/tag:Pref/X"), "this collector's, not Site B's");
    }

    private void addArray(String name, String uuid, String collector) {
        factry.measurements.add(io.factry.historian.proto.Measurement.newBuilder().setName(name).setUuid(uuid)
                .setCollectorUUID(collector).setStatus("Active").setDatatype("[]number").build());
        factry.databaseOf.put(uuid, factry.database);
    }

    @Test
    void arrayElementsFollowTheSameSiteRules() throws Exception {
        addArray("default/Arr", "a-arr", FakeFactry.OWN);
        factry.database = "db-b";
        addArray("default/Arr", "b-arr", SITE_B);
        addArray("default/ArrB", "b-arr-only", SITE_B);
        factry.database = FakeFactry.DB;
        cache.refresh(factry);

        read("sys:siteA:/prov:default:/tag:Arr[1]");
        assertEquals(List.of("a-arr"), uuidsRead(), "the own Site's array");
        factry.queries.clear();
        read("histprov:Factry:/tag:[site_b]default/Arr[1]");
        assertEquals(List.of("b-arr"), uuidsRead(), "Site B's array by its database");
        factry.queries.clear();
        assertNull(read("sys:siteA:/prov:default:/tag:ArrB[0]"));
        assertEquals(List.of(), uuidsRead(), "an array only Site B has does not read by path");
    }

    @Test
    void anotherCollectorsMeasurementInAnUnlistedDatabaseDoesNotReadByTagPath() throws Exception {
        factry.database = "db-hidden";
        factry.add("default/Hidden/Flow", "b-hidden", SITE_B, "Active");
        factry.database = FakeFactry.DB;
        factry.defaultDatabase = FakeFactry.DB;
        cache.refresh(factry);

        assertNull(read("sys:siteA:/prov:default:/tag:Hidden/Flow"), "its Site is unknown: not by tag path");
    }

    @Test
    void aFailedRestCallKeepsTheLastDefaultDatabase() throws Exception {
        // This collector's only measurement is in Site B's database; its default is site_a.
        FakeFactry other = new FakeFactry();
        try {
            other.databases.put("db-b", "site_b");
            other.database = "db-b";
            other.add("default/Stray/Level", "x-only");
            other.add("default/OnlyB/Flow", "b-only", SITE_B, "Active");
            other.database = FakeFactry.DB;
            other.defaultDatabase = FakeFactry.DB;
            MeasurementCache fresh = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
            fresh.refresh(other);
            other.respond = this::answer;
            FactryHistorianSettings settings = new FactryHistorianSettings();
            settings.setDelimiter("/");
            FactryQueryEngine e = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings,
                    other, fresh, new HistorianMetrics());
            assertNull(readWith(e, "sys:siteA:/prov:default:/tag:OnlyB/Flow"));

            other.defaultDatabaseFails = true;
            fresh.refresh(other);

            assertNull(readWith(e, "sys:siteA:/prov:default:/tag:OnlyB/Flow"), "Site B's data after a REST blip");
        } finally {
            other.shutdown();
        }
    }

    @Test
    void factryAnsweringNoDefaultDropsTheLastOne() throws Exception {
        factry.defaultDatabase = "db-b";
        cache.refresh(factry);
        assertEquals(22.0, read("sys:siteA:/prov:default:/tag:OnlyB/Flow"), "db-b is the default here");

        factry.defaultDatabase = null;
        cache.refresh(factry);
        factry.queries.clear();

        assertNull(read("sys:siteA:/prov:default:/tag:OnlyB/Flow"));
    }
}
