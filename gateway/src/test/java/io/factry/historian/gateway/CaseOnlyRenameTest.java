package io.factry.historian.gateway;

import ch.qos.logback.classic.Level;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import io.factry.historian.proto.Point;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Renaming a tag in letter case only ({@code Tank/Flow} to {@code Tank/flow}) must not split its
 * history (#38). Factry measurement names are case-sensitive, so the new spelling has no exact
 * match; Factry's name search ignores case. When no measurement has the exact name, the one
 * whose name matches ignoring case is the tag's, for writing and for reading. Several such
 * names are not guessed between.
 */
class CaseOnlyRenameTest {

    private static final long T0 = 1_794_480_000_000L;
    private static final Map<String, Double> VALUES = Map.of(
            "flow-uuid", 1.0, "lower-uuid", 2.0, "upper-uuid", 3.0, "b-flow", 22.0);

    private final FakeFactry factry = new FakeFactry();
    private final MeasurementCache cache = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
    private final PointWriter writer = new PointWriter(factry, cache, new HistorianMetrics());
    private FactryQueryEngine engine;

    @BeforeEach
    void start() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, factry, cache,
                new HistorianMetrics());
        factry.respond = CaseOnlyRenameTest::answer;
    }

    @AfterEach
    void stop() {
        factry.shutdown();
    }

    private static QueryTimeseriesResponse answer(QueryTimeseriesRequest request) {
        QueryTimeseriesResponse.Builder reply = QueryTimeseriesResponse.newBuilder();
        for (String uuid : request.getMeasurementUUIDsList()) {
            reply.addSeries(Series.newBuilder().setMeasurementUUID(uuid)
                    .setTags(Struct.newBuilder().putFields("status", Value.newBuilder().setStringValue("Good").build()))
                    .addDataPoints(SeriesPoint.newBuilder().setTimestamp(T0 - 30_000)
                            .setValue(Value.newBuilder().setNumberValue(VALUES.getOrDefault(uuid, -1.0)))));
        }
        return reply.build();
    }

    private void write(String tag) {
        AtomicPoint<?> point = PointWriterTest.point(tag, 5.0);
        assertTrue(writer.write(List.of(point)).error().isEmpty());
    }

    private List<String> sentUUIDs() {
        return factry.sent.stream().flatMap(p -> p.getPointsList().stream()).map(Point::getMeasurementUUID).toList();
    }

    /** The value a raw read gives a path, or null when it gives none. */
    private Object read(String path) throws Exception {
        IgnitionHistoryQuery.Dataset ds = IgnitionHistoryQuery.rawPaths(engine, T0 - 60_000, T0, path);
        return ds.rows.stream().map(row -> row[1]).filter(v -> v != null).findFirst().orElse(null);
    }

    // Writing

    @Test
    void aTagRenamedInCaseOnlyKeepsWritingToItsMeasurement() {
        factry.add("default/Tank/Flow", "flow-uuid");
        cache.refresh(factry);

        write("Tank/flow");

        assertEquals(List.of("flow-uuid"), sentUUIDs());
        assertEquals(List.of(), factry.createdNames, "no second measurement for the new spelling");
    }

    @Test
    void aMeasurementTheCacheDoesNotHaveYetIsFoundIgnoringCase() {
        cache.refresh(factry);
        factry.add("default/Tank/Flow", "flow-uuid");

        write("Tank/flow");

        assertEquals(List.of("flow-uuid"), sentUUIDs());
        assertEquals(List.of(), factry.createdNames);
    }

    @Test
    void theExactNameWins() {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/flow", "lower-uuid");
        cache.refresh(factry);

        write("Tank/flow");
        write("Tank/Flow");

        assertEquals(List.of("lower-uuid", "flow-uuid"), sentUUIDs());
    }

    @Test
    void severalNamesThatMatchIgnoringCaseAreNotGuessedBetween() {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/FLOW", "upper-uuid");
        cache.refresh(factry);

        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), Level.INFO)) {
            write("Tank/flow");
            write("Tank/flow");

            assertEquals(List.of("default/Tank/flow"), factry.createdNames, "created as before");
            assertEquals(List.of("new-default/Tank/flow", "new-default/Tank/flow"), sentUUIDs());
            assertEquals(1, log.text().split("match 'default/Tank/flow' ignoring case", -1).length - 1, log.text());
        }
    }

    @Test
    void anotherCollectorsMeasurementIsNotTakenIgnoringCase() {
        factry.add("default/Tank/Flow", "flow-uuid", FakeFactry.OTHER, "Active");
        cache.refresh(factry);

        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), Level.INFO)) {
            write("Tank/flow");

            assertEquals(List.of("default/Tank/flow"), factry.createdNames, "this collector's own, as before");
            assertEquals(List.of("new-default/Tank/flow"), sentUUIDs());
            assertFalse(log.text().contains("belongs to another collector"), log.text());
        }
    }

    // Reading

    @Test
    void aTagRenamedInCaseOnlyReadsItsHistory() throws Exception {
        factry.add("default/Tank/Flow", "flow-uuid");
        cache.refresh(factry);

        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Tank/flow"));
    }

    @Test
    void aReadFindsAMeasurementTheCacheDoesNotHaveYetIgnoringCase() throws Exception {
        cache.refresh(factry);
        factry.add("default/Tank/Flow", "flow-uuid");

        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Tank/flow"));
    }

    @Test
    void aReadOfTheExactNameReadsThatMeasurement() throws Exception {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/flow", "lower-uuid");
        cache.refresh(factry);

        assertEquals(2.0, read("sys:siteA:/prov:default:/tag:Tank/flow"));
        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Tank/Flow"));
    }

    @Test
    void aReadOfSeveralNamesThatMatchIgnoringCaseReadsNothing() throws Exception {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/FLOW", "upper-uuid");
        cache.refresh(factry);

        assertNull(read("sys:siteA:/prov:default:/tag:Tank/flow"));
    }

    @Test
    void anotherSitesMeasurementStillReadsOnlyByItsDatabase() throws Exception {
        factry.databases.put("db-b", "site_b");
        factry.database = "db-b";
        factry.add("default/OnlyB/Flow", "b-flow", "collector-b", "Active");
        factry.database = FakeFactry.DB;
        cache.refresh(factry);

        assertNull(read("sys:siteA:/prov:default:/tag:OnlyB/flow"), "not Site B's data by the own path");
        assertEquals(22.0, read("histprov:Factry:/tag:[site_b]default/OnlyB/flow"));
    }

    @Test
    void writeAndReadAgreeWhenAnotherCollectorHasAThirdSpelling() throws Exception {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/FLOW", "upper-uuid", FakeFactry.OTHER, "Active");
        cache.refresh(factry);

        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), Level.INFO)) {
            write("Tank/flow");

            assertEquals(List.of("flow-uuid"), sentUUIDs(), "writes go to this collector's Tank/Flow");
            assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Tank/flow"), "and the tag reads them back");
            assertFalse(log.text().contains("ignoring case"), log.text());
        }
    }

    @Test
    void aMissLooksOnlyAtNamesThatDifferInCase() {
        for (int i = 0; i < 10_000; i++) {
            factry.add("default/Area/Tag" + i, "u" + i);
        }
        factry.add("default/Tank/Flow", "flow-uuid");
        cache.refresh(factry);

        for (int i = 0; i < 100; i++) {
            assertEquals("flow-uuid", cache.writeUUID("default/Tank/flow", FakeFactry.OWN));
            assertEquals("flow-uuid", cache.cachedForRead("default/Tank/flow").getUuid());
            assertNull(cache.cachedForRead("default/Arr[2]"));
        }

        assertTrue(cache.caseCandidatesExamined <= 400,
                "no walk over all 10,000 names per miss: " + cache.caseCandidatesExamined);
    }

    @Test
    void aClashThatIsGoneIsReportedAgainWhenItComesBack() {
        factry.add("default/Tank/Flow", "flow-uuid");
        factry.add("default/Tank/FLOW", "upper-uuid");
        cache.refresh(factry);

        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), Level.INFO)) {
            assertNull(cache.cachedForRead("default/Tank/flow"));
            factry.setStatus("upper-uuid", "Deleted");
            cache.refresh(factry);
            assertEquals("flow-uuid", cache.cachedForRead("default/Tank/flow").getUuid());
            factry.setStatus("upper-uuid", "Active");
            cache.refresh(factry);
            assertNull(cache.cachedForRead("default/Tank/flow"));

            assertEquals(2, log.text().split("match 'default/Tank/flow' ignoring case", -1).length - 1, log.text());
        }
    }

    @Test
    void aLookupThatFoundAnotherSpellingDoesNotWarnItFoundNothing() {
        cache.refresh(factry);
        // The search matches substrings: the measurement sits on the fifth and last page read.
        for (int i = 0; i < 300; i++) {
            if (i == 200) {
                factry.add("default/Tank/Flow", "flow-uuid");
            }
            factry.add("default/Tank/Flow/x" + i, "x" + i);
        }

        try (LogCapture log = new LogCapture(MeasurementCache.class.getName(), Level.INFO)) {
            write("Tank/flow");

            assertEquals(List.of("flow-uuid"), sentUUIDs());
            assertFalse(log.text().contains("without finding an Active measurement"), log.text());
        }
    }

    @Test
    void aMeasurementWithoutACollectorCountsAsOwnForReadsAsForWrites() throws Exception {
        factry.add("default/Tank/Flow", "flow-uuid", "", "Active");
        factry.add("default/Tank/FLOW", "upper-uuid", FakeFactry.OTHER, "Active");
        cache.refresh(factry);

        write("Tank/flow");

        assertEquals(List.of("flow-uuid"), sentUUIDs(), "the write takes the one without a collector");
        assertEquals(1.0, read("sys:siteA:/prov:default:/tag:Tank/flow"), "and the read takes the same one");
    }
}
