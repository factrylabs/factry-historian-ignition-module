package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.gateway.api.query.browsing.RootBrowsePublisher;
import com.inductiveautomation.ignition.common.QualifiedPath;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scheduled cache sync (#34): every 30 s it asks Factry only for this collector's measurements
 * changed since the last one it saw; every 10 minutes it reloads everything. Writes still follow a
 * measurement paused, resumed or deleted in Factry from the next tick.
 */
class MeasurementCacheSyncTest {

    private static final long T0 = 1_794_480_000_000L;

    private final FakeFactry factry = new FakeFactry();
    private final MeasurementCache cache = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
    private final HistorianMetrics metrics = new HistorianMetrics();
    private final PointWriter writer = new PointWriter(factry, cache, metrics);
    private long now = T0;

    @BeforeEach
    void start() {
        factry.add("default/Tank/Temp", "temp-uuid");
        cache.now = () -> now;
        cache.refresh(factry);
        factry.fullLoads = 0;
    }

    @AfterEach
    void stop() {
        factry.shutdown();
    }

    private void tick() {
        now += 30_000;
        cache.sync(factry);
    }

    private List<String> write(String tag, double value) {
        int before = factry.sent.size();
        writer.write(List.<AtomicPoint<?>>of(PointWriterTest.point(tag, value, now)));
        return factry.sent.subList(before, factry.sent.size()).stream()
                .flatMap(p -> p.getPointsList().stream()).map(Point::getMeasurementUUID).toList();
    }

    @Test
    void aTickAsksOnlyForThisCollectorsChanges() {
        tick();

        assertEquals(0, factry.fullLoads, "no download of every measurement");
        assertEquals(1, factry.measurementRequests.size());
        MeasurementRequest request = factry.measurementRequests.get(0);
        assertEquals(FakeFactry.OWN, request.getCollectorUUID());
        assertEquals(factry.measurements.get(0).getUpdatedAt(), request.getSince(),
                "since the newest change the cache has seen, by Factry's clock");
    }

    @Test
    void aPausedMeasurementIsRefusedFromTheNextTickAndWrittenAgainWhenResumed() {
        assertEquals(List.of("temp-uuid"), write("Tank/Temp", 1.0));

        factry.setStatus("temp-uuid", "Paused");
        tick();
        assertEquals(List.of(), write("Tank/Temp", 2.0));
        assertEquals(1, metrics.getPausedDroppedPointCount());

        factry.setStatus("temp-uuid", "Active");
        tick();
        assertEquals(List.of("temp-uuid"), write("Tank/Temp", 3.0));
        assertTrue(factry.createdNames.isEmpty());
        assertEquals(0, factry.fullLoads);
    }

    @Test
    void aDeletedMeasurementIsCreatedAgainFromTheNextTick() {
        factry.setStatus("temp-uuid", "Deleted");
        tick();

        assertEquals(List.of("new-default/Tank/Temp"), write("Tank/Temp", 1.0));
        assertEquals(List.of("default/Tank/Temp"), factry.createdNames);
    }

    @Test
    void everyTenMinutesATickReloadsEverything() {
        factry.add("default/Other/Flow", "other-uuid", FakeFactry.OTHER, "Active");

        now = T0 + 9 * 60_000;
        cache.sync(factry);
        assertEquals(0, factry.fullLoads);
        assertTrue(cache.getAllMeasurements().stream().noneMatch(m -> m.getUuid().equals("other-uuid")));

        now = T0 + 10 * 60_000;
        cache.sync(factry);
        assertTrue(factry.fullLoads > 0);
        assertTrue(cache.getAllMeasurements().stream().anyMatch(m -> m.getUuid().equals("other-uuid")),
                "another collector's new measurement shows up in browse within 10 minutes");
    }

    @Test
    void aTickReloadsEverythingWhileNoReloadHasWorked() {
        MeasurementCache fresh = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
        fresh.now = () -> now;

        fresh.sync(factry);

        assertTrue(factry.fullLoads > 0, "the startup refresh failed, so the first tick loads everything");
        assertEquals(1, fresh.size());
    }

    @Test
    void aNewTagShowsInBrowseUnderItsCollectorAtOnce() {
        write("Line/Speed", 4.0);

        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        FactryQueryEngine engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings,
                factry, cache, new HistorianMetrics());
        QualifiedPath root = QualifiedPath.parseSafe("histprov:Factry:/folder:Measurements");
        RootBrowsePublisher publisher = new RootBrowsePublisher(root);
        engine.doBrowse(root, null, publisher);

        // FakeFactry names no collectors, so the folder is the collector's UUID.
        List<String> folders = publisher.getResults().getResults().stream()
                .map(r -> r.getPath().getLastPathComponent()).toList();
        assertEquals(List.of("collector"), folders, "not a 'default' folder outside the collector's");
        assertFalse(factry.createdNames.isEmpty());
    }
}
