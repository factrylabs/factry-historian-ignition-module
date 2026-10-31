package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import io.factry.historian.proto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PointWriterTest {

    final FakeFactry factry = new FakeFactry();
    final MeasurementCache cache = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
    final HistorianMetrics metrics = new HistorianMetrics();
    final PointWriter writer = new PointWriter(factry, cache, metrics);

    @AfterEach
    void shutDown() {
        factry.shutdown();
    }

    static AtomicPoint<?> point(String tag, Object value) {
        return point(tag, value, 1_000L);
    }

    static AtomicPoint<?> point(String tag, Object value, long epochMillis) {
        return DataPointFactory.createAtomicPoint(value, QualityCode.Good, Instant.ofEpochMilli(epochMillis),
                QualifiedPath.parseSafe("prov:default:/tag:" + tag));
    }

    /** The measurement UUIDs of every point sent to Factry, in order. */
    List<String> sentUUIDs() {
        return factry.sent.stream()
                .flatMap(p -> p.getPointsList().stream())
                .map(Point::getMeasurementUUID)
                .collect(Collectors.toList());
    }

    @Test
    void aFlushIsSentInOneCall() {
        factry.add("default/Tank/Temp", "temp-uuid");
        factry.add("default/Tank/Level", "level-uuid");
        cache.refresh(factry);

        StorageResult<AtomicPoint<?>> result = writer.write(List.of(point("Tank/Temp", 21.5), point("Tank/Level", 3.2)));

        assertEquals(StorageResult.Type.SUCCESS, result.type());
        assertEquals(1, factry.sent.size());
        assertEquals(List.of("temp-uuid", "level-uuid"), sentUUIDs());
        assertEquals(2, metrics.getStorePointCount());
    }

    @Test
    void anArrayIsSentAsOneMergedValue() {
        factry.add("default/Tank/Profile", "profile-uuid");
        cache.refresh(factry);

        writer.write(List.of(point("Tank/Profile[0]", 1), point("Tank/Profile[1]", 2)));
        writer.write(List.of(point("Tank/Profile[1]", 5, 2_000L)));

        Point last = factry.sent.get(1).getPoints(0);
        assertEquals(List.of(1.0, 5.0), last.getValue().getListValue().getValuesList().stream()
                .map(com.google.protobuf.Value::getNumberValue).collect(Collectors.toList()));
    }

    @Nested
    class MeasurementThatIsNotActive {

        @Test
        void aloneItsPointsAreDiscardedWithoutALookupPerFlush() {
            factry.add("default/Tank/Temp", "disc-uuid", "Discovered");
            factry.add("default/Tank/Level", "level-uuid");
            cache.refresh(factry);

            for (int i = 0; i < 3; i++) {
                StorageResult<AtomicPoint<?>> result = writer.write(List.of(point("Tank/Temp", 1.0)));
                assertEquals(StorageResult.Type.SUCCESS, result.type(), "handled, never retried");
            }

            assertEquals(0, factry.lookups);
            assertTrue(factry.sent.isEmpty());
            assertTrue(factry.createdNames.isEmpty());
            assertEquals(3, metrics.getDiscardedPointCount());
        }

        @Test
        void inAMixedFlushTheOthersAreSent() {
            factry.add("default/Tank/Temp", "disc-uuid", "Discovered");
            factry.add("default/Tank/Level", "level-uuid");
            cache.refresh(factry);

            StorageResult<AtomicPoint<?>> result = writer.write(List.of(point("Tank/Temp", 1.0), point("Tank/Level", 2.0)));

            assertEquals(StorageResult.Type.SUCCESS, result.type());
            assertEquals(List.of("level-uuid"), sentUUIDs());
            assertEquals(1, metrics.getDiscardedPointCount());
        }

        @Test
        void foundByTheLookupDuringAFlushItIsDiscardedToo() {
            factry.add("default/Tank/Level", "level-uuid");
            cache.refresh(factry);
            // Discovered after the last refresh.
            factry.add("default/Tank/Temp", "disc-uuid", "Discovered");

            StorageResult<AtomicPoint<?>> result = writer.write(List.of(point("Tank/Temp", 1.0), point("Tank/Level", 2.0)));

            assertEquals(StorageResult.Type.SUCCESS, result.type());
            assertEquals(List.of("level-uuid"), sentUUIDs());
            assertTrue(factry.createdNames.isEmpty());
            assertEquals(1, metrics.getDiscardedPointCount());
        }

        @Test
        void anArrayIsDiscardedWithoutACreationAndTheOthersAreSent() throws InterruptedException {
            factry.add("default/Tank/Profile", "disc-uuid", "Discovered");
            factry.add("default/Tank/Level", "level-uuid");
            cache.refresh(factry);

            for (int i = 0; i < 3; i++) {
                StorageResult<AtomicPoint<?>> result = writer.write(List.of(
                        point("Tank/Profile[0]", 1, 1_000L + i), point("Tank/Level", 2.0, 1_000L + i)));
                assertEquals(StorageResult.Type.SUCCESS, result.type());
            }
            Thread.sleep(100);

            assertEquals(List.of("level-uuid", "level-uuid", "level-uuid"), sentUUIDs());
            assertTrue(factry.createdNames.isEmpty(), "no creator thread for a refused array");
            assertEquals(3, metrics.getDiscardedPointCount());
        }

        @Test
        void aNameWithManySubstringMatchesCostsNoLookupPerFlush() {
            factry.add("default/T", "disc-uuid", "Discovered");
            for (int i = 0; i < 300; i++) {
                factry.add("default/T" + i, "u" + i);
            }
            cache.refresh(factry);

            StorageResult<AtomicPoint<?>> result = writer.write(List.of(point("T", 1.0), point("T1", 2.0)));

            assertEquals(StorageResult.Type.SUCCESS, result.type());
            assertEquals(0, factry.lookups);
            assertEquals(List.of("u1"), sentUUIDs());
        }

        @Test
        void itIsLoggedOnceUntilTheTagIsWrittenAgain() {
            factry.add("default/Tank/Temp", "disc-uuid", "Discovered");
            cache.refresh(factry);
            RefusalLog log = writer.refusalLog();

            writer.write(List.of(point("Tank/Temp", 1.0)));
            assertTrue(log.isReported("default/Tank/Temp"));
            writer.write(List.of(point("Tank/Temp", 2.0, 2_000L)));
            assertTrue(log.isReported("default/Tank/Temp"));

            factry.setStatus("disc-uuid", "Active");
            cache.refresh(factry);
            writer.write(List.of(point("Tank/Temp", 3.0, 3_000L)));

            assertFalse(log.isReported("default/Tank/Temp"), "a later refusal is logged again");
            assertEquals(List.of("disc-uuid"), sentUUIDs());
        }

        @Test
        void clearingTheRefusalsLogsThemAgain() {
            factry.add("default/Tank/Temp", "disc-uuid", "Discovered");
            cache.refresh(factry);
            writer.write(List.of(point("Tank/Temp", 1.0)));

            writer.clearRefusals();

            assertFalse(writer.refusalLog().isReported("default/Tank/Temp"));
        }
    }
}
