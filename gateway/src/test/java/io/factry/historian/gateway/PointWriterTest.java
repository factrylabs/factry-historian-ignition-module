package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import io.factry.historian.proto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PointWriterTest {

    final FakeFactry factry = new FakeFactry();
    final MeasurementCache cache = new MeasurementCache();
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
}
