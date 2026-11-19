package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.tags.config.properties.WellKnownTagProps;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.Point;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.factry.historian.gateway.PointWriterTest.point;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Every value type Ignition historizes either reaches Factry in a form that reads back
 * faithfully, or is reported as not stored. Driven through the point writer against the fake
 * Factry client, which looks measurements up, creates them and takes points like Factry does.
 */
class PointWriterValueTypesTest {

    final FakeFactry factry = new FakeFactry();
    final MeasurementCache cache = new MeasurementCache(MeasurementCacheTest.NO_WAIT);
    final PointWriter writer = new PointWriter(factry, cache, new HistorianMetrics());

    @AfterEach
    void shutDown() {
        factry.shutdown();
    }

    private StorageResult<AtomicPoint<?>> store(AtomicPoint<?>... points) {
        return writer.write(List.of(points));
    }

    /** What FactryStorageEngine.doStoreMetadata hands the cache for a tag's data type property. */
    private void tagDataType(String storedPath, DataType type) {
        cache.storeMetadata(storedPath, Map.of(WellKnownTagProps.DataType.getName(), type.toString()));
    }

    private CreateMeasurement createdFor(String name) {
        return factry.created.stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no measurement created for " + name));
    }

    private List<Point> writtenTo(String uuid) {
        return factry.sent.stream()
                .flatMap(p -> p.getPointsList().stream())
                .filter(p -> p.getMeasurementUUID().equals(uuid))
                .toList();
    }

    /** The first point sent for a measurement the writer created. */
    private Point writtenFor(String measurementName) {
        return writtenTo("new-" + measurementName).stream()
                .findFirst()
                .orElseThrow(() -> new AssertionError("nothing written for " + measurementName));
    }

    /** Runs the action and returns what the writer logged meanwhile. */
    private static String logOf(Runnable action) {
        try (LogCapture log = new LogCapture(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME, null)) {
            action.run();
            return log.text();
        }
    }

    private static int count(String haystack, String needle) {
        Matcher m = Pattern.compile(Pattern.quote(needle)).matcher(haystack);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    /** Every value class DataPointFactory accepts; AtomicPoint is sealed, so nothing else can arrive. */
    static Stream<Object> everyAtomicValueType() {
        return Stream.of(true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.5, "text", new Date(7_000L));
    }

    @ParameterizedTest
    @MethodSource("everyAtomicValueType")
    void everyValueTypeIsWrittenBeforeItIsReportedAsStored(Object value) {
        String tag = "Types/" + value.getClass().getSimpleName();
        AtomicPoint<?> point = point(tag, value);

        StorageResult<AtomicPoint<?>> result = store(point);

        assertEquals(List.of(point), result.successfulData());
        assertTrue(writtenFor("default/" + tag).hasValue(), "nothing was sent for " + value.getClass());
    }

    @Test
    void dateIsStoredAsEpochMillisInANumberMeasurement() {
        long when = 1_794_493_871_153L;
        AtomicPoint<?> date = point("Line/LastBatchStart", new Date(when));

        StorageResult<AtomicPoint<?>> result = store(date);

        assertEquals("number", createdFor("default/Line/LastBatchStart").getDataType());
        Point sent = writtenFor("default/Line/LastBatchStart");
        assertEquals(Value.KindCase.NUMBER_VALUE, sent.getValue().getKindCase());
        assertEquals((double) when, sent.getValue().getNumberValue());
        assertEquals("Good", sent.getStatus());
        assertEquals(List.of(date), result.successfulData());
    }

    @Test
    void dateTimeTagIsMarkedAsDateTimeOnItsMeasurement() {
        // The read side only gets tag paths, so the measurement must say its numbers are dates.
        tagDataType("default/Line/Stamp", DataType.DateTime);
        // A Date stored without metadata (for example from a script) is marked too.
        store(point("Line/Stamp", new Date(7_000L)), point("Script/Stamp", new Date(7_000L)),
                point("Line/Speed", 1.5));

        for (String name : List.of("default/Line/Stamp", "default/Script/Stamp")) {
            Struct settings = createdFor(name).getSettings();
            assertEquals("DateTime", settings.getFieldsMap()
                    .get(MeasurementCache.IGNITION_DATA_TYPE_SETTING).getStringValue(), name);
        }
        assertEquals(0, createdFor("default/Line/Speed").getSettings().getFieldsCount());
    }

    @Test
    void booleanTagGetsABooleanMeasurementWhenValuesArriveAsLong() {
        // Store and Forward persists Boolean values as Int8, so they come back as Long 0/1. The tag's
        // data type arrives as metadata before the first value.
        tagDataType("default/Test/Flag", DataType.Boolean);

        StorageResult<AtomicPoint<?>> result = store(point("Test/Flag", 1L));

        assertEquals("boolean", createdFor("default/Test/Flag").getDataType());
        assertTrue(result.failedData().isEmpty());
    }

    @Test
    void booleanTagKeepsItsTypeWhenTheFirstCreateFails() {
        tagDataType("default/Test/Flag", DataType.Boolean);
        factry.createFailure = io.grpc.Status.UNAVAILABLE;
        StorageResult<AtomicPoint<?>> failed = store(point("Test/Flag", 1L, 1_000L));
        assertFalse(failed.failedData().isEmpty(), "the flush is left for store and forward to retry");

        factry.createFailure = null;
        store(point("Test/Flag", 1L, 1_000L));

        CreateMeasurement created = createdFor("default/Test/Flag");
        assertEquals("boolean", created.getDataType());
        assertEquals("Boolean", created.getSettings().getFieldsMap()
                .get(MeasurementCache.IGNITION_DATA_TYPE_SETTING).getStringValue());
    }

    @Test
    void tagKeepsItsDescriptionAndUnitsWhenTheFirstCreateFails() {
        cache.storeMetadata("default/Motor/Speed", Map.of(
                WellKnownTagProps.DataType.getName(), DataType.Float8.toString(),
                "engUnit", "rpm",
                "description", "Motor speed"));
        factry.createFailure = io.grpc.Status.UNAVAILABLE;
        StorageResult<AtomicPoint<?>> failed = store(point("Motor/Speed", 1450.0, 1_000L));
        assertFalse(failed.failedData().isEmpty(), "the flush is left for store and forward to retry");

        factry.createFailure = null;
        store(point("Motor/Speed", 1450.0, 1_000L));

        CreateMeasurement created = createdFor("default/Motor/Speed");
        assertEquals("Motor speed", created.getDescription());
        assertEquals("rpm", created.getAttributes().getFieldsMap().get("Config")
                .getStructValue().getFieldsMap().get("UoM").getStringValue());
        assertEquals("Float8", created.getSettings().getFieldsMap()
                .get(MeasurementCache.IGNITION_DATA_TYPE_SETTING).getStringValue());
    }

    @Test
    void booleanTagKeepsItsTypeWhenItsMeasurementIsCreatedAgain() {
        factry.add("default/Test/Flag", "flag-uuid");
        tagDataType("default/Test/Flag", DataType.Boolean);
        store(point("Test/Flag", 1L, 1_000L));
        assertEquals(1, writtenTo("flag-uuid").size());

        // The measurement is deleted in Factry; a rejected write evicts it from the cache.
        factry.measurements.removeIf(m -> m.getUuid().equals("flag-uuid"));
        cache.evictByUUID("flag-uuid");
        store(point("Test/Flag", 0L, 2_000L));

        assertEquals("boolean", createdFor("default/Test/Flag").getDataType());
    }

    @Test
    void nanAndInfinityAreStoredAsBadPointsWithoutAValue() {
        String log = logOf(() -> {
            store(point("Pump/Flow", Double.NaN, 1_000L), point("Pump/Speed", Float.POSITIVE_INFINITY, 1_000L));
            store(point("Pump/Flow", Double.NaN, 2_000L));
        });

        for (String name : List.of("default/Pump/Flow", "default/Pump/Speed")) {
            Point sent = writtenFor(name);
            assertEquals("Bad", sent.getStatus(), name);
            // Factry turns NaN into 0 and keeps an Uncertain status; no value at all keeps it honest.
            assertFalse(sent.hasValue(), name);
        }
        assertEquals(1, count(log, "Tag 'default/Pump/Flow' has value NaN"),
                "warn once per tag, not per point:\n" + log);
    }

    @Test
    void nanInsideAnArrayIsStoredAsABadPointWithoutAValue() {
        factry.add("default/Line/Temps", "temps-uuid");
        cache.refresh(factry);

        String log = logOf(() -> {
            store(point("Line/Temps[0]", 1.0, 1_000L), point("Line/Temps[1]", Double.NaN, 1_000L));
            store(point("Line/Temps[1]", Double.NaN, 1_500L));
            store(point("Line/Temps[1]", 2.0, 2_000L));
        });

        List<Point> rows = writtenTo("temps-uuid");
        Point nanRow = rows.stream().filter(p -> p.getTimestamp().getSeconds() == 1).findFirst().orElseThrow();
        assertEquals("Bad", nanRow.getStatus());
        // Factry stores an array with a NaN element as an empty array; send no value at all.
        assertFalse(nanRow.hasValue());
        Point nextRow = rows.stream().filter(p -> p.getTimestamp().getSeconds() == 2).findFirst().orElseThrow();
        assertEquals("Good", nextRow.getStatus());
        assertEquals(List.of(1.0, 2.0), nextRow.getValue().getListValue().getValuesList().stream()
                .map(Value::getNumberValue).toList());
        assertEquals(1, count(log, "Array tag 'default/Line/Temps' has element value NaN"),
                "warn once per tag, not per point:\n" + log);
    }

    @Test
    void longAbove2pow53IsSentAsTheNearestDoubleAndWarnedOncePerTag() {
        long big = (1L << 53) + 1;
        String log = logOf(() -> {
            store(point("Counter/Total", big, 1_000L), point("Counter/Small", 42L, 1_000L));
            store(point("Counter/Total", big, 2_000L));
        });

        assertEquals((double) (1L << 53), writtenFor("default/Counter/Total").getValue().getNumberValue());
        assertEquals(1, count(log, "Tag 'default/Counter/Total' has value " + big),
                "warn once per tag, not per point:\n" + log);
        assertEquals(0, count(log, "default/Counter/Small"), log);
    }
}
