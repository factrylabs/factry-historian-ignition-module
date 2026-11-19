package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StaticHelpersTest {

    // =========================================================================
    // FactryQueryEngine.toFactryAggregateFunction
    // =========================================================================

    @Nested
    class ToFactryAggregateFunction {

        @Test
        void average() {
            assertEquals("mean", FactryQueryEngine.toFactryAggregateFunction("Average"));
        }

        @Test
        void simpleAverage() {
            assertEquals("mean", FactryQueryEngine.toFactryAggregateFunction("SimpleAverage"));
        }

        @Test
        void minimum() {
            assertEquals("min", FactryQueryEngine.toFactryAggregateFunction("Minimum"));
        }

        @Test
        void maximum() {
            assertEquals("max", FactryQueryEngine.toFactryAggregateFunction("Maximum"));
        }

        @Test
        void sum() {
            assertEquals("sum", FactryQueryEngine.toFactryAggregateFunction("Sum"));
        }

        @Test
        void count() {
            assertEquals("count", FactryQueryEngine.toFactryAggregateFunction("Count"));
        }

        @Test
        void lastValue() {
            assertEquals("last", FactryQueryEngine.toFactryAggregateFunction("LastValue"));
        }

        @Test
        void range() {
            assertEquals("spread", FactryQueryEngine.toFactryAggregateFunction("Range"));
        }

        @Test
        void variance() {
            assertEquals("variance", FactryQueryEngine.toFactryAggregateFunction("Variance"));
        }

        @Test
        void stdDev() {
            assertEquals("stddev", FactryQueryEngine.toFactryAggregateFunction("StdDev"));
        }

        @Test
        void unknownDefaultsToMean() {
            assertEquals("mean", FactryQueryEngine.toFactryAggregateFunction("UnknownAgg"));
        }
    }

    // =========================================================================
    // FactryQueryEngine.timeWindowToPeriod
    // =========================================================================

    @Nested
    class TimeWindowToPeriod {

        @Test
        void subSecond_keepsMilliseconds() {
            assertEquals("600ms", FactryQueryEngine.timeWindowToPeriod(TimeWindow.ofMillis(600)));
        }

        @Test
        void fractionalSeconds_keepsMilliseconds() {
            assertEquals("1s500ms", FactryQueryEngine.timeWindowToPeriod(TimeWindow.ofMillis(1500)));
        }

        @Test
        void ninetySeconds() {
            assertEquals("1m30s", FactryQueryEngine.timeWindowToPeriod(TimeWindow.ofSeconds(90)));
        }

        @Test
        void oneHour() {
            assertEquals("1h", FactryQueryEngine.timeWindowToPeriod(TimeWindow.ofHours(1)));
        }

        @Test
        void oneDay() {
            assertEquals("24h", FactryQueryEngine.timeWindowToPeriod(TimeWindow.ofDays(1)));
        }

        @Test
        void zero_cannotReachTheEngine() {
            // A zero period makes Factry's InfluxDB query panic (divide by zero). Ignition's
            // TimeWindow rejects a zero size, so the engine never has to convert one.
            assertThrows(IllegalArgumentException.class, () -> TimeWindow.ofMillis(0));
        }
    }

    // =========================================================================
    // FactryQueryEngine.statusToQuality
    // =========================================================================

    @Nested
    class StatusToQuality {

        @Test
        void goodStatus() {
            assertEquals(QualityCode.Good, FactryQueryEngine.statusToQuality("Good"));
        }

        @Test
        void uncertainStatus() {
            assertEquals(QualityCode.Uncertain, FactryQueryEngine.statusToQuality("Uncertain"));
        }

        @Test
        void badStatus() {
            assertEquals(QualityCode.Bad, FactryQueryEngine.statusToQuality("Bad"));
        }

        @Test
        void nullDefaultsToGood() {
            assertEquals(QualityCode.Good, FactryQueryEngine.statusToQuality(null));
        }

        @Test
        void unknownDefaultsToGood() {
            assertEquals(QualityCode.Good, FactryQueryEngine.statusToQuality("SomethingElse"));
        }
    }

    // =========================================================================
    // FactryQueryEngine.protoValueToJava
    // =========================================================================

    @Nested
    class ProtoValueToJava {

        @Test
        void boolValue() {
            Value v = Value.newBuilder().setBoolValue(true).build();
            assertEquals(true, FactryQueryEngine.protoValueToJava(v));
        }

        @Test
        void numberValue() {
            Value v = Value.newBuilder().setNumberValue(42.5).build();
            assertEquals(42.5, FactryQueryEngine.protoValueToJava(v));
        }

        @Test
        void stringValue() {
            Value v = Value.newBuilder().setStringValue("hello").build();
            assertEquals("hello", FactryQueryEngine.protoValueToJava(v));
        }

        @Test
        void nullValue_returnsNull() {
            assertNull(FactryQueryEngine.protoValueToJava(null));
        }

        @Test
        void nullKind_returnsNull() {
            Value v = Value.newBuilder().setNullValueValue(0).build();
            assertNull(FactryQueryEngine.protoValueToJava(v));
        }
    }

    // =========================================================================
    // MeasurementCache.toFactryDataType
    // =========================================================================

    @Nested
    class ToFactryDataType {

        @Test
        void booleanValue() {
            assertEquals("boolean", MeasurementCache.toFactryDataType(true));
        }

        @Test
        void integerValue() {
            assertEquals("number", MeasurementCache.toFactryDataType(42));
        }

        @Test
        void doubleValue() {
            assertEquals("number", MeasurementCache.toFactryDataType(3.14));
        }

        @Test
        void stringValue() {
            assertEquals("string", MeasurementCache.toFactryDataType("hello"));
        }

        @Test
        void nullValue_returnsNull() {
            assertNull(MeasurementCache.toFactryDataType(null));
        }
    }

    // =========================================================================
    // FactryRecord.toIgnitionDataType
    // =========================================================================

    @Nested
    class ToIgnitionDataType {

        @Test
        void booleanType() {
            assertEquals(DataType.Boolean, FactryRecord.toIgnitionDataType("boolean"));
        }

        @Test
        void numberType() {
            assertEquals(DataType.Float8, FactryRecord.toIgnitionDataType("number"));
        }

        @Test
        void stringType() {
            assertEquals(DataType.String, FactryRecord.toIgnitionDataType("string"));
        }

        @Test
        void nullDefaultsToFloat8() {
            assertEquals(DataType.Float8, FactryRecord.toIgnitionDataType(null));
        }

        @Test
        void unknownDefaultsToFloat8() {
            assertEquals(DataType.Float8, FactryRecord.toIgnitionDataType("custom"));
        }
    }
}
