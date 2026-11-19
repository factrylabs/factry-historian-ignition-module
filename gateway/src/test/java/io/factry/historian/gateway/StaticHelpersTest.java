package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.TimeWindow;
import io.factry.historian.proto.QueryTimeseriesRequest;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StaticHelpersTest {

    // =========================================================================
    // FactryQueryEngine.toFactryAggregateFunction
    // =========================================================================

    @Nested
    class ToFactryAggregateFunction {

        @Test
        void average_isTimeWeighted() {
            // Ignition's "Average" is time-weighted; "SimpleAverage" is the mean of the samples.
            assertEquals("twa", FactryQueryEngine.toFactryAggregateFunction("Average"));
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
    // FactryQueryEngine.aggregateRequest
    // =========================================================================

    @Nested
    class AggregateRequest {

        @Test
        void timeWeightedAverage_holdsPreviousValueWithoutStatusFilter() {
            // Checked against Factry 8.2.3: twa is only right with fill "previous" (a tag at 100
            // for 1 minute of the hour gives 1.666667; fill "none" gives 100). A tag filter
            // status=Good holds the last Good value through an outage, and misses Good
            // sub-statuses such as "GoodLocalOverride" (exact match), so there is none: Bad
            // time is masked from a status aggregate instead (GoodTimeAverage).
            QueryTimeseriesRequest req = FactryQueryEngine.aggregateRequest("u1", "twa", "10m").build();

            assertEquals("twa", req.getAggregation().getName());
            assertEquals("10m", req.getAggregation().getPeriod());
            assertEquals("previous", req.getAggregation().getFillType());
            assertEquals(0, req.getGroupByCount());
            assertFalse(req.hasTags());
            assertEquals(List.of("u1"), req.getMeasurementUUIDsList());
        }

        @Test
        void bucketStatus_isLastPerStatusWithoutFill() {
            QueryTimeseriesRequest req = FactryQueryEngine.bucketStatusRequest("u1", "10m").build();

            assertEquals("last", req.getAggregation().getName());
            assertEquals("10m", req.getAggregation().getPeriod());
            assertEquals("none", req.getAggregation().getFillType());
            assertEquals(List.of("status"), req.getGroupByList());
        }

        @Test
        void sampleBefore_isTheLatestSamplePerStatusBeforeStart() {
            // Factry's end is exclusive and its limit applies per series.
            QueryTimeseriesRequest req = FactryQueryEngine.sampleBeforeRequest("u1", 5_000);

            assertEquals(5_000, req.getEnd().getSeconds() * 1000);
            assertTrue(req.getDesc());
            assertEquals(1, req.getLimit());
            assertEquals(List.of("status"), req.getGroupByList());
            assertFalse(req.hasAggregation());
        }

        @Test
        void otherAggregates_groupByStatusWithoutFill() {
            QueryTimeseriesRequest req = FactryQueryEngine.aggregateRequest("u1", "mean", "10m").build();

            assertEquals("mean", req.getAggregation().getName());
            assertEquals("none", req.getAggregation().getFillType());
            assertEquals(List.of("status"), req.getGroupByList());
            assertFalse(req.hasTags());
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
        void millisAgreeWithThePeriod() {
            // GoodTimeAverage cuts raw samples into buckets of timeWindowToMillis; Factry makes
            // them from the period string. A 600 ms window must be 600 ms on both sides.
            assertEquals(600, FactryQueryEngine.timeWindowToMillis(TimeWindow.ofMillis(600)));
            assertEquals(1_500, FactryQueryEngine.timeWindowToMillis(TimeWindow.ofMillis(1500)));
            assertEquals(86_400_000, FactryQueryEngine.timeWindowToMillis(TimeWindow.ofDays(1)));
            for (TimeWindow tw : List.of(TimeWindow.ofMillis(600), TimeWindow.ofMillis(1500),
                    TimeWindow.ofSeconds(90), TimeWindow.ofHours(1), TimeWindow.ofDays(1))) {
                assertEquals(FactryQueryEngine.timeWindowToPeriod(tw),
                        FactryQueryEngine.toPeriod(FactryQueryEngine.timeWindowToMillis(tw)));
            }
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
