package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.MetadataProperty;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MeasurementCacheTest {

    @Nested
    class BuildCreateMeasurement {

        private Struct configGroup(CreateMeasurement m) {
            assertTrue(m.hasAttributes(), "attributes struct should be set");
            Value config = m.getAttributes().getFieldsMap().get("Config");
            assertNotNull(config, "attributes should contain a 'Config' group");
            assertEquals(Value.KindCase.STRUCT_VALUE, config.getKindCase(), "'Config' should be a nested struct");
            return config.getStructValue();
        }

        @Test
        void mapsEngineeringSpecsIntoConfigGroupWithFactryKeys() {
            Map<String, String> metadata = new LinkedHashMap<>();
            metadata.put("engUnit", "degC");
            metadata.put("engLow", "0.0");
            metadata.put("engHigh", "100.0");

            CreateMeasurement m = MeasurementCache.buildCreateMeasurement("[default]Tank/Temp", "number", metadata);

            Struct config = configGroup(m);
            assertEquals("degC", config.getFieldsOrThrow("UoM").getStringValue());
            assertEquals(Value.KindCase.NUMBER_VALUE, config.getFieldsOrThrow("ValueMin").getKindCase());
            assertEquals(0.0, config.getFieldsOrThrow("ValueMin").getNumberValue());
            assertEquals(Value.KindCase.NUMBER_VALUE, config.getFieldsOrThrow("ValueMax").getKindCase());
            assertEquals(100.0, config.getFieldsOrThrow("ValueMax").getNumberValue());

            // LimitLo/LimitHi have no plain Ignition tag property equivalent.
            assertFalse(config.containsFields("LimitLo"));
            assertFalse(config.containsFields("LimitHi"));

            // The known specs travel via Config only, not the metadata map.
            assertFalse(m.containsMetadata("engUnit"));
            assertFalse(m.containsMetadata("engLow"));
            assertFalse(m.containsMetadata("engHigh"));

            assertEquals("[default]Tank/Temp", m.getName());
            assertEquals("number", m.getDataType());
            assertTrue(m.getAutoOnboard());
        }

        @Test
        void unknownPropertiesStillRoundTripThroughMetadata() {
            Map<String, String> metadata = new LinkedHashMap<>();
            metadata.put("engUnit", "bar");
            metadata.put("documentation", "Boiler pressure");
            metadata.put("tooltip", "PT-101");

            CreateMeasurement m = MeasurementCache.buildCreateMeasurement("[default]Boiler/P", "number", metadata);

            MetadataProperty doc = m.getMetadataOrThrow("documentation");
            assertEquals(MetadataProperty.DataType.STRING, doc.getDataType());
            assertEquals("Boiler pressure", doc.getValue().getStringValue());
            assertEquals("PT-101", m.getMetadataOrThrow("tooltip").getValue().getStringValue());

            assertEquals("bar", configGroup(m).getFieldsOrThrow("UoM").getStringValue());
        }

        @Test
        void descriptionBecomesTheMeasurementDescription() {
            CreateMeasurement m = MeasurementCache.buildCreateMeasurement(
                    "[default]Tank/Level", "number", Map.of("description", "Tank level"));

            assertEquals("Tank level", m.getDescription());
            assertFalse(m.containsMetadata("description"));
            assertFalse(m.hasAttributes(), "no Config keys means no attributes struct");
        }

        @Test
        void nonNumericRangeValueFallsBackToMetadata() {
            CreateMeasurement m = MeasurementCache.buildCreateMeasurement(
                    "[default]Tank/Level", "number", Map.of("engLow", "n/a"));

            assertEquals("n/a", m.getMetadataOrThrow("engLow").getValue().getStringValue());
            assertFalse(m.hasAttributes());
        }

        @Test
        void noMetadataProducesABareRequest() {
            CreateMeasurement m = MeasurementCache.buildCreateMeasurement("[default]Tag", "string", null);

            assertEquals("[default]Tag", m.getName());
            assertEquals("string", m.getDataType());
            assertTrue(m.getAutoOnboard());
            assertFalse(m.hasAttributes());
            assertTrue(m.getMetadataMap().isEmpty());
            assertFalse(m.hasDescription());
        }
    }
}
