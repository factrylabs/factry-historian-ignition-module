package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.MetadataProperty;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
        void unknownPropertiesGoToTheMetadataMap() {
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

    /** Polls for a created measurement without waiting. */
    static final long[] NO_WAIT = {0, 0, 0, 0, 0};

    /** Fake Factry clients made by the current test, shut down after it. */
    private final List<FakeFactry> fakes = new ArrayList<>();

    FakeFactry fakeFactry() {
        FakeFactry factry = new FakeFactry();
        fakes.add(factry);
        return factry;
    }

    @AfterEach
    void shutDownFakes() {
        fakes.forEach(FactryGrpcClient::shutdown);
    }

    @Nested
    class ExistingMeasurementNotInCache {

        @Test
        void getOrCreateUsesTheExistingMeasurementInsteadOfCreating() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            // Created in Factry after the cache was last refreshed.
            factry.add("[default]Tank/Temp", "existing-uuid");

            String uuid = cache.getOrCreateUUID("[default]Tank/Temp", factry, 21.5);

            assertEquals("existing-uuid", uuid);
            assertTrue(factry.createdNames.isEmpty(), "must not ask Factry to create an existing name");
            assertEquals("existing-uuid", cache.getUUID("[default]Tank/Temp"));
        }

        @Test
        void batchCreatesOnlyTheNamesFactryDoesNotHave() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add("[default]Tank/Temp", "existing-uuid");

            Map<String, Object> values = new LinkedHashMap<>();
            values.put("[default]Tank/Temp", 21.5);
            values.put("[default]Tank/Level", 3.2);
            Map<String, String> uuids = cache.batchGetOrCreateUUIDs(values, factry);

            assertEquals("existing-uuid", uuids.get("[default]Tank/Temp"));
            assertEquals("new-[default]Tank/Level", uuids.get("[default]Tank/Level"));
            assertEquals(List.of("[default]Tank/Level"), factry.createdNames);
        }

        @Test
        void aNameThatOnlyContainsTheTagPathDoesNotCount() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            // The keyword filter matches substrings; only an exact name is the same measurement.
            factry.add("[default]Tank/Temp2", "other-uuid");

            String uuid = cache.getOrCreateUUID("[default]Tank/Temp", factry, 21.5);

            assertEquals("new-[default]Tank/Temp", uuid);
            assertEquals(List.of("[default]Tank/Temp"), factry.createdNames);
        }

        @Test
        void theLookupReadsFurtherPagesForTheExactName() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            for (int i = 0; i < 60; i++) {
                factry.add("[default]Tank/Temp/" + i, "sub-" + i);
            }
            factry.add("[default]Tank/Temp", "existing-uuid");

            assertEquals("existing-uuid", cache.getOrCreateUUID("[default]Tank/Temp", factry, 21.5));
            assertTrue(factry.createdNames.isEmpty());
            assertEquals(2, factry.lookups);
        }

        @Test
        void theLookupGivesUpAfterItsPageBoundAndCreates() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            for (int i = 0; i < 300; i++) {
                factry.add("[default]Tank/Temp/" + i, "sub-" + i);
            }

            Map<String, String> uuids = cache.batchGetOrCreateUUIDs(Map.of("[default]Tank/Temp", 21.5), factry);

            assertEquals("new-[default]Tank/Temp", uuids.get("[default]Tank/Temp"));
            assertEquals(List.of("[default]Tank/Temp"), factry.createdNames);
        }

        @Test
        void anActiveMeasurementWinsOverADiscoveredDuplicate() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add("[default]Tank/Temp", "discovered-uuid", "Discovered");
            factry.add("[default]Tank/Temp", "active-uuid");

            assertEquals("active-uuid", cache.getOrCreateUUID("[default]Tank/Temp", factry, 21.5));
            assertTrue(factry.createdNames.isEmpty());
        }

        @Test
        void aPathAnotherCallIsCreatingIsPolledNotCreatedAgain() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add("[default]Tank/Temp", "existing-uuid");
            Map<String, Object> second = new LinkedHashMap<>();
            second.put("[default]Tank/Level", 3.2);
            second.put("[default]Tank/Temp", 21.5);
            List<Map<String, String>> secondResult = new ArrayList<>();
            // While the first call creates Tank/Level, a second call asks for it and Tank/Temp.
            factry.onCreate = () -> {
                factry.onCreate = () -> { };
                secondResult.add(cache.batchGetOrCreateUUIDs(second, factry));
            };

            Map<String, String> first = cache.batchGetOrCreateUUIDs(Map.of("[default]Tank/Level", 3.2), factry);

            assertEquals("new-[default]Tank/Level", first.get("[default]Tank/Level"));
            assertEquals(Map.of("[default]Tank/Level", "new-[default]Tank/Level",
                    "[default]Tank/Temp", "existing-uuid"), secondResult.get(0));
            assertEquals(List.of("[default]Tank/Level"), factry.createdNames);
        }
    }

    @Nested
    class NameThatIsNotActive {

        private static final String TEMP = "[default]Tank/Temp";

        @Test
        void foundByTheLookupItIsRefusedNotCreatedAndCached() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "discovered-uuid", "Discovered");

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(MeasurementCache.WriteRefusal.NOT_ACTIVE, cache.writeRefusal(TEMP, FakeFactry.OWN));
            int lookups = factry.lookups;
            assertEquals("", cache.getOrCreateUUID(TEMP, factry, 21.5));
            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());

            assertTrue(factry.createdNames.isEmpty(), "creating would add another Discovered duplicate");
            assertEquals(lookups, factry.lookups, "cached: no lookup per flush");
        }

        @Test
        void fromTheRefreshItCostsNoLookup() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "discovered-uuid", "Discovered");
            cache.refresh(factry);

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(0, factry.lookups);
            assertEquals(MeasurementCache.WriteRefusal.NOT_ACTIVE, cache.writeRefusal(TEMP, FakeFactry.OWN));
        }

        @Test
        void isWrittenToFromTheRefreshAfterItIsActive() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "discovered-uuid", "Discovered");
            cache.refresh(factry);
            factry.setStatus("discovered-uuid", "Active");
            cache.refresh(factry);

            assertNull(cache.writeRefusal(TEMP, FakeFactry.OWN));
            assertEquals(Map.of(TEMP, "discovered-uuid"), cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry));
        }

        @Test
        void browseAndQueryDoNotSeeIt() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "discovered-uuid", "Discovered");
            cache.refresh(factry);

            assertNull(cache.getMeasurementByName(TEMP));
            assertTrue(cache.getAllMeasurements().isEmpty());
            assertEquals(0, cache.size());
        }
    }

    @Nested
    class LookupFailure {

        @Test
        void batchCreatesWhenFactryRefusesTheLookup() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            // The lookup before create is refused; the polling after the create succeeds.
            factry.lookupFailures = 1;

            Map<String, String> uuids = cache.batchGetOrCreateUUIDs(
                    Map.of("[default]Tank/Level", 3.2), factry);

            assertEquals("new-[default]Tank/Level", uuids.get("[default]Tank/Level"));
            assertEquals(List.of("[default]Tank/Level"), factry.createdNames);
        }

        @Test
        void batchStopsAtTheFirstConnectionErrorAndCreatesNothing() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add("[default]Tank/Temp", "cached-uuid");
            cache.refresh(factry);
            factry.lookupFailures = 10;
            factry.lookupFailure = Status.DEADLINE_EXCEEDED;
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("[default]Tank/Temp", 21.5);
            for (int i = 0; i < 5; i++) {
                values.put("[default]New/" + i, i);
            }

            Map<String, String> uuids = cache.batchGetOrCreateUUIDs(values, factry);

            assertEquals(Map.of("[default]Tank/Temp", "cached-uuid"), uuids, "cache hits still resolve");
            assertEquals(1, factry.lookups, "one deadline per flush, not one per new tag");
            assertTrue(factry.createdNames.isEmpty());
        }

        @Test
        void singlePathCreatesWhenFactryRefusesTheLookup() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.lookupFailures = 1;

            assertEquals("new-[default]Tank/Level", cache.getOrCreateUUID("[default]Tank/Level", factry, 3.2));
        }

        @Test
        void singlePathCreatesNothingWhenFactryIsUnavailable() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.lookupFailures = 1;
            factry.lookupFailure = Status.UNAVAILABLE;

            assertEquals("", cache.getOrCreateUUID("[default]Tank/Level", factry, 3.2));
            assertTrue(factry.createdNames.isEmpty());
            assertEquals(1, factry.lookups);
        }
    }

    @Nested
    class MeasurementOfAnotherCollector {

        private static final String TEMP = "[default]Tank/Temp";

        @Test
        void inTheSameDatabaseItIsCreatedOnceThenRefused() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            cache.refresh(factry);
            assertNull(cache.writeRefusal(TEMP, FakeFactry.OWN), "not settled before the module tried");

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(List.of(TEMP), factry.createdNames, "one try, accepting a Discovered duplicate");
            assertEquals(MeasurementCache.WriteRefusal.OTHER_COLLECTOR, cache.writeRefusal(TEMP, FakeFactry.OWN));

            int lookups = factry.lookups;
            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals("", cache.getOrCreateUUID(TEMP, factry, 21.5));
            assertEquals(List.of(TEMP), factry.createdNames, "never created again");
            assertEquals(lookups, factry.lookups, "settled refusals cost no gRPC call");
        }

        @Test
        void aRestartDoesNotCreateItAgain() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            cache.refresh(factry);
            cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry);
            int measurements = factry.measurements.size();

            MeasurementCache afterRestart = new MeasurementCache(NO_WAIT);
            afterRestart.refresh(factry);

            assertTrue(afterRestart.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(List.of(TEMP), factry.createdNames, "the earlier duplicate shows it was tried");
            assertEquals(measurements, factry.measurements.size());
            assertEquals(MeasurementCache.WriteRefusal.OTHER_COLLECTOR,
                    afterRestart.writeRefusal(TEMP, FakeFactry.OWN));
        }

        @Test
        void aRestartWithoutARefreshDoesNotCreateItAgainEither() {
            FakeFactry factry = fakeFactry();
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            factry.add(TEMP, "duplicate-uuid", FakeFactry.OWN, "Discovered");
            MeasurementCache afterRestart = new MeasurementCache(NO_WAIT);

            assertEquals("", afterRestart.getOrCreateUUID(TEMP, factry, 21.5));
            assertTrue(factry.createdNames.isEmpty());
        }

        @Test
        void inAnotherDatabaseOurOwnIsCreatedAndWrittenTo() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            factry.existingInOtherDatabase = true;
            cache.refresh(factry);

            assertEquals(Map.of(TEMP, "own-" + TEMP), cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry));
            assertNull(cache.writeRefusal(TEMP, FakeFactry.OWN));
            // Query by name prefers this collector's measurement too.
            assertEquals("own-" + TEMP, cache.getUUID(TEMP));
        }

        @Test
        void foundByTheLookupItIsAlsoCreatedOnceThenRefused() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            // Created by the other collector after the cache was last refreshed.
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");

            assertEquals("", cache.getOrCreateUUID(TEMP, factry, 21.5));
            assertEquals(List.of(TEMP), factry.createdNames);
            assertEquals(MeasurementCache.WriteRefusal.OTHER_COLLECTOR, cache.writeRefusal(TEMP, FakeFactry.OWN));
        }

        @Test
        void anotherCollectorsPausedOrDiscoveredNameIsAlsoTriedOnce() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Paused");
            cache.refresh(factry);

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());

            assertEquals(List.of(TEMP), factry.createdNames);
            assertEquals(MeasurementCache.WriteRefusal.NOT_ACTIVE, cache.writeRefusal(TEMP, FakeFactry.OWN),
                    "what is left is this collector's Discovered duplicate");
        }

        @Test
        void aConnectionErrorLeavesItUnsettled() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            cache.refresh(factry);
            factry.lookupFailures = 1;
            factry.lookupFailure = Status.UNAVAILABLE;

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertTrue(factry.createdNames.isEmpty());
            assertNull(cache.writeRefusal(TEMP, FakeFactry.OWN), "its points are retried, not discarded");
        }

        @Test
        void aCreateFactryRefusesCountsAsTriedUntilTheSettingsChange() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            cache.refresh(factry);
            factry.createFailure = Status.ALREADY_EXISTS;

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(MeasurementCache.WriteRefusal.OTHER_COLLECTOR, cache.writeRefusal(TEMP, FakeFactry.OWN));

            cache.clearWriteRefusals();

            assertNull(cache.writeRefusal(TEMP, FakeFactry.OWN));
        }

        @Test
        void pollingStopsWhenAnotherCollectorsMeasurementAppears() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.createdCollector = FakeFactry.OTHER;

            assertTrue(cache.batchGetOrCreateUUIDs(Map.of(TEMP, 21.5), factry).isEmpty());
            assertEquals(2, factry.lookups, "the lookup before create and one poll");
        }

        @Test
        void browseAndQueryStillSeeIt() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            cache.refresh(factry);

            assertEquals("other-uuid", cache.getMeasurementByName(TEMP).getUuid());
            assertEquals(1, cache.getAllMeasurements().size());
        }

        @Test
        void ownMeasurementWinsANameClashInTheRefreshAndTheLookup() {
            FakeFactry factry = fakeFactry();
            MeasurementCache cache = new MeasurementCache(NO_WAIT);
            factry.add(TEMP, "other-uuid", FakeFactry.OTHER, "Active");
            factry.add(TEMP, "own-uuid");
            factry.add("[default]Tank/Level", "other-level", FakeFactry.OTHER, "Active");
            cache.refresh(factry);
            factry.add("[default]Tank/Level", "own-level");

            assertEquals("own-uuid", cache.getUUID(TEMP));
            assertEquals("own-level", cache.getOrCreateUUID("[default]Tank/Level", factry, 3.2));
            assertEquals(4, cache.getAllMeasurements().size(), "browse keeps every collector's");
            assertTrue(factry.createdNames.isEmpty());
        }

        @Test
        void ownershipIsJudgedByCollectorUUIDAndUnknownMeansOwn() {
            Measurement m = Measurement.newBuilder().setName("x").setUuid("u")
                    .setCollectorUUID("ABC-123").setStatus("Active").build();

            assertNull(MeasurementCache.writeRefusal(m, "abc-123"));
            assertEquals(MeasurementCache.WriteRefusal.OTHER_COLLECTOR, MeasurementCache.writeRefusal(m, "def-456"));
            assertNull(MeasurementCache.writeRefusal(m.toBuilder().clearCollectorUUID().build(), "abc-123"),
                    "a measurement without a collector is written to as before");
            assertNull(MeasurementCache.writeRefusal(m, ""), "so is any measurement without our own UUID");
            assertNull(MeasurementCache.writeRefusal((Measurement) null, "abc-123"));
        }
    }
}
