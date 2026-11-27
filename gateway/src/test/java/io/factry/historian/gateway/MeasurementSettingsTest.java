package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.config.BasicPropertySet;
import com.inductiveautomation.ignition.common.sqltags.history.InterpolationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.tags.config.properties.TagHistoryProps;
import com.inductiveautomation.ignition.common.tags.config.properties.WellKnownTagProps;
import io.factry.historian.proto.CreateMeasurement;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Each tag's Deadband Style and Ignition data type are stored as settings on its measurement:
 * at creation, and kept up to date on existing measurements with MigrateSettings.
 */
class MeasurementSettingsTest {

    private static final String TYPE = MeasurementCache.IGNITION_DATA_TYPE_SETTING;
    private static final String STYLE = MeasurementCache.DEADBAND_STYLE_SETTING;

    private static Struct settings(String... keyValues) {
        Struct.Builder b = Struct.newBuilder();
        for (int i = 0; i < keyValues.length; i += 2) {
            b.putFields(keyValues[i], Value.newBuilder().setStringValue(keyValues[i + 1]).build());
        }
        return b.build();
    }

    private static MetadataPoint metadata(String tag, InterpolationMode style) {
        return metadata(tag, style, null);
    }

    private static MetadataPoint metadata(String tag, InterpolationMode style, DataType type) {
        BasicPropertySet ps = new BasicPropertySet();
        ps.set(TagHistoryProps.HistoricalDeadbandStyle, style);
        if (type != null) {
            ps.set(WellKnownTagProps.DataType, type);
        }
        return MetadataPoint.from(ps, QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:" + tag));
    }

    private static FactryStorageEngine storage(FactryGrpcClient client, MeasurementCache cache) {
        return new FactryStorageEngine(FakeFactryServer.gatewayContext(), "Factry", new FactryHistorianSettings(),
                client, cache, new HistorianMetrics());
    }

    /** A cache over {@code server}, its metadata for "out" stored as Discrete. */
    private static MeasurementCache cacheWithPending(FactryGrpcClient client) {
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(client);
        storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
        return cache;
    }

    // At creation.

    @Test
    void aNewMeasurement_getsTheStyleAndTheType() {
        CreateMeasurement m = MeasurementCache.buildCreateMeasurement("default/out", "number",
                Map.of(MeasurementCache.IGNITION_DATA_TYPE, "Float8", MeasurementCache.DEADBAND_STYLE, "Discrete"));
        assertEquals(settings(TYPE, "Float8", STYLE, "Discrete"), m.getSettings());
    }

    @Test
    void theStoredStyle_isTheConcreteOne() {
        // Another gateway may not know the type, so Auto is stored as what it means for the tag.
        assertEquals("Discrete", MeasurementCache.buildCreateMeasurement("default/out", "number",
                Map.of(MeasurementCache.IGNITION_DATA_TYPE, "Int4", MeasurementCache.DEADBAND_STYLE, "Auto"))
                .getSettings().getFieldsOrThrow(STYLE).getStringValue());
        assertEquals("Analog_Compressed", MeasurementCache.buildCreateMeasurement("default/out", "number",
                Map.of(MeasurementCache.IGNITION_DATA_TYPE, "Float8", MeasurementCache.DEADBAND_STYLE, "Auto"))
                .getSettings().getFieldsOrThrow(STYLE).getStringValue());
        // Auto on a Float8Array: Ignition picks the style by the element type, not the DataSet class.
        assertEquals("Analog_Compressed", MeasurementCache.concreteStyle("Auto", "Float8Array"));
        // Unknown type: Auto stays Auto.
        assertEquals("Auto", MeasurementCache.concreteStyle("Auto", null));
    }

    @Test
    void metadataWithoutAStyle_meansAuto() {
        // With Include Metadata on, Ignition leaves a style at its default out.
        MeasurementCache cache = new MeasurementCache();
        FactryStorageEngine storage = storage(null, cache);
        storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
        BasicPropertySet ps = new BasicPropertySet();
        ps.set(WellKnownTagProps.EngUnit, "m3/h");
        storage.doStoreMetadata(List.of(MetadataPoint.from(ps, QualifiedPath.parseSafe("sys:gw:/prov:default:/tag:out"))));
        assertEquals("Auto", cache.tagMetadata("default/out").get(MeasurementCache.DEADBAND_STYLE));
        assertEquals("m3/h", cache.tagMetadata("default/out").get("engUnit"));
    }

    // On existing measurements.

    @Test
    void aChangedStyle_isStoredOnTheOwnMeasurement_keepingItsOtherSettings() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            server.collectorSettings = settings("siteName", "North");
            String own = server.addMeasurement("default/out", "number", "collector",
                    settings(TYPE, "Float8", STYLE, "Analog_Compressed"));
            String same = server.addMeasurement("default/same", "number", "collector", settings(STYLE, "Discrete"));
            String other = server.addMeasurement("default/other", "number", "another-collector",
                    settings(TYPE, "Float8"));
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                storage(client, cache).doStoreMetadata(List.of(
                        metadata("out", InterpolationMode.Discrete),
                        metadata("same", InterpolationMode.Discrete),
                        metadata("other", InterpolationMode.Discrete)));
                cache.syncTagSettings(client);

                // One call, only for the own measurement whose style changed.
                assertEquals(1, server.migrations.size());
                var changes = server.migrations.get(0).getMeasurementSettingsList();
                assertEquals(1, changes.size());
                assertEquals(own, changes.get(0).getMeasurementUUID());
                assertEquals(settings(TYPE, "Float8", STYLE, "Discrete"), server.measurement(own).getSettings());
                assertEquals(settings(STYLE, "Discrete"), server.measurement(same).getSettings());
                assertEquals(settings(TYPE, "Float8"), server.measurement(other).getSettings());
                // MigrateSettings also replaces the collector's own settings: they are sent along.
                assertEquals(settings("siteName", "North"), server.migrations.get(0).getSettings());
                assertEquals(settings("siteName", "North"), server.collectorSettings);
                assertEquals(settings(TYPE, "Float8", STYLE, "Discrete"), cache.getMeasurementByUUID(own).getSettings());

                // The cache has the new setting, so the same metadata again sends nothing.
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
                cache.syncTagSettings(client);
                assertEquals(1, server.migrations.size());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aMigration_storesTheConcreteStyle_andTheType() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/count", "number", "collector", settings("other", "x"));
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                storage(client, cache).doStoreMetadata(List.of(metadata("count", InterpolationMode.Auto, DataType.Int4)));
                cache.syncTagSettings(client);
                assertEquals(settings("other", "x", STYLE, "Discrete", TYPE, "Int4"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aChangedDataType_isStoredToo() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector",
                    settings(TYPE, "Float8", STYLE, "Analog_Compressed"));
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Auto, DataType.Int4)));
                cache.syncTagSettings(client);
                assertEquals(settings(TYPE, "Int4", STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void autoThenDiscreteThenAuto_endsAsAutoInFactry() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector",
                    settings(TYPE, "Float8", STYLE, "Analog_Compressed"));
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                FactryStorageEngine storage = storage(client, cache);
                storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete, DataType.Float8)));
                cache.syncTagSettings(client);
                assertEquals(settings(TYPE, "Float8", STYLE, "Discrete"), server.measurement(own).getSettings());
                cache.refresh(client);
                storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Auto, DataType.Float8)));
                cache.syncTagSettings(client);
                assertEquals(settings(TYPE, "Float8", STYLE, "Analog_Compressed"), server.measurement(own).getSettings());
                assertEquals(2, server.migrations.size());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aSettingChangedInFactrySinceTheLastRefresh_isKept() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", settings(TYPE, "Float8"));
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                // An operator adds a setting in Factry; the cache has not refreshed since.
                server.editSettings(own, settings(TYPE, "Float8", "operator", "x"));
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
                cache.syncTagSettings(client);
                assertEquals(settings(TYPE, "Float8", "operator", "x", STYLE, "Discrete"),
                        server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void theFreshRead_isOnePagedListingOfTheCollector_perRun() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            server.addMeasurement("default/other", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete),
                        metadata("other", InterpolationMode.Discrete)));
                int before = server.filterRequests.size();
                cache.syncTagSettings(client);
                var reads = server.filterRequests.subList(before, server.filterRequests.size());
                assertEquals(1, reads.size());
                assertEquals(List.of("collector"), reads.get(0).getCollectorUUIDsList());
                assertFalse(reads.get(0).hasKeyword());
                assertTrue(reads.get(0).getPagination().getLimit() > 0);
                assertEquals(1, server.migrations.size());
                assertEquals(2, server.migrations.get(0).getMeasurementSettingsCount());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void manyMeasurements_goInBatches_aFewPerRun() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            List<MetadataPoint> points = new ArrayList<>();
            int count = 2 * MeasurementCache.MIGRATE_BATCH + 1;
            for (int i = 0; i < count; i++) {
                server.addMeasurement("default/t" + i, "number", "collector", Struct.getDefaultInstance());
                points.add(metadata("t" + i, InterpolationMode.Discrete));
            }
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                storage(client, cache).doStoreMetadata(points);
                int reads = server.filterRequests.size();
                cache.syncTagSettings(client);
                // Two calls this run, after one listing of the collector; the rest waits for the next run.
                assertEquals(List.of(MeasurementCache.MIGRATE_BATCH, MeasurementCache.MIGRATE_BATCH),
                        server.migrations.stream().map(r -> r.getMeasurementSettingsCount()).toList());
                assertEquals(reads + 1, server.filterRequests.size());
                cache.syncTagSettings(client);
                assertEquals(3, server.migrations.size());
                assertEquals(1, server.migrations.get(2).getMeasurementSettingsCount());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aRefusal_stopsTheCalls_otherFailuresAreTriedAgain() throws Exception {
        for (Status refusal : List.of(Status.PERMISSION_DENIED, Status.UNIMPLEMENTED)) {
            try (FakeFactryServer server = new FakeFactryServer()) {
                String own = server.addMeasurement("default/out", "number", "collector", settings(TYPE, "Float8"));
                FactryGrpcClient client = server.client();
                try {
                    MeasurementCache cache = new MeasurementCache();
                    cache.refresh(client);
                    FactryStorageEngine storage = storage(client, cache);
                    // Another failure: tried again at the next run, without new metadata.
                    server.migrateFailure = Status.INTERNAL.withDescription("hiccup");
                    var result = storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
                    cache.syncTagSettings(client);
                    cache.syncTagSettings(client);
                    assertTrue(result.error().isEmpty(), result::toString);
                    assertEquals(2, server.migrations.size());
                    // A refusal: no more calls this session.
                    server.migrateFailure = refusal.withDescription("refused on purpose");
                    cache.syncTagSettings(client);
                    storage.doStoreMetadata(List.of(metadata("out", InterpolationMode.Analog_Compressed)));
                    cache.syncTagSettings(client);
                    assertEquals(3, server.migrations.size(), refusal.toString());
                    // A failure changes nothing.
                    assertEquals(settings(TYPE, "Float8"), cache.getMeasurementByUUID(own).getSettings());
                    assertEquals(settings(TYPE, "Float8"), server.measurement(own).getSettings());
                } finally {
                    client.shutdown();
                }
            }
        }
    }

    @Test
    void aFailedMigration_isTriedAgainWithoutNewMetadata() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                server.migrateFailure = Status.UNAVAILABLE;
                cache.syncTagSettings(client);
                server.migrateFailure = null;
                cache.syncTagSettings(client);
                cache.syncTagSettings(client);
                assertEquals(2, server.migrations.size());
                assertEquals(settings(STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void whenTheFreshReadFails_theBatchIsSkipped() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                server.filterFailure = Status.UNAVAILABLE;
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
                // The tag stays pending: the next run sends it.
                server.filterFailure = null;
                cache.syncTagSettings(client);
                assertEquals(settings(STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aMeasurementMissingFromTheListing_staysPending() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                server.hiddenFromListing.add(own);
                cache.syncTagSettings(client);
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
                server.hiddenFromListing.clear();
                cache.syncTagSettings(client);
                assertEquals(settings(STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aMeasurementDeletedInFactry_isNotSent() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                server.change(own, "Deleted");
                cache.syncTagSettings(client);
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aMeasurementPausedInFactry_isSent_andStaysPaused() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                server.change(own, "Paused");
                cache.syncTagSettings(client);
                assertEquals(1, server.migrations.size());
                assertEquals("Paused", server.measurement(own).getStatus());
                assertEquals(settings(STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void aRefreshDuringTheCall_keepsTheSentSettings() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                // The listing of this refresh is read before Factry applies the call.
                server.onMigrate = () -> cache.refresh(client);
                cache.syncTagSettings(client);
                server.onMigrate = null;
                assertEquals(settings(STYLE, "Discrete"), cache.getMeasurementByUUID(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void beforeTheCacheIsLoaded_tagsStayPending() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            String own = server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                // The startup refresh has not worked yet.
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
                cache.refresh(client);
                cache.syncTagSettings(client);
                assertEquals(settings(STYLE, "Discrete"), server.measurement(own).getSettings());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void storingMetadata_asksFactryNothing() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = new MeasurementCache();
                cache.refresh(client);
                int before = server.filterRequests.size();
                storage(client, cache).doStoreMetadata(List.of(metadata("out", InterpolationMode.Discrete)));
                assertEquals(before, server.filterRequests.size());
                assertEquals(0, server.migrations.size());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    void anUncheckedFactryVersion_isNotCalled() throws Exception {
        for (String version : List.of("v8.1.0", "v8.2.2", "v8.4.0", "v9.0.0", "dev")) {
            try (FakeFactryServer server = new FakeFactryServer()) {
                server.version = version;
                server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
                FactryGrpcClient client = server.client();
                try {
                    MeasurementCache cache = cacheWithPending(client);
                    cache.syncTagSettings(client);
                    cache.syncTagSettings(client);
                    assertEquals(0, server.migrations.size(), version);
                } finally {
                    client.shutdown();
                }
            }
        }
        assertTrue(MeasurementCache.checkedVersion("v8.2.3"));
        assertTrue(MeasurementCache.checkedVersion("v8.3.1"));
        assertTrue(MeasurementCache.checkedVersion("8.3.12"));
    }

    @Test
    void theVersion_isReadAgainAfterAReconnect() throws Exception {
        try (FakeFactryServer server = new FakeFactryServer()) {
            server.addMeasurement("default/out", "number", "collector", Struct.getDefaultInstance());
            FactryGrpcClient client = server.client();
            try {
                MeasurementCache cache = cacheWithPending(client);
                int reads = server.filterRequests.size();
                server.getInfoFailure = Status.UNAVAILABLE;
                cache.syncTagSettings(client);
                server.getInfoFailure = null;
                server.version = "v8.4.0";
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
                // The gate comes first: nothing else was asked.
                assertEquals(reads, server.filterRequests.size());
                // Factry changed version underneath: read again only after a failed check or a reconnect.
                server.version = "v8.3.1";
                cache.syncTagSettings(client);
                assertEquals(0, server.migrations.size());
                cache.forgetFactryVersion();
                cache.syncTagSettings(client);
                assertEquals(1, server.migrations.size());
            } finally {
                client.shutdown();
            }
        }
    }
}
