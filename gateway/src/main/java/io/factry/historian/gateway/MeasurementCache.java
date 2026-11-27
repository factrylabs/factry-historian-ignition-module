package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.inductiveautomation.ignition.common.sqltags.history.InterpolationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataTypeClass;
import io.factry.historian.proto.Asset;
import io.factry.historian.proto.AssetProperties;
import io.factry.historian.proto.AssetProperty;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.CreateMeasurementsRequest;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.GetAssetPropertiesRequest;
import io.factry.historian.proto.GetAssetsRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.MetadataProperty;
import io.factry.historian.proto.MigrateMeasurementSettings;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Pagination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class MeasurementCache {
    private static final Logger logger = LoggerFactory.getLogger(MeasurementCache.class);

    // These lookup maps are rebuilt on every refresh and swapped in by reference (not cleared and
    // refilled in place), so a concurrent browse or query always sees a fully-populated map — never
    // an empty or half-filled one. Each field is volatile so the swapped-in reference is visible to
    // other threads immediately; the maps themselves stay ConcurrentHashMap so the occasional in-place
    // mutation (evictByUUID, targeted single-measurement inserts) remains thread-safe.
    private volatile ConcurrentHashMap<String, String> tagPathToUUID = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Measurement> uuidToMeasurement = new ConcurrentHashMap<>();
    private final Set<String> pendingCreations = ConcurrentHashMap.newKeySet();

    private volatile ConcurrentHashMap<String, String> assetNameToUUID = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Asset> uuidToAsset = new ConcurrentHashMap<>();
    /** Asset UUID → list of properties belonging to that asset. */
    private volatile ConcurrentHashMap<String, List<AssetProperty>> assetProperties = new ConcurrentHashMap<>();

    /** Measurement UUID → collector name, for grouping in the browse tree. */
    private volatile ConcurrentHashMap<String, String> measurementToCollectorName = new ConcurrentHashMap<>();

    /**
     * Metadata properties cached from doStoreMetadata: applied when creating measurements, and the
     * source of the settings kept up to date on them ({@link #syncTagSettings}). Creating a
     * measurement does not remove them: Ignition sends them at start and on a change only.
     */
    private final ConcurrentHashMap<String, Map<String, String>> tagMetadata = new ConcurrentHashMap<>();

    /** Set once a refresh has listed Factry's measurements. */
    private volatile boolean loaded;

    /** The current time; tests replace it. */
    LongSupplier now = System::currentTimeMillis;

    private final int pageSize = ModuleProperties.getMeasurementCachePageSize();

    public void refresh(FactryGrpcClient grpcClient) {
        long refreshStarted = now.getAsLong();
        try {
            // Fetch all measurements using pagination
            Map<String, String> freshPaths = new HashMap<>();
            Map<String, Measurement> freshMeasurements = new HashMap<>();
            long offset = 0;
            long serverTotal = Long.MAX_VALUE;

            while (offset < serverTotal) {
                GetMeasurementsByFilterRequest request = GetMeasurementsByFilterRequest.newBuilder()
                        .setPagination(Pagination.newBuilder()
                                .setLimit(pageSize)
                                .setOffset(offset)
                                .build())
                        .build();
                Measurements response = grpcClient.getMeasurementsByFilter(request);

                if (response.getTotal() > 0) {
                    serverTotal = response.getTotal();
                }

                int pageCount = response.getMeasurementsCount();
                if (pageCount == 0) {
                    break;
                }

                for (Measurement m : response.getMeasurementsList()) {
                    if ("active".equalsIgnoreCase(m.getStatus())) {
                        freshPaths.put(m.getName(), m.getUuid());
                        freshMeasurements.put(m.getUuid(), m);
                    } else {
                        logger.debug("Skipping measurement '{}' with status '{}'", m.getName(), m.getStatus());
                    }
                }
                offset += pageCount;
            }

            // Swap in freshly-built maps by reference so deleted measurements don't linger and
            // concurrent readers never observe a half-filled map.
            tagPathToUUID = new ConcurrentHashMap<>(freshPaths);
            uuidToMeasurement = new ConcurrentHashMap<>(freshMeasurements);
            // The listing shows MigrateSettings; a call made while it was read may not be in it
            // yet, so its settings go over the listed copy.
            settingsSent.entrySet().removeIf(e -> e.getValue().sentAt() < refreshStarted);
            settingsSent.forEach(this::putSettings);
            loaded = true;
            logger.info("Measurement cache refreshed, {} active of {} total from Factry",
                    freshPaths.size(), serverTotal == Long.MAX_VALUE ? offset : serverTotal);

            // Build measurement → collector name mapping
            try {
                var collectors = grpcClient.getCollectors();
                Map<String, String> collectorUUIDToName = new HashMap<>();
                for (Collector c : collectors.getCollectorsList()) {
                    String collectorName = !c.getName().isEmpty() ? c.getName() : c.getUuid();
                    collectorUUIDToName.put(c.getUuid(), collectorName);
                }

                Map<String, String> freshCollectorMap = new HashMap<>();
                for (Measurement m : freshMeasurements.values()) {
                    String collectorUUID = m.getCollectorUUID();
                    if (!collectorUUID.isEmpty()) {
                        String collectorName = collectorUUIDToName.getOrDefault(collectorUUID, collectorUUID);
                        freshCollectorMap.put(m.getUuid(), collectorName);
                    }
                }

                measurementToCollectorName = new ConcurrentHashMap<>(freshCollectorMap);
                logger.debug("Collector mapping refreshed: {} measurements mapped to collectors", freshCollectorMap.size());
            } catch (Exception ce) {
                logger.error("Failed to refresh collector mapping", ce);
            }

            // Fetch assets and their properties
            try {
                Assets assetsResponse = grpcClient.getAssets();
                Map<String, String> freshAssetNames = new HashMap<>();
                Map<String, Asset> freshAssets = new HashMap<>();
                List<String> assetUUIDs = new ArrayList<>();
                for (Asset a : assetsResponse.getAssetsList()) {
                    freshAssetNames.put(a.getName(), a.getUuid());
                    freshAssets.put(a.getUuid(), a);
                    assetUUIDs.add(a.getUuid());
                }
                assetNameToUUID = new ConcurrentHashMap<>(freshAssetNames);
                uuidToAsset = new ConcurrentHashMap<>(freshAssets);

                // Fetch properties for all assets
                Map<String, List<AssetProperty>> freshProps = new HashMap<>();
                if (!assetUUIDs.isEmpty()) {
                    try {
                        GetAssetPropertiesRequest propReq = GetAssetPropertiesRequest.newBuilder()
                                .addAllAssetUUIDs(assetUUIDs)
                                .setRecursive(true)
                                .build();
                        AssetProperties propsResponse = grpcClient.getAssetProperties(propReq);
                        for (AssetProperty prop : propsResponse.getAssetPropertiesList()) {
                            freshProps.computeIfAbsent(prop.getAssetUUID(), k -> new ArrayList<>()).add(prop);
                        }
                    } catch (Exception pe) {
                        logger.error("Failed to refresh asset properties cache", pe);
                    }
                }
                assetProperties = new ConcurrentHashMap<>(freshProps);

                logger.debug("Asset cache refreshed, {} assets, {} properties",
                        freshAssetNames.size(), freshProps.values().stream().mapToInt(List::size).sum());
            } catch (Exception ae) {
                logger.error("Failed to refresh asset cache", ae);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh measurement cache", e);
        }
    }

    /**
     * Get or create a measurement UUID, with an explicit Factry data type (e.g. "string" for arrays).
     */
    public String getOrCreateUUID(String tagPath, FactryGrpcClient grpcClient, String explicitDataType) {
        return getOrCreateUUID(tagPath, grpcClient, (Object) null, explicitDataType);
    }

    public String getOrCreateUUID(String tagPath, FactryGrpcClient grpcClient, Object value) {
        return getOrCreateUUID(tagPath, grpcClient, value, null);
    }

    private String getOrCreateUUID(String tagPath, FactryGrpcClient grpcClient, Object value, String explicitDataType) {
        // Fast path: already cached
        String uuid = tagPathToUUID.get(tagPath);
        if (uuid != null) {
            logger.debug("Cache hit for '{}': uuid={}", tagPath, uuid);
            return uuid;
        }
        logger.debug("Cache miss for '{}', cache size={}, keys={}", tagPath, tagPathToUUID.size(), tagPathToUUID.keySet());

        // Determine data type from value — refuse to create without it
        String dataType = explicitDataType != null ? explicitDataType : toFactryDataType(value);
        if (dataType == null) {
            logger.debug("Skipping measurement creation for '{}': value is null or unknown type", tagPath);
            return "";
        }

        // Prevent concurrent creation for the same tag path
        if (!pendingCreations.add(tagPath)) {
            logger.debug("Measurement creation already in progress for '{}'", tagPath);
            return "";
        }

        try {
            // Double-check cache after acquiring the "lock"
            uuid = tagPathToUUID.get(tagPath);
            if (uuid != null) {
                return uuid;
            }

            logger.debug("Creating measurement for '{}' with dataType={}", tagPath, dataType);

            CreateMeasurement createMeasurement = buildCreateMeasurement(
                    tagPath, dataType, tagMetadata.get(tagPath));

            CreateMeasurementsRequest request = CreateMeasurementsRequest.newBuilder()
                    .addMeasurements(createMeasurement)
                    .build();

            grpcClient.createMeasurements(request);

            // Poll until the measurement becomes visible in Factry. This runs on the
            // store-and-forward thread, so keep it cheap: use a targeted single-measurement
            // lookup (keyword filter) instead of a full cache reload, and a tight fixed
            // backoff. This scales with the number of new tags, not the total measurement
            // count. Total worst-case wait is ~3s rather than the old 7.5s.
            long[] backoffMs = {100L, 200L, 400L, 800L, 1500L};
            for (int attempt = 0; attempt < backoffMs.length; attempt++) {
                try {
                    Thread.sleep(backoffMs[attempt]);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }

                uuid = fetchSingleMeasurementUUID(tagPath, grpcClient);
                if (uuid != null) {
                    logger.debug("Measurement UUID resolved for '{}' on attempt {}: {}", tagPath, attempt + 1, uuid);
                    return uuid;
                }
                logger.debug("Measurement '{}' not visible yet, attempt {}/{}", tagPath, attempt + 1, backoffMs.length);
            }

            // Fallback: one full refresh in case the keyword filter never surfaced it
            // (e.g. unusual characters in the path defeat the server-side keyword match).
            refresh(grpcClient);
            uuid = tagPathToUUID.get(tagPath);
            if (uuid != null) {
                logger.debug("Measurement UUID resolved for '{}' via fallback refresh: {}", tagPath, uuid);
                return uuid;
            }

            logger.warn("Measurement UUID not found after create + retries for '{}'", tagPath);
            return "";
        } catch (Exception e) {
            logger.error("Failed to create measurement for tag path: " + tagPath, e);
            return "";
        } finally {
            pendingCreations.remove(tagPath);
        }
    }

    /**
     * Resolve UUIDs for a batch of tag paths in a single gRPC round-trip.
     *
     * All paths already in the cache are returned immediately. For unknown paths:
     * the ones not being created by another thread are batched into a single
     * {@code CreateMeasurementsRequest}; then all missing paths (newly created plus
     * those delegated to another thread) are polled together using targeted keyword
     * lookups. This keeps the store-and-forward thread's cost proportional to the
     * number of distinct new tags in the flush, not the number of points.
     *
     * @param tagPathToValue map from tag path to a representative value (used only for
     *                       data-type inference — the actual written values come from
     *                       the caller)
     * @return map from tag path to UUID; absent entries mean creation failed (caller
     *         should mark those points for S&amp;F retry)
     */
    public Map<String, String> batchGetOrCreateUUIDs(
            Map<String, Object> tagPathToValue, FactryGrpcClient grpcClient) {

        Map<String, String> result = new HashMap<>();

        // Split into cache hits and paths that need a UUID.
        Map<String, String> pathToDataType = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : tagPathToValue.entrySet()) {
            String path = entry.getKey();
            String uuid = tagPathToUUID.get(path);
            if (uuid != null) {
                result.put(path, uuid);
            } else {
                String dataType = toFactryDataType(entry.getValue());
                if (dataType != null) {
                    pathToDataType.put(path, dataType);
                }
                // Unsupported value type → no entry in result (caller skips the point).
            }
        }

        if (pathToDataType.isEmpty()) {
            return result;
        }

        // Separate paths this call will create from ones another thread is already
        // handling (pendingCreations guards against duplicate-creation races).
        List<String> toCreate = new ArrayList<>();
        List<String> alreadyPending = new ArrayList<>();
        for (String path : pathToDataType.keySet()) {
            if (pendingCreations.add(path)) {
                // Double-check cache after acquiring the creation slot.
                String uuid = tagPathToUUID.get(path);
                if (uuid != null) {
                    result.put(path, uuid);
                    pendingCreations.remove(path);
                } else {
                    toCreate.add(path);
                }
            } else {
                alreadyPending.add(path);
            }
        }

        try {
            // One CreateMeasurementsRequest for all new paths.
            if (!toCreate.isEmpty()) {
                CreateMeasurementsRequest.Builder req = CreateMeasurementsRequest.newBuilder();
                for (String path : toCreate) {
                    req.addMeasurements(buildCreateMeasurement(
                            path, pathToDataType.get(path), tagMetadata.get(path)));
                }
                grpcClient.createMeasurements(req.build());
                logger.info("Batch-created {} measurements in one gRPC call", toCreate.size());
            }

            // Poll for all missing paths together (newly created + delegated).
            Set<String> stillMissing = new HashSet<>(toCreate);
            stillMissing.addAll(alreadyPending);

            long[] backoffMs = {100L, 200L, 400L, 800L, 1500L};
            for (int attempt = 0; attempt < backoffMs.length && !stillMissing.isEmpty(); attempt++) {
                try {
                    Thread.sleep(backoffMs[attempt]);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                stillMissing.removeIf(path -> {
                    String uuid = fetchSingleMeasurementUUID(path, grpcClient);
                    if (uuid != null) {
                        result.put(path, uuid);
                        return true;
                    }
                    return false;
                });
            }

            // Fallback: full refresh for anything still not visible.
            if (!stillMissing.isEmpty()) {
                logger.warn("Batch: {} paths not visible after retries, triggering full refresh",
                        stillMissing.size());
                refresh(grpcClient);
                for (String path : stillMissing) {
                    String uuid = tagPathToUUID.get(path);
                    if (uuid != null) {
                        result.put(path, uuid);
                    } else {
                        logger.warn("Measurement not found after batch create + full refresh: '{}'", path);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Batch measurement creation failed", e);
        } finally {
            toCreate.forEach(pendingCreations::remove);
        }

        return result;
    }

    /**
     * Targeted lookup of a single measurement by its exact name, used while polling for a
     * just-created measurement to become visible. Uses the server-side keyword filter with a
     * small page instead of reloading the entire cache (measurements, collectors, assets,
     * asset properties), so a burst of new tags doesn't trigger repeated full reloads on the
     * store-and-forward thread. On a hit the entry is inserted into the live maps so later
     * lookups hit the fast path. Returns null if the measurement is not yet visible.
     */
    private String fetchSingleMeasurementUUID(String tagPath, FactryGrpcClient grpcClient) {
        try {
            GetMeasurementsByFilterRequest request = GetMeasurementsByFilterRequest.newBuilder()
                    .setKeyword(tagPath)
                    .setPagination(Pagination.newBuilder().setLimit(50).setOffset(0).build())
                    .build();
            Measurements response = grpcClient.getMeasurementsByFilter(request);
            for (Measurement m : response.getMeasurementsList()) {
                // Keyword search is fuzzy/substring, so match the exact name ourselves.
                if (tagPath.equals(m.getName()) && "active".equalsIgnoreCase(m.getStatus())) {
                    tagPathToUUID.put(m.getName(), m.getUuid());
                    uuidToMeasurement.put(m.getUuid(), m);
                    return m.getUuid();
                }
            }
        } catch (Exception e) {
            logger.debug("Targeted lookup for '{}' failed: {}", tagPath, e.getMessage());
        }
        return null;
    }

    /**
     * Remove a measurement from the cache by UUID so the next
     * {@link #getOrCreateUUID} call will re-create it in Factry.
     */
    public void evictByUUID(String uuid) {
        uuidToMeasurement.remove(uuid);
        tagPathToUUID.entrySet().removeIf(e -> e.getValue().equals(uuid));
        logger.info("Evicted measurement UUID '{}' from cache", uuid);
    }

    public int size() {
        return tagPathToUUID.size();
    }

    public String getUUID(String tagPath) {
        return tagPathToUUID.get(tagPath);
    }

    public Measurement getMeasurementByUUID(String uuid) {
        return uuidToMeasurement.get(uuid);
    }

    public Measurement getMeasurementByName(String name) {
        String uuid = tagPathToUUID.get(name);
        return uuid != null ? uuidToMeasurement.get(uuid) : null;
    }

    public Collection<Measurement> getAllMeasurements() {
        return uuidToMeasurement.values();
    }

    // --- Asset accessors ---

    public String getAssetUUID(String name) {
        return assetNameToUUID.get(name);
    }

    public Asset getAssetByName(String name) {
        String uuid = assetNameToUUID.get(name);
        return uuid != null ? uuidToAsset.get(uuid) : null;
    }

    public Collection<Asset> getAllAssets() {
        return uuidToAsset.values();
    }

    public String getCollectorName(String measurementUUID) {
        return measurementToCollectorName.get(measurementUUID);
    }

    public List<AssetProperty> getPropertiesForAsset(String assetUUID) {
        return assetProperties.getOrDefault(assetUUID, Collections.emptyList());
    }

    /**
     * Cache metadata properties for a tag path. These will be applied as initial
     * values when the measurement is created in Factry via {@link #getOrCreateUUID}.
     */
    public void storeMetadata(String tagPath, Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        // Copy on write, so a create or a settings sync reading the map never sees it change.
        tagMetadata.merge(tagPath, Collections.unmodifiableMap(new HashMap<>(properties)),
                (existing, incoming) -> {
                    Map<String, String> merged = new HashMap<>(existing);
                    merged.putAll(incoming);
                    return Collections.unmodifiableMap(merged);
                });
        logger.debug("Cached metadata for '{}': {}", tagPath, properties);
    }

    /** Ignition tag property that holds the tag's data type (e.g. "Float8"). */
    static final String IGNITION_DATA_TYPE = "dataType";

    /**
     * Measurement setting that keeps the tag's Ignition data type. Settings are used because the
     * gRPC read calls return them, while they leave the metadata map empty (Factry v8.2.3).
     */
    static final String IGNITION_DATA_TYPE_SETTING = "ignitionDataType";

    /**
     * Ignition tag property that holds the tag's History Deadband Style (an InterpolationMode
     * name). Ignition sends it as metadata every time a tag's history is configured, at every
     * start and on every change, even with Include Metadata off (AtomicHistoryActor).
     */
    static final String DEADBAND_STYLE = "historicalDeadbandStyle";

    /**
     * Measurement setting that keeps the tag's Deadband Style, so that another gateway, and this
     * one before Ignition sends the metadata, can draw the tag as Ignition's own historians do.
     */
    static final String DEADBAND_STYLE_SETTING = "ignitionDeadbandStyle";

    /** The metadata cached for a tag, or null. */
    Map<String, String> tagMetadata(String tagPath) {
        return tagMetadata.get(tagPath);
    }

    /** A string setting of a measurement, or null when there is none. */
    static String stringSetting(Measurement m, String name) {
        Value v = m.getSettings().getFieldsMap().get(name);
        return v != null && v.getKindCase() == Value.KindCase.STRING_VALUE ? v.getStringValue() : null;
    }

    /**
     * The Deadband Style to store for a tag: the concrete mode, as the SQL historian stores it
     * (InterpolationMode.getConcreteMode), so that another gateway, which may not know the tag's
     * data type, still holds an integer tag with Auto. Auto stays Auto when the type is unknown.
     */
    static String concreteStyle(String style, String ignitionDataType) {
        if (style == null) {
            return null;
        }
        try {
            InterpolationMode mode = InterpolationMode.valueOf(style);
            DataTypeClass typeClass = null;
            if (ignitionDataType != null) {
                // The element type for an array, as Ignition picks the style for one
                // (AtomicHistoryActor.configure): Float8Array's own class is DataSet.
                typeClass = DataType.valueOf(ignitionDataType).getComponentDataType().getTypeClass();
            }
            if (mode == InterpolationMode.Auto && typeClass == null) {
                return style;
            }
            return mode.getConcreteMode(typeClass).name();
        } catch (IllegalArgumentException e) {
            return style;
        }
    }

    /**
     * The measurement settings the module keeps from a tag's metadata, setting name to value.
     * Kept up to date on the measurement by {@link #syncTagSettings}; a new one is one line here.
     */
    static Map<String, String> derivedSettings(Map<String, String> metadata) {
        Map<String, String> out = new LinkedHashMap<>();
        if (metadata == null) {
            return out;
        }
        String type = metadata.get(IGNITION_DATA_TYPE);
        out.put(IGNITION_DATA_TYPE_SETTING, type);
        out.put(DEADBAND_STYLE_SETTING, concreteStyle(metadata.get(DEADBAND_STYLE), type));
        out.values().removeIf(Objects::isNull);
        return out;
    }

    /** {@code settings} with the derived ones put over it. */
    private static Struct withDerived(Struct settings, Map<String, String> derived) {
        Struct.Builder b = settings.toBuilder();
        derived.forEach((name, value) -> b.putFields(name, Value.newBuilder().setStringValue(value).build()));
        return b.build();
    }

    /** Most measurements in one MigrateSettings call. */
    static final int MIGRATE_BATCH = 500;

    /** Most MigrateSettings calls per {@link #syncTagSettings}. */
    static final int MIGRATE_BATCHES_PER_RUN = 2;

    /** Tags whose derived settings may differ from their own measurement's; see {@link #syncTagSettings}. */
    private final Set<String> settingsPending = ConcurrentHashMap.newKeySet();

    /** Runs one {@link #syncTagSettings} at a time. */
    private final Object settingsLock = new Object();

    /**
     * The gRPC status codes of failed MigrateSettings calls already logged: once per code per
     * profile, since a proxy in front of Factry may refuse the call on purpose.
     */
    private final Set<io.grpc.Status.Code> migrateFailuresLogged = ConcurrentHashMap.newKeySet();

    /**
     * Set once Factry refuses MigrateSettings for good (PermissionDenied: a proxy that refuses it
     * on purpose; Unimplemented: a Factry without it). No more calls until the profile restarts.
     */
    private volatile String migrateRefused;

    /** Logged once: a measurement the cache holds was not in the listing before MigrateSettings. */
    private volatile boolean missingFromListingLogged;

    /** Logged once: the read of this collector's measurements before MigrateSettings failed. */
    private volatile boolean freshReadFailureLogged;

    /**
     * Null until GetInfo answered; then "" when this Factry's version was checked live with
     * MigrateSettings (v8.2.3 to v8.3.x), else why the module does not call it.
     */
    private volatile String versionRefusal;

    /** Logged once: GetInfo failed. */
    private volatile boolean versionReadFailureLogged;

    /** The unchecked version last logged, so a version is logged once. */
    private volatile String versionRefusalLogged;

    /**
     * Marks tags whose metadata was just stored: their derived settings are compared with their
     * measurement's, and sent if they differ, by the next {@link #syncTagSettings}. Called on the
     * store-and-forward thread, so it asks Factry nothing.
     */
    void markSettingsPending(Collection<String> tagPaths) {
        settingsPending.addAll(tagPaths);
    }

    /** The measurement this collector writes {@code tagPath} to, or null when the cache has none. */
    Measurement writeTarget(String tagPath, String collectorUUID) {
        Measurement m = getMeasurementByName(tagPath);
        return m != null && collectorUUID != null && collectorUUID.equalsIgnoreCase(m.getCollectorUUID()) ? m : null;
    }

    /**
     * Keeps the settings the module derives from the pending tags' metadata
     * ({@link #derivedSettings}) up to date on the measurement this collector writes each tag
     * to, with MigrateSettings, in calls of at most {@link #MIGRATE_BATCH}. Runs from the
     * scheduler, after the cache refresh, until each tag's settings are stored: Ignition sends the
     * metadata at every start and on every change of a tag's history configuration.
     *
     * <p>MigrateSettings replaces each measurement's whole settings Struct. Right before the calls
     * this collector's measurements are listed once (GetMeasurementsByFilter, paged), and each
     * entry is built from that fresh copy plus the derived settings, so a setting changed in
     * Factry since the last refresh is kept. A measurement the cache holds but the listing lacks
     * stays pending; one the cache no longer holds is dropped. Only Active and Paused measurements
     * are sent. Another collector's measurement is never changed. A failure changes nothing.
     * After PermissionDenied or Unimplemented, or with a Factry version not checked live, no more
     * calls are made until the profile restarts (logged once at INFO); any other failure is logged
     * once per status code and tried again at the next run.
     */
    void syncTagSettings(FactryGrpcClient grpcClient) {
        synchronized (settingsLock) {
            String collectorUUID = grpcClient.getCollectorUUID();
            // Not loaded: writeTarget knows no measurement yet, and the pending tags would be dropped.
            if (settingsPending.isEmpty() || migrateRefused != null || !loaded
                    || collectorUUID == null || collectorUUID.isEmpty() || !migrationSupported(grpcClient)) {
                return;
            }
            // Measurement UUID to the derived settings and the tags they come from.
            Map<String, Map<String, String>> derivedByUuid = new LinkedHashMap<>();
            Map<String, List<String>> pathsByUuid = new HashMap<>();
            for (String path : List.copyOf(settingsPending)) {
                Map<String, String> derived = derivedSettings(tagMetadata.get(path));
                Measurement m = derived.isEmpty() ? null : writeTarget(path, collectorUUID);
                if (m == null || withDerived(m.getSettings(), derived).equals(m.getSettings())) {
                    // Nothing to send; a measurement still to be created gets them at creation.
                    settingsPending.remove(path);
                    continue;
                }
                derivedByUuid.put(m.getUuid(), derived);
                pathsByUuid.computeIfAbsent(m.getUuid(), u -> new ArrayList<>()).add(path);
            }
            List<String> uuids = new ArrayList<>(derivedByUuid.keySet());
            // A few batches per run: the scheduler thread also refreshes the cache. The rest stay
            // pending for the next run.
            int end = Math.min(uuids.size(), MIGRATE_BATCHES_PER_RUN * MIGRATE_BATCH);
            if (end == 0) {
                return;
            }
            // Read again right before the calls, so each entry is built from a fresh copy: one
            // paged listing of this collector's measurements.
            Set<String> wanted = new HashSet<>(uuids.subList(0, end));
            Map<String, Measurement> listed = new HashMap<>();
            try {
                listOwn(grpcClient, collectorUUID, m -> {
                    if (wanted.contains(m.getUuid())) {
                        listed.put(m.getUuid(), m);
                    }
                });
            } catch (RuntimeException e) {
                if (!freshReadFailureLogged) {
                    freshReadFailureLogged = true;
                    logger.info("Could not list this collector's measurements before storing the settings derived"
                            + " from the tags ({}); tried again later. Logged once.", e.getMessage());
                }
                return;
            }
            for (int from = 0; from < end; from += MIGRATE_BATCH) {
                List<MigrateMeasurementSettings> request = new ArrayList<>();
                Map<String, Struct> sent = new LinkedHashMap<>();
                for (String uuid : uuids.subList(from, Math.min(end, from + MIGRATE_BATCH))) {
                    Measurement m = listed.get(uuid);
                    if (m == null && uuidToMeasurement.containsKey(uuid)) {
                        // Cached but not listed: not dropped on a doubt, tried again at the next run.
                        if (!missingFromListingLogged) {
                            missingFromListingLogged = true;
                            logger.info("Measurement {} was not in this collector's list from Factry; its settings"
                                    + " are tried again later. Logged once.", uuid);
                        }
                        continue;
                    }
                    if (m != null && isActive(m)) {
                        // The truth: into the cache as it is.
                        uuidToMeasurement.computeIfPresent(uuid, (k, cur) -> m);
                    }
                    Struct settings = m == null ? null : withDerived(m.getSettings(), derivedByUuid.get(uuid));
                    if (m == null || !(isActive(m) || "paused".equalsIgnoreCase(m.getStatus()))
                            || !collectorUUID.equalsIgnoreCase(m.getCollectorUUID())
                            || settings.equals(m.getSettings())) {
                        // Deleted, Discovered, gone or up to date since: nothing to send.
                        pathsByUuid.get(uuid).forEach(settingsPending::remove);
                        continue;
                    }
                    request.add(MigrateMeasurementSettings.newBuilder().setMeasurementUUID(uuid).setSettings(settings).build());
                    sent.put(uuid, settings);
                }
                if (request.isEmpty()) {
                    continue;
                }
                try {
                    grpcClient.migrateMeasurementSettings(request);
                } catch (RuntimeException e) {
                    io.grpc.Status.Code code = io.grpc.Status.fromThrowable(e).getCode();
                    if (code == io.grpc.Status.Code.PERMISSION_DENIED || code == io.grpc.Status.Code.UNIMPLEMENTED) {
                        migrateRefused = e.getMessage();
                        logger.info("Factry refuses to store the measurement settings derived from the tags ({});"
                                + " not tried again until the profile restarts.", e.getMessage());
                    } else if (migrateFailuresLogged.add(code)) {
                        logger.info("Factry did not store the settings of {} measurement(s) ({}); tried again"
                                + " later. Logged once per answer.", request.size(), e.getMessage());
                    }
                    return;
                }
                long sentAt = now.getAsLong();
                sent.forEach((uuid, settings) -> {
                    // Into the copy cached now, and kept over a refresh that was under way.
                    SentSettings record = new SentSettings(settings, sentAt);
                    settingsSent.put(uuid, record);
                    putSettings(uuid, record);
                    for (String path : pathsByUuid.get(uuid)) {
                        // Unless new metadata came in during the call.
                        if (derivedByUuid.get(uuid).equals(derivedSettings(tagMetadata.get(path)))) {
                            settingsPending.remove(path);
                        }
                    }
                });
                logger.debug("Stored the derived settings of {} measurement(s)", sent.size());
            }
        }
    }

    /** Every measurement of {@code collectorUUID}, one paged listing. */
    private void listOwn(FactryGrpcClient grpcClient, String collectorUUID, Consumer<Measurement> out) {
        long offset = 0;
        long total = Long.MAX_VALUE;
        while (offset < total) {
            Measurements page = grpcClient.getMeasurementsByFilter(GetMeasurementsByFilterRequest.newBuilder()
                    .addCollectorUUIDs(collectorUUID)
                    .setPagination(Pagination.newBuilder().setLimit(pageSize).setOffset(offset).build())
                    .build());
            if (page.getMeasurementsCount() == 0) {
                break;
            }
            total = page.getTotal() > 0 ? page.getTotal() : total;
            page.getMeasurementsList().forEach(out);
            offset += page.getMeasurementsCount();
        }
    }

    private static boolean isActive(Measurement m) {
        return "active".equalsIgnoreCase(m.getStatus());
    }

    /** Settings this module sent with MigrateSettings, and when (by {@link #now}). */
    private record SentSettings(Struct settings, long sentAt) {}

    /**
     * Per measurement UUID, the settings this module sent since the last full refresh listed it,
     * so a refresh whose listing was read before the call does not put the old ones back.
     */
    private final ConcurrentHashMap<String, SentSettings> settingsSent = new ConcurrentHashMap<>();

    /** Writes {@code sent}'s settings into the cached copy of {@code uuid}. */
    private void putSettings(String uuid, SentSettings sent) {
        uuidToMeasurement.computeIfPresent(uuid, (k, cur) -> cur.toBuilder().setSettings(sent.settings()).build());
    }

    /**
     * Whether this Factry's version is one MigrateSettings was checked live on (v8.2.3 to v8.3.x):
     * the call also replaces the collector's own settings, and an unchecked version may do more.
     */
    private boolean migrationSupported(FactryGrpcClient grpcClient) {
        // One read: forgetFactryVersion() may null the field from another thread meanwhile.
        String refusal = versionRefusal;
        if (refusal == null) {
            String version;
            try {
                version = grpcClient.getInfo().getVersion();
            } catch (RuntimeException e) {
                if (!versionReadFailureLogged) {
                    versionReadFailureLogged = true;
                    logger.info("Could not read Factry's version ({}), so the settings derived from the tags are not"
                            + " stored yet; asked again at every run. Logged once.", e.getMessage());
                }
                return false;
            }
            refusal = checkedVersion(version) ? "" : version;
            versionRefusal = refusal;
            if (!refusal.isEmpty() && !version.equals(versionRefusalLogged)) {
                versionRefusalLogged = version;
                logger.info("Factry {} is not a version MigrateSettings was checked on (v8.2.3 to v8.3.x), so the"
                        + " settings derived from the tags are not stored on existing measurements.", version);
            }
        }
        return refusal.isEmpty();
    }

    /**
     * Forgets Factry's version, to read it again at the next {@link #syncTagSettings}: after a
     * failed connection check or a reconnect, Factry may have been upgraded underneath.
     */
    void forgetFactryVersion() {
        versionRefusal = null;
    }

    /** True for v8.2.3 and later v8.2 versions, and every v8.3 version. */
    static boolean checkedVersion(String version) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("^v?(\\d+)\\.(\\d+)\\.(\\d+)").matcher(
                version == null ? "" : version.trim());
        if (!m.find()) {
            return false;
        }
        int major = Integer.parseInt(m.group(1));
        int minor = Integer.parseInt(m.group(2));
        int patch = Integer.parseInt(m.group(3));
        return major == 8 && (minor == 3 || (minor == 2 && patch >= 3));
    }

    /** Factry derives EngineeringSpecs from a nested "Config" attribute group with these keys. */
    private static final String CONFIG_GROUP = "Config";

    /** Ignition tag property -> Factry Config attribute key, for the string-valued specs. */
    private static final Map<String, String> CONFIG_STRING_KEYS = Map.of("engUnit", "UoM");

    /** Ignition tag property -> Factry Config attribute key, for the number-valued specs. */
    private static final Map<String, String> CONFIG_NUMBER_KEYS = Map.of(
            "engLow", "ValueMin",
            "engHigh", "ValueMax");

    /**
     * Build the CreateMeasurement for a new tag, mapping the cached Ignition metadata onto the
     * shape Factry expects.
     *
     * Factry does not store engineering specs as a field. The server derives EngineeringSpecs from
     * a nested "Config" attribute group (UoM, ValueMin, ValueMax, LimitLo, LimitHi), the same shape
     * the Excel measurement importer writes. So the known Ignition properties go into that group,
     * numeric ones as numbers. LimitLo and LimitHi have no plain Ignition tag property equivalent
     * (those values live in alarm setpoints) and stay unset.
     *
     * Every other property goes to the metadata map, which stays the channel for arbitrary extras:
     * it is the only one that round-trips back on read, since the read-side Measurement proto has
     * no 'attributes' field. On read, Config-derived engineeringSpecs wins over the metadata map.
     */
    static CreateMeasurement buildCreateMeasurement(String tagPath, String dataType,
                                                    Map<String, String> metadata) {
        CreateMeasurement.Builder builder = CreateMeasurement.newBuilder()
                .setName(tagPath)
                .setAutoOnboard(true)
                .setDataType(dataType);

        if (metadata == null || metadata.isEmpty()) {
            return builder.build();
        }

        // The tag's data type and Deadband Style go to the settings, which the gRPC read calls
        // return (see syncTagSettings for existing measurements).
        Struct settings = withDerived(Struct.getDefaultInstance(), derivedSettings(metadata));
        if (settings.getFieldsCount() > 0) {
            builder.setSettings(settings);
        }

        Struct.Builder config = Struct.newBuilder();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                continue;
            }

            if ("description".equals(name)) {
                builder.setDescription(value);
                continue;
            }

            String stringKey = CONFIG_STRING_KEYS.get(name);
            if (stringKey != null) {
                config.putFields(stringKey, Value.newBuilder().setStringValue(value).build());
                continue;
            }

            String numberKey = CONFIG_NUMBER_KEYS.get(name);
            if (numberKey != null) {
                try {
                    config.putFields(numberKey, Value.newBuilder()
                            .setNumberValue(Double.parseDouble(value.trim()))
                            .build());
                    continue;
                } catch (NumberFormatException nfe) {
                    logger.debug("Property '{}' of '{}' is not numeric ('{}'), keeping it in metadata",
                            name, tagPath, value);
                }
            }

            builder.putMetadata(name, MetadataProperty.newBuilder()
                    .setDataType(MetadataProperty.DataType.STRING)
                    .setValue(Value.newBuilder().setStringValue(value).build())
                    .build());
        }

        if (config.getFieldsCount() > 0) {
            builder.setAttributes(Struct.newBuilder()
                    .putFields(CONFIG_GROUP, Value.newBuilder()
                            .setStructValue(config.build())
                            .build()));
        }

        logger.debug("Applied cached metadata to new measurement '{}'", tagPath);
        return builder.build();
    }

    static String toFactryDataType(Object value) {
        if (value instanceof Boolean) {
            return "boolean";
        } else if (value instanceof Number) {
            return "number";
        } else if (value instanceof String) {
            return "string";
        }
        return null;
    }
}
