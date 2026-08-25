package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    /** Metadata properties cached from doStoreMetadata, applied when creating measurements. */
    private final ConcurrentHashMap<String, Map<String, String>> pendingMetadata = new ConcurrentHashMap<>();

    private final int pageSize = ModuleProperties.getMeasurementCachePageSize();

    public void refresh(FactryGrpcClient grpcClient) {
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

            CreateMeasurement.Builder builder = CreateMeasurement.newBuilder()
                    .setName(tagPath)
                    .setAutoOnboard(true)
                    .setDataType(dataType);

            Map<String, String> metadata = pendingMetadata.remove(tagPath);
            if (metadata != null && !metadata.isEmpty()) {
                String description = metadata.remove("description");
                if (description != null) {
                    builder.setDescription(description);
                }
                if (!metadata.isEmpty()) {
                    // Write remaining properties (engUnit, engLow, engHigh, ...) to BOTH
                    // the metadata map and the attributes Struct. The metadata map is the
                    // symmetric read/write channel — it round-trips back via
                    // Measurement.metadata on queryMetadata (the read-side proto has no
                    // 'attributes' field, so attributes alone would be write-only). We keep
                    // attributes too so the values remain visible in the Factry UI.
                    Struct.Builder attrs = Struct.newBuilder();
                    for (Map.Entry<String, String> entry : metadata.entrySet()) {
                        attrs.putFields(entry.getKey(),
                                Value.newBuilder().setStringValue(entry.getValue()).build());
                        builder.putMetadata(entry.getKey(), MetadataProperty.newBuilder()
                                .setDataType(MetadataProperty.DataType.STRING)
                                .setValue(Value.newBuilder().setStringValue(entry.getValue()).build())
                                .build());
                    }
                    builder.setAttributes(attrs);
                }
                logger.debug("Applied cached metadata to new measurement '{}'", tagPath);
            }

            CreateMeasurement createMeasurement = builder.build();

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
                    CreateMeasurement.Builder builder = CreateMeasurement.newBuilder()
                            .setName(path)
                            .setAutoOnboard(true)
                            .setDataType(pathToDataType.get(path));

                    Map<String, String> metadata = pendingMetadata.remove(path);
                    if (metadata != null && !metadata.isEmpty()) {
                        String description = metadata.remove("description");
                        if (description != null) builder.setDescription(description);
                        if (!metadata.isEmpty()) {
                            Struct.Builder attrs = Struct.newBuilder();
                            for (Map.Entry<String, String> e : metadata.entrySet()) {
                                attrs.putFields(e.getKey(),
                                        Value.newBuilder().setStringValue(e.getValue()).build());
                                builder.putMetadata(e.getKey(), MetadataProperty.newBuilder()
                                        .setDataType(MetadataProperty.DataType.STRING)
                                        .setValue(Value.newBuilder().setStringValue(e.getValue()).build())
                                        .build());
                            }
                            builder.setAttributes(attrs);
                        }
                    }
                    req.addMeasurements(builder.build());
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
        pendingMetadata.merge(tagPath, new HashMap<>(properties), (existing, incoming) -> {
            existing.putAll(incoming);
            return existing;
        });
        logger.debug("Cached metadata for '{}': {}", tagPath, properties);
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
