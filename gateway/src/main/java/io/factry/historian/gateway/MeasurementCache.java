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
    /**
     * Measurements that are not Active (Discovered, Paused, ...) by name, for the write path
     * only: browse and query see Active measurements. Rebuilt on every refresh, so a name that
     * exists only in another status costs no lookup per flush.
     */
    private volatile ConcurrentHashMap<String, Measurement> inactiveByName = new ConcurrentHashMap<>();
    /**
     * Tag paths for which this collector has asked Factry once to create its own measurement,
     * although another collector's had the name (see {@link WriteRefusal#OTHER_COLLECTOR}).
     */
    private final Set<String> ownCreationTried = ConcurrentHashMap.newKeySet();

    private volatile ConcurrentHashMap<String, String> assetNameToUUID = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<String, Asset> uuidToAsset = new ConcurrentHashMap<>();
    /** Asset UUID → list of properties belonging to that asset. */
    private volatile ConcurrentHashMap<String, List<AssetProperty>> assetProperties = new ConcurrentHashMap<>();

    /** Measurement UUID → collector name, for grouping in the browse tree. */
    private volatile ConcurrentHashMap<String, String> measurementToCollectorName = new ConcurrentHashMap<>();

    /** Metadata properties cached from doStoreMetadata, applied when creating measurements. */
    private final ConcurrentHashMap<String, Map<String, String>> pendingMetadata = new ConcurrentHashMap<>();

    private final int pageSize = ModuleProperties.getMeasurementCachePageSize();

    /** Sleeps between the polls for a just-created measurement: about 3 s in all. */
    private final long[] pollBackoffMs;

    public MeasurementCache() {
        this(new long[] {100L, 200L, 400L, 800L, 1500L});
    }

    /** For tests, which poll without waiting. */
    MeasurementCache(long[] pollBackoffMs) {
        this.pollBackoffMs = pollBackoffMs.clone();
    }

    public void refresh(FactryGrpcClient grpcClient) {
        try {
            String ownCollectorUUID = grpcClient.getCollectorUUID();
            // Fetch all measurements using pagination
            Map<String, Measurement> freshByName = new HashMap<>();
            Map<String, Measurement> freshMeasurements = new HashMap<>();
            Map<String, Measurement> freshInactive = new HashMap<>();
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
                        putPreferringOwn(freshByName, m, ownCollectorUUID);
                        freshMeasurements.put(m.getUuid(), m);
                    } else {
                        putInactive(freshInactive, m, ownCollectorUUID);
                    }
                }
                offset += pageCount;
            }

            Map<String, String> freshPaths = new HashMap<>();
            for (Measurement m : freshByName.values()) {
                freshPaths.put(m.getName(), m.getUuid());
            }

            // Swap in freshly-built maps by reference so deleted measurements don't linger and
            // concurrent readers never observe a half-filled map. The UUID map goes first, so a
            // name found in the new path map always has its measurement.
            uuidToMeasurement = new ConcurrentHashMap<>(freshMeasurements);
            tagPathToUUID = new ConcurrentHashMap<>(freshPaths);
            inactiveByName = new ConcurrentHashMap<>(freshInactive);
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
        // Fast path: already cached ("" when values for it are not written, see WriteRefusal)
        String uuid = writeUUID(tagPath, grpcClient.getCollectorUUID());
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

        return resolveMissing(Map.of(tagPath, dataType), grpcClient).getOrDefault(tagPath, "");
    }

    /**
     * Resolve UUIDs for a batch of tag paths.
     *
     * All paths already in the cache are returned immediately; the others are resolved by
     * {@link #resolveMissing}. This keeps the store-and-forward thread's cost proportional
     * to the number of distinct new tags in the flush, not the number of points.
     *
     * @param tagPathToValue map from tag path to a representative value (used only for
     *                       data-type inference — the actual written values come from
     *                       the caller)
     * @return map from tag path to UUID; absent entries mean either that values for the
     *         path are not written (see {@link #writeRefusal(String, String)}) or that the
     *         measurement could not be resolved or created (caller should mark those points
     *         for S&amp;F retry)
     */
    public Map<String, String> batchGetOrCreateUUIDs(
            Map<String, Object> tagPathToValue, FactryGrpcClient grpcClient) {

        String collectorUUID = grpcClient.getCollectorUUID();
        Map<String, String> result = new HashMap<>();

        // Split into cache hits and paths that need a UUID.
        Map<String, String> pathToDataType = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : tagPathToValue.entrySet()) {
            String path = entry.getKey();
            String uuid = writeUUID(path, collectorUUID);
            if (uuid != null) {
                if (!uuid.isEmpty()) {
                    result.put(path, uuid);
                }
            } else {
                String dataType = toFactryDataType(entry.getValue());
                if (dataType != null) {
                    pathToDataType.put(path, dataType);
                }
                // Unsupported value type → no entry in result (caller skips the point).
            }
        }

        if (!pathToDataType.isEmpty()) {
            result.putAll(resolveMissing(pathToDataType, grpcClient));
        }
        return result;
    }

    /**
     * Resolve UUIDs for tag paths the cache does not have, creating the measurements Factry
     * does not have either.
     *
     * For each path not being created by another thread, Factry is first asked for the exact
     * name ({@link #lookUpByName}): CreateMeasurements with a name that already exists does
     * not return the existing measurement but adds a duplicate in status Discovered. What the
     * lookup finds is cached. An Active measurement with the name is used; a name that exists
     * only in another status is refused ({@link WriteRefusal#NOT_ACTIVE}) and not created
     * again. When the measurement with the name is another collector's, this collector's own
     * is created once per tag ({@link WriteRefusal#OTHER_COLLECTOR}). The names Factry does
     * not have are batched into a single {@code CreateMeasurementsRequest}; then all missing
     * paths (newly created plus those delegated to another thread) are polled together with
     * the same lookup: until a measurement with the name turns up, or, for a name another
     * collector had, until one of this collector's turns up.
     *
     * If Factry does not answer a lookup in time (UNAVAILABLE or DEADLINE_EXCEEDED), the
     * rest is abandoned: nothing is created, and only the paths resolved so far are returned.
     * One slow Factry costs a flush one write deadline, not one per new tag.
     *
     * @return map from tag path to UUID; absent entries were refused or could not be resolved
     */
    private Map<String, String> resolveMissing(Map<String, String> pathToDataType, FactryGrpcClient grpcClient) {
        String collectorUUID = grpcClient.getCollectorUUID();
        Map<String, String> result = new HashMap<>();
        // Paths created although another collector's measurement has the name, and those of
        // them whose creation Factry answered (or refused): tried, once the polling is done.
        Set<String> clashes = new HashSet<>();
        Set<String> triedClashes = new HashSet<>();

        // Separate paths this call will create from ones another thread is already
        // handling (pendingCreations guards against duplicate-creation races).
        List<String> toCreate = new ArrayList<>();
        List<String> alreadyPending = new ArrayList<>();
        try {
            for (String path : pathToDataType.keySet()) {
                if (!pendingCreations.add(path)) {
                    alreadyPending.add(path);
                    continue;
                }
                // Track the slot in toCreate right away so the finally below releases it
                // even if the lookup throws.
                toCreate.add(path);
                // Double-check the cache after acquiring the creation slot, then Factry itself.
                String uuid = writeUUID(path, collectorUUID);
                if (uuid == null) {
                    lookUpByName(path, grpcClient).forEach(m -> cacheMeasurement(m, collectorUUID));
                    uuid = writeUUID(path, collectorUUID);
                    Measurement other = writeTarget(path, collectorUUID);
                    if (uuid == null && other != null) {
                        logger.info("Measurement '{}' belongs to another collector ({}); asking Factry"
                                + " once to create one for this collector", path, other.getCollectorUUID());
                        clashes.add(path);
                        continue;
                    }
                }
                if (uuid != null) {
                    logger.debug("Measurement '{}' already exists in Factry: '{}'", path, uuid);
                    if (!uuid.isEmpty()) {
                        result.put(path, uuid);
                    }
                    pendingMetadata.remove(path);
                    toCreate.remove(path);
                    pendingCreations.remove(path);
                }
            }

            // One CreateMeasurementsRequest for all new paths.
            if (!toCreate.isEmpty()) {
                CreateMeasurementsRequest.Builder req = CreateMeasurementsRequest.newBuilder();
                for (String path : toCreate) {
                    req.addMeasurements(buildCreateMeasurement(
                            path, pathToDataType.get(path), pendingMetadata.remove(path)));
                }
                try {
                    grpcClient.createMeasurements(req.build());
                } catch (RuntimeException e) {
                    if (!FactryGrpcClient.isConnectionError(e)) {
                        triedClashes.addAll(clashes);
                    }
                    throw e;
                }
                triedClashes.addAll(clashes);
                logger.info("Batch-created {} measurements in one gRPC call", toCreate.size());
            }

            // Poll for all missing paths together (newly created + delegated). This runs on
            // the store-and-forward thread, so keep it cheap: targeted lookups and a tight
            // fixed backoff, about 3 s of sleeps in the worst case.
            Set<String> stillMissing = new HashSet<>(toCreate);
            stillMissing.addAll(alreadyPending);

            for (int attempt = 0; attempt < pollBackoffMs.length && !stillMissing.isEmpty(); attempt++) {
                try {
                    Thread.sleep(pollBackoffMs[attempt]);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                stillMissing.removeIf(path -> {
                    lookUpByName(path, grpcClient).forEach(m -> cacheMeasurement(m, collectorUUID));
                    String uuid = writeUUID(path, collectorUUID);
                    if (uuid == null) {
                        // Another collector's turned up: done, unless this is the clash we are
                        // creating ours for, which waits for one of this collector's.
                        return !clashes.contains(path) && writeTarget(path, collectorUUID) != null;
                    }
                    if (!uuid.isEmpty()) {
                        result.put(path, uuid);
                    }
                    return true;
                });
            }

            // Fallback: full refresh for anything still not visible (e.g. unusual characters
            // in the path defeat the server-side keyword match).
            if (!stillMissing.isEmpty()) {
                logger.warn("Batch: {} paths not visible after retries, triggering full refresh",
                        stillMissing.size());
                refresh(grpcClient);
                for (String path : stillMissing) {
                    String uuid = writeUUID(path, collectorUUID);
                    if (uuid == null && clashes.contains(path)) {
                        logger.info("Factry made no measurement '{}' for this collector", path);
                    } else if (uuid == null) {
                        logger.warn("Measurement not found after batch create + full refresh: '{}'", path);
                    } else if (!uuid.isEmpty()) {
                        result.put(path, uuid);
                    }
                }
            }
        } catch (Exception e) {
            if (FactryGrpcClient.isConnectionError(e)) {
                logger.warn("Factry did not answer while resolving measurements ({}), {} of {} new tag"
                                + " paths left for store and forward to retry",
                        e.getMessage(), pathToDataType.size() - result.size(), pathToDataType.size());
            } else {
                logger.error("Batch measurement creation failed", e);
            }
        } finally {
            toCreate.forEach(pendingCreations::remove);
            ownCreationTried.addAll(triedClashes);
        }

        return result;
    }

    /** Page size of a single-name lookup, and the most pages it reads. */
    private static final int LOOKUP_PAGE_SIZE = 50;
    private static final int LOOKUP_MAX_PAGES = 5;
    /** How often the "lookup stopped at its page bound" warning is repeated per tag at most. */
    private static final long LOOKUP_BOUND_WARNING_INTERVAL_MS = 10 * 60 * 1000L;
    private final ConcurrentHashMap<String, Long> lookupBoundWarnedAt = new ConcurrentHashMap<>();

    /**
     * Targeted lookup of a measurement by its exact name, used before creating a measurement
     * (to reuse one Factry already has) and while polling for a just-created measurement to
     * become visible. Uses the server-side keyword filter, bounded by the write deadline,
     * instead of reloading the entire cache (measurements, collectors, assets, asset
     * properties), so a burst of new tags doesn't trigger repeated full reloads on the
     * store-and-forward thread. The keyword filter matches substrings, so a common name can
     * have many matches: it reads further pages until an Active measurement with the name
     * that this collector may write to, or its Paused one, turns up, up to
     * {@link #LOOKUP_MAX_PAGES}.
     *
     * Returns every measurement with exactly that name, of any status; empty when Factry has
     * none. A connection error (UNAVAILABLE, DEADLINE_EXCEEDED) is thrown, so the caller
     * stops rather than waiting out the deadline once per tag. Any other error is logged and
     * ends the lookup with the matches so far, so the measurement is created as before.
     */
    private List<Measurement> lookUpByName(String tagPath, FactryGrpcClient grpcClient) {
        String collectorUUID = grpcClient.getCollectorUUID();
        List<Measurement> matches = new ArrayList<>();
        try {
            long offset = 0;
            for (int page = 1; ; page++) {
                GetMeasurementsByFilterRequest request = GetMeasurementsByFilterRequest.newBuilder()
                        .setKeyword(tagPath)
                        .setPagination(Pagination.newBuilder().setLimit(LOOKUP_PAGE_SIZE).setOffset(offset).build())
                        .build();
                Measurements response = grpcClient.getMeasurementsByFilterWithDeadline(request);
                for (Measurement m : response.getMeasurementsList()) {
                    // Keyword search is fuzzy/substring, so match the exact name ourselves.
                    if (tagPath.equals(m.getName())) {
                        matches.add(m);
                    }
                }
                int count = response.getMeasurementsCount();
                offset += count;
                boolean more = count == LOOKUP_PAGE_SIZE
                        && (response.getTotal() == 0 || offset < response.getTotal());
                if (!more || matches.stream().anyMatch(m -> writeRefusal(m, collectorUUID) == null
                        || writeRefusal(m, collectorUUID) == WriteRefusal.PAUSED)) {
                    break;
                }
                if (page == LOOKUP_MAX_PAGES) {
                    warnLookupBound(tagPath, offset, response.getTotal());
                    break;
                }
            }
        } catch (RuntimeException e) {
            if (FactryGrpcClient.isConnectionError(e)) {
                throw e;
            }
            logger.debug("Targeted lookup for '{}' failed: {}", tagPath, e.getMessage());
        }
        return matches;
    }

    private void warnLookupBound(String tagPath, long read, long total) {
        long now = System.currentTimeMillis();
        Long last = lookupBoundWarnedAt.get(tagPath);
        if (last == null || now - last >= LOOKUP_BOUND_WARNING_INTERVAL_MS) {
            lookupBoundWarnedAt.put(tagPath, now);
            logger.warn("Lookup of '{}' stopped after {} of {} keyword matches without finding an Active"
                    + " measurement with that name (logged at most every 10 minutes per tag)", tagPath, read, total);
        }
    }

    private static boolean isActive(Measurement m) {
        return "active".equalsIgnoreCase(m.getStatus());
    }

    /**
     * Insert a measurement found by a lookup into the live maps, so later flushes hit the
     * cache: an Active one into the maps browse and query use, any other into
     * {@code inactiveByName}; under its name only if this collector's does not have it already.
     */
    private void cacheMeasurement(Measurement m, String collectorUUID) {
        if (isActive(m)) {
            uuidToMeasurement.put(m.getUuid(), m);
            Measurement cached = getMeasurementByName(m.getName());
            if (cached == null || !isOwnedBy(cached, collectorUUID) || isOwnedBy(m, collectorUUID)) {
                tagPathToUUID.put(m.getName(), m.getUuid());
            }
        } else {
            putInactive(inactiveByName, m, collectorUUID);
        }
    }

    /** Why the module does not write a tag's values to the measurement Factry has under its name. */
    public enum WriteRefusal {
        /**
         * Factry has a measurement with the name, but not an Active one (for example Discovered).
         * Creating another would only add a Discovered duplicate, so the values are discarded
         * until the measurement is Active, from the first cache refresh after that.
         */
        NOT_ACTIVE,
        /**
         * The measurement with the name belongs to another collector. Factry answers OK to
         * CreatePoints for it but drops the points ("measurements were not defined on
         * collector"). Measurement names are unique per time-series database, and nothing in
         * the gRPC API tells which database a measurement is in, so the module asks Factry once
         * per tag to create its own: if the other one is in another database, that gives an
         * Active measurement to write to. If not, Factry adds a Discovered duplicate, which also
         * tells later lookups and refreshes, after a restart too, that the module has tried.
         */
        OTHER_COLLECTOR,
        /**
         * This collector's measurement is Paused: Factry's way to tell collectors to stop
         * collecting it without deleting it. Its values are dropped, as Factry's own collectors
         * stop collecting, and it is never created again; from the first cache refresh after it
         * is Active again, new values are stored.
         */
        PAUSED
    }

    /**
     * The one check of whether this collector may write to a measurement: null when it may (or
     * when {@code m} is null), else why not. A measurement without a collector UUID, or a
     * client without one, counts as this collector's, as before. Every write path uses it;
     * browse and query keep every collector's measurements.
     */
    static WriteRefusal writeRefusal(Measurement m, String collectorUUID) {
        if (m == null) {
            return null;
        }
        if (!isOwnedBy(m, collectorUUID)) {
            return WriteRefusal.OTHER_COLLECTOR;
        }
        if (isActive(m)) {
            return null;
        }
        return "paused".equalsIgnoreCase(m.getStatus()) ? WriteRefusal.PAUSED : WriteRefusal.NOT_ACTIVE;
    }

    private static boolean isOwnedBy(Measurement m, String collectorUUID) {
        return collectorUUID == null || collectorUUID.isEmpty() || m.getCollectorUUID().isEmpty()
                || collectorUUID.equalsIgnoreCase(m.getCollectorUUID());
    }

    /**
     * The cached measurement the write path judges for {@code tagPath}: this collector's
     * Active one, else its Paused one, else another Active one, else one in another status
     * (this collector's if several); or null when the cache has none.
     */
    Measurement writeTarget(String tagPath, String collectorUUID) {
        Measurement active = getMeasurementByName(tagPath);
        if (active != null && isOwnedBy(active, collectorUUID)) {
            return active;
        }
        Measurement inactive = inactiveByName.get(tagPath);
        if (inactive != null && writeRefusal(inactive, collectorUUID) == WriteRefusal.PAUSED) {
            return inactive;
        }
        return active != null ? active : inactive;
    }

    /**
     * Why values for {@code tagPath} are not written to {@code target}, its measurement in
     * the cache: null when they may be, when there is no target, or when the target is another
     * collector's and this collector has not yet tried to create its own.
     */
    public WriteRefusal writeRefusal(String tagPath, Measurement target, String collectorUUID) {
        WriteRefusal refusal = writeRefusal(target, collectorUUID);
        if (refusal == WriteRefusal.OTHER_COLLECTOR && !hasTriedOwnCreation(tagPath, collectorUUID)) {
            return null;
        }
        return refusal;
    }

    /** {@link #writeRefusal(String, Measurement, String)} for the cached target of {@code tagPath}. */
    public WriteRefusal writeRefusal(String tagPath, String collectorUUID) {
        return writeRefusal(tagPath, writeTarget(tagPath, collectorUUID), collectorUUID);
    }

    /**
     * Whether this collector has tried to create its own measurement for {@code tagPath}: in
     * this process, or before, as a measurement of its own with that name in another status
     * (the Discovered duplicate such a try leaves) shows.
     */
    private boolean hasTriedOwnCreation(String tagPath, String collectorUUID) {
        if (ownCreationTried.contains(tagPath)) {
            return true;
        }
        Measurement inactive = inactiveByName.get(tagPath);
        return inactive != null && isOwnedBy(inactive, collectorUUID);
    }

    /**
     * What the cache says about writing {@code tagPath}'s values: the UUID to write them to,
     * "" when they are refused, or null when the cache has no measurement with that name or
     * this collector has yet to try creating its own.
     */
    String writeUUID(String tagPath, String collectorUUID) {
        Measurement m = writeTarget(tagPath, collectorUUID);
        if (m == null) {
            return null;
        }
        if (writeRefusal(m, collectorUUID) == null) {
            return m.getUuid();
        }
        return writeRefusal(tagPath, m, collectorUUID) != null ? "" : null;
    }

    /** Forget the create attempts of this process, for a new collector or new settings. */
    public void clearWriteRefusals() {
        ownCreationTried.clear();
    }

    /**
     * Put a non-Active {@code m} under its name, unless a better one has it: this collector's
     * Paused measurement, else another of its own, else another collector's.
     */
    private static void putInactive(Map<String, Measurement> byName, Measurement m, String collectorUUID) {
        Measurement current = byName.get(m.getName());
        if (current == null || inactiveRank(m, collectorUUID) >= inactiveRank(current, collectorUUID)) {
            byName.put(m.getName(), m);
        }
    }

    private static int inactiveRank(Measurement m, String collectorUUID) {
        WriteRefusal refusal = writeRefusal(m, collectorUUID);
        return refusal == WriteRefusal.PAUSED ? 2 : refusal == WriteRefusal.OTHER_COLLECTOR ? 0 : 1;
    }

    /**
     * Put {@code m} under its name, unless this collector's measurement already has that name.
     * Names are unique per time-series database, not per organisation, so several collectors
     * may have one; the write path must find this collector's.
     */
    private static void putPreferringOwn(Map<String, Measurement> byName, Measurement m, String collectorUUID) {
        Measurement current = byName.get(m.getName());
        if (current == null || !isOwnedBy(current, collectorUUID) || isOwnedBy(m, collectorUUID)) {
            byName.put(m.getName(), m);
        }
    }

    /**
     * Remove a measurement from the cache by UUID. The next {@link #getOrCreateUUID} call for
     * its tag path looks the measurement up in Factry first and only re-creates it if Factry
     * no longer has it.
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
     * Every other property goes to the metadata map, which stays the channel for arbitrary extras.
     * Factry stores it and returns it over REST, but on Factry Historian v8.2.3 and v8.2.4 the gRPC
     * read calls (GetMeasurements, GetMeasurementsByFilter) leave Measurement.metadata empty, so
     * these extras do not come back to Ignition. The read side still merges the map in case Factry
     * starts returning it; Config-derived engineeringSpecs wins over it.
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
