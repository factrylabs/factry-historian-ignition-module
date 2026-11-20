package io.factry.historian.gateway;

import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
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
import io.factry.historian.proto.TimeseriesDatabase;
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
import java.util.function.Predicate;

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

    /** Collector UUID → collector name, for grouping in the browse tree. */
    private volatile ConcurrentHashMap<String, String> collectorNames = new ConcurrentHashMap<>();

    /**
     * What reads and browse use, apart from the write path's maps above. Several Sites can share
     * one organisation: each Site's gateway writes to its own time-series database with its own
     * collector, and the same tag gets the same measurement name at every Site, since names are
     * unique per database only. Every measurement whose database is known has the read key
     * {@code [<database name>]<name>}. The bare name, which a tag's own path gives, reads only
     * this collector's measurement, or another collector's in the own Site's one database; so a
     * tag's own path never reads another Site's measurement. Read key → UUID.
     */
    private volatile ConcurrentHashMap<String, String> readKeyToUUID = new ConcurrentHashMap<>();
    /** UUID → the key browse publishes: the bare name when it reads this measurement, else qualified. */
    private volatile ConcurrentHashMap<String, String> uuidToReadKey = new ConcurrentHashMap<>();
    /** Measurement UUID → database UUID, where known. */
    private volatile ConcurrentHashMap<String, String> uuidToDatabase = new ConcurrentHashMap<>();
    /** Time-series database UUID → name, from the last full refresh. */
    private volatile Map<String, String> databaseNames = Map.of();
    /**
     * The databases this collector has measurements in, or the only database there is. When it
     * is exactly one, that is the own Site, and every collector's measurement in it reads by its
     * bare name. Otherwise (none yet, or several, for example after a measurement was made in
     * another Site's database by hand) only this collector's own measurements do. Null when
     * Factry did not list its databases: every measurement reads by its bare name, as before
     * Sites were told apart.
     */
    private volatile Set<String> ownDatabases = Set.of();

    /**
     * Metadata properties cached from doStoreMetadata, applied when creating measurements. Creating a
     * measurement does not remove them: a failed create or a measurement deleted in Factry creates it
     * again, and Ignition may not send the metadata again.
     */
    private final ConcurrentHashMap<String, Map<String, String>> tagMetadata = new ConcurrentHashMap<>();

    private final int pageSize = ModuleProperties.getMeasurementCachePageSize();

    /** The current time; tests replace it. */
    LongSupplier now = System::currentTimeMillis;

    private final long fullRefreshMillis = ModuleProperties.getMeasurementCacheFullRefreshSeconds() * 1000;
    /** Whether a full refresh has worked, and when the last one did, by {@link #now}. */
    private volatile boolean loaded = false;
    private volatile long lastFullRefresh;
    /**
     * The newest updatedAt of this collector's measurements the cache has seen, by Factry's clock;
     * null before it has seen one. {@link #sync} asks for the changes after it.
     */
    private volatile Timestamp ownChangesSince;

    /** Sleeps between the polls for a just-created measurement: about 3 s in all. */
    private final long[] pollBackoffMs;

    public MeasurementCache() {
        this(new long[] {100L, 200L, 400L, 800L, 1500L});
    }

    /** For tests, which poll without waiting. */
    MeasurementCache(long[] pollBackoffMs) {
        this.pollBackoffMs = pollBackoffMs.clone();
    }

    /**
     * The scheduled update. Reloads everything when the last full refresh is
     * {@code measurement.cache.full.refresh.seconds} old or none has worked yet. Otherwise it asks
     * Factry only for this collector's measurements changed since the newest one the cache has
     * seen (GetMeasurements with {@code since}, which also returns Paused and Deleted ones), so
     * the write path still follows a measurement paused, resumed or deleted in Factry within one
     * tick, without downloading every measurement of the organisation each time. Measurements of
     * other collectors and assets change from the next full refresh.
     */
    public synchronized void sync(FactryGrpcClient grpcClient) {
        if (!loaded || now.getAsLong() - lastFullRefresh >= fullRefreshMillis) {
            refresh(grpcClient);
            return;
        }
        String collectorUUID = grpcClient.getCollectorUUID();
        MeasurementRequest.Builder request = MeasurementRequest.newBuilder().setCollectorUUID(collectorUUID);
        Timestamp since = ownChangesSince;
        if (since != null) {
            request.setSince(since);
        }
        Measurements changes = grpcClient.getMeasurements(request.build());
        for (Measurement m : changes.getMeasurementsList()) {
            applyChange(m, collectorUUID);
            since = later(since, m);
        }
        ownChangesSince = since;
        if (changes.getMeasurementsCount() > 0) {
            logger.debug("Measurement cache: applied {} changes of this collector's measurements",
                    changes.getMeasurementsCount());
        }
    }

    /** Put a changed measurement of this collector in the cache the way a full refresh would. */
    private void applyChange(Measurement m, String collectorUUID) {
        String uuid = m.getUuid();
        Measurement old = uuidToMeasurement.get(uuid);
        if (old != null && !old.getName().equals(m.getName())) {
            tagPathToUUID.remove(old.getName(), uuid);
        }
        inactiveByName.computeIfPresent(m.getName(), (name, cached) -> cached.getUuid().equals(uuid) ? null : cached);
        forgetReadKey(uuid);
        if (isActive(m)) {
            cacheMeasurement(m, collectorUUID);
        } else {
            uuidToMeasurement.remove(uuid);
            tagPathToUUID.remove(m.getName(), uuid);
            // A full refresh does not list Deleted measurements either: the write path creates
            // the tag's measurement again.
            if (!"deleted".equalsIgnoreCase(m.getStatus())) {
                putInactive(inactiveByName, m, collectorUUID);
            }
        }
    }

    private static Timestamp later(Timestamp since, Measurement m) {
        if (!m.hasUpdatedAt()) {
            return since;
        }
        return since == null || Timestamps.compare(m.getUpdatedAt(), since) > 0 ? m.getUpdatedAt() : since;
    }

    /** Whether Factry listed its databases at the last full refresh; null before the first. */
    private Boolean databasesListed = null;
    /**
     * This collector's default database as Factry's REST API last gave it, and for which collector;
     * null when it never did or answered without one. Kept when a later call fails.
     */
    private String lastDefaultDatabase = null;
    private String lastDefaultCollector = null;
    /** What the log last said about the default database, so it is said once per change. */
    private String defaultDatabaseReport = null;
    /** Measurements in databases Factry did not list, at the last full refresh. */
    private int unlistedCount = 0;

    /**
     * Factry's time-series databases, UUID → name, or null when Factry does not list them to this
     * collector, or lists none: then Sites are not told apart. Says so once, and again only when
     * the answer changes. A connection error is thrown.
     */
    private Map<String, String> listDatabases(FactryGrpcClient grpcClient) {
        Map<String, String> databases = new LinkedHashMap<>();
        String failure = null;
        try {
            for (TimeseriesDatabase d : grpcClient.getTimeseriesDatabases().getTimeseriesDatabasesList()) {
                databases.put(d.getUuid(), d.getName());
            }
            if (databases.isEmpty()) {
                failure = "it listed none";
            }
        } catch (RuntimeException e) {
            if (FactryGrpcClient.isConnectionError(e)) {
                throw e;
            }
            failure = e.getMessage();
        }
        boolean listed = failure == null;
        if (!Boolean.valueOf(listed).equals(databasesListed)) {
            if (listed) {
                if (databasesListed != null) {
                    logger.info("Factry lists its time-series databases again; reads and browse tell Sites apart");
                }
            } else {
                logger.warn("Factry did not list its time-series databases ({}); reads and browse cover every"
                        + " measurement of the organisation as one Site", failure);
            }
            databasesListed = listed;
        }
        return listed ? databases : null;
    }

    /**
     * This collector's default database from Factry's REST API. When the call fails (REST port
     * not reachable, a time-out, a refusal), the last answer for this collector stays: a blip
     * must not bring back reading another Site by tag path. An answer without one drops it.
     * Logged once per change.
     */
    private String defaultDatabase(FactryGrpcClient grpcClient, Map<String, String> databases) {
        String collectorUUID = grpcClient.getCollectorUUID();
        if (!Objects.equals(collectorUUID, lastDefaultCollector)) {
            lastDefaultDatabase = null;
            lastDefaultCollector = collectorUUID;
        }
        String report;
        try {
            lastDefaultDatabase = grpcClient.getDefaultDatabaseUUID();
            report = lastDefaultDatabase != null
                    ? "This collector's default time-series database: "
                        + databases.getOrDefault(lastDefaultDatabase, lastDefaultDatabase)
                    : "Factry's REST API names no default time-series database for this collector; going by"
                        + " where its measurements are";
        } catch (RuntimeException e) {
            report = "Could not ask Factry's REST API for this collector's default time-series database ("
                    + e.getMessage() + "); " + (lastDefaultDatabase != null
                        ? "keeping " + databases.getOrDefault(lastDefaultDatabase, lastDefaultDatabase)
                        : "going by where its measurements are");
        }
        if (!report.equals(defaultDatabaseReport)) {
            logger.info(report);
            defaultDatabaseReport = report;
        }
        return lastDefaultDatabase;
    }

    /** Read every page of a measurement list, handing each measurement to {@code take}. */
    private long loadAll(FactryGrpcClient grpcClient, GetMeasurementsByFilterRequest.Builder filter,
                         Consumer<Measurement> take) {
        long offset = 0;
        long serverTotal = Long.MAX_VALUE;
        while (offset < serverTotal) {
            Measurements response = grpcClient.getMeasurementsByFilter(filter.clone()
                    .setPagination(Pagination.newBuilder().setLimit(pageSize).setOffset(offset).build())
                    .build());
            if (response.getTotal() > 0) {
                serverTotal = response.getTotal();
            }
            int pageCount = response.getMeasurementsCount();
            if (pageCount == 0) {
                break;
            }
            response.getMeasurementsList().forEach(take);
            offset += pageCount;
        }
        return offset;
    }

    private String ownSiteDescription() {
        Set<String> own = ownDatabases;
        if (own == null) {
            return "every database";
        }
        if (own.isEmpty()) {
            return "this collector's measurements (no database known yet)";
        }
        Map<String, String> names = databaseNames;
        return own.stream().map(uuid -> names.getOrDefault(uuid, uuid)).sorted().toList().toString();
    }

    /**
     * Reload every measurement of the organisation, the collectors and the assets. Runs at
     * startup, on a settings change and every {@code measurement.cache.full.refresh.seconds}
     * from {@link #sync}.
     */
    public synchronized void refresh(FactryGrpcClient grpcClient) {
        try {
            String ownCollectorUUID = grpcClient.getCollectorUUID();
            Map<String, Measurement> freshByName = new HashMap<>();
            // In load order, so a name this collector has in two databases reads the first's.
            Map<String, Measurement> freshMeasurements = new LinkedHashMap<>();
            Map<String, Measurement> freshInactive = new HashMap<>();
            Timestamp[] freshSince = {null};
            Consumer<Measurement> take = m -> {
                if (m.getCollectorUUID().equalsIgnoreCase(ownCollectorUUID)) {
                    freshSince[0] = later(freshSince[0], m);
                }
                if (isActive(m)) {
                    putPreferringOwn(freshByName, m, ownCollectorUUID);
                    freshMeasurements.put(m.getUuid(), m);
                } else {
                    putInactive(freshInactive, m, ownCollectorUUID);
                }
            };

            // Measurement has no database field, so load database by database to learn which
            // Site each one is in. Discovered measurements are in no database yet.
            Map<String, String> freshDatabases = listDatabases(grpcClient);
            int freshUnlisted = 0;
            Map<String, String> databaseOf = new HashMap<>();
            Set<String> freshOwnDatabases = null;
            long total;
            if (freshDatabases == null) {
                total = loadAll(grpcClient, GetMeasurementsByFilterRequest.newBuilder(), take);
            } else {
                freshOwnDatabases = new HashSet<>();
                total = 0;
                boolean anyCollector = ownCollectorUUID == null || ownCollectorUUID.isEmpty();
                for (String database : freshDatabases.keySet()) {
                    Set<String> own = freshOwnDatabases;
                    total += loadAll(grpcClient, GetMeasurementsByFilterRequest.newBuilder().addDatabaseUUIDs(database),
                            m -> {
                                databaseOf.put(m.getUuid(), database);
                                // Not isOwnedBy: a measurement without a collector (a
                                // calculation, a manual entry) says nothing about the Site.
                                if (anyCollector || m.getCollectorUUID().equalsIgnoreCase(ownCollectorUUID)) {
                                    own.add(database);
                                }
                                take.accept(m);
                            });
                }
                Set<String> seen = new HashSet<>(databaseOf.keySet());
                total += loadAll(grpcClient, GetMeasurementsByFilterRequest.newBuilder().addStatuses("Discovered"),
                        m -> {
                            if (seen.add(m.getUuid())) {
                                take.accept(m);
                            }
                        });
                // Measurements in a database Factry did not list: kept, with no database.
                long all = grpcClient.getMeasurementsByFilter(GetMeasurementsByFilterRequest.newBuilder()
                        .setPagination(Pagination.newBuilder().setLimit(1).build()).build()).getTotal();
                // Also after a total that says nothing is missing, while the last reload found some:
                // a total of 0, or a delete racing the count, must not drop them.
                if (all > seen.size() || unlistedCount > 0) {
                    if (all > seen.size()) {
                        logger.info("{} measurements in time-series databases Factry did not list; loading all",
                                all - seen.size());
                    }
                    int before = seen.size();
                    total += loadAll(grpcClient, GetMeasurementsByFilterRequest.newBuilder(), m -> {
                        if (seen.add(m.getUuid())) {
                            take.accept(m);
                        }
                    });
                    freshUnlisted = seen.size() - before;
                }
                // Factry's REST API knows the collector's default database; gRPC does not. Without it,
                // go by where this collector's measurements are.
                String defaultDatabase = defaultDatabase(grpcClient, freshDatabases);
                if (defaultDatabase != null && freshDatabases.containsKey(defaultDatabase)) {
                    freshOwnDatabases.clear();
                    freshOwnDatabases.add(defaultDatabase);
                } else if (freshOwnDatabases.isEmpty() && freshDatabases.size() == 1) {
                    freshOwnDatabases.addAll(freshDatabases.keySet());
                }
            }

            Map<String, String> freshPaths = new HashMap<>();
            for (Measurement m : freshByName.values()) {
                freshPaths.put(m.getName(), m.getUuid());
            }

            // Real names first: a name that happens to look like "[database]name" wins over that key.
            Map<String, String> freshReadKeys = new HashMap<>();
            for (Measurement m : freshMeasurements.values()) {
                if (mayReadByName(m, databaseOf.get(m.getUuid()), freshOwnDatabases, ownCollectorUUID,
                        freshDatabases == null ? 0 : freshDatabases.size())) {
                    String current = freshReadKeys.get(m.getName());
                    if (current == null || takesName(freshMeasurements.get(current), m, ownCollectorUUID)) {
                        freshReadKeys.put(m.getName(), m.getUuid());
                    }
                }
            }
            for (Measurement m : freshMeasurements.values()) {
                String database = databaseOf.get(m.getUuid());
                if (database != null) {
                    freshReadKeys.putIfAbsent(TagPathUtil.qualify(freshDatabases.get(database), m.getName()), m.getUuid());
                }
            }
            Map<String, String> freshKeyOf = new HashMap<>();
            for (Measurement m : freshMeasurements.values()) {
                String database = databaseOf.get(m.getUuid());
                if (m.getUuid().equals(freshReadKeys.get(m.getName()))) {
                    freshKeyOf.put(m.getUuid(), m.getName());
                } else if (database != null) {
                    String qualified = TagPathUtil.qualify(freshDatabases.get(database), m.getName());
                    if (m.getUuid().equals(freshReadKeys.get(qualified))) {
                        freshKeyOf.put(m.getUuid(), qualified);
                    }
                }
            }
            if (freshOwnDatabases != null && freshOwnDatabases.size() > 1 && !freshOwnDatabases.equals(ownDatabases)) {
                logger.warn("This collector has measurements in several time-series databases ({}). A tag path"
                                + " reads only this collector's own measurement with that name; read another"
                                + " collector's with histprov:<historian>:/tag:[<database>]<name>",
                        freshOwnDatabases.stream().map(freshDatabases::get).sorted().toList());
            }

            // Swap in freshly-built maps by reference so deleted measurements don't linger and
            // concurrent readers never observe a half-filled map. The UUID map goes first, so a
            // name found in the new path map always has its measurement.
            uuidToMeasurement = new ConcurrentHashMap<>(freshMeasurements);
            tagPathToUUID = new ConcurrentHashMap<>(freshPaths);
            inactiveByName = new ConcurrentHashMap<>(freshInactive);
            uuidToReadKey = new ConcurrentHashMap<>(freshKeyOf);
            readKeyToUUID = new ConcurrentHashMap<>(freshReadKeys);
            uuidToDatabase = new ConcurrentHashMap<>(databaseOf);
            unlistedCount = freshUnlisted;
            databaseNames = freshDatabases != null ? Map.copyOf(freshDatabases) : Map.of();
            ownDatabases = freshOwnDatabases != null ? Set.copyOf(freshOwnDatabases) : null;
            ownChangesSince = freshSince[0];
            lastFullRefresh = now.getAsLong();
            loaded = true;
            logger.info("Measurement cache refreshed, {} active of {} total from Factry, own Site: {}",
                    freshPaths.size(), total, ownSiteDescription());

            // Collector names, for the browse folders
            try {
                var collectors = grpcClient.getCollectors();
                Map<String, String> collectorUUIDToName = new HashMap<>();
                for (Collector c : collectors.getCollectorsList()) {
                    String collectorName = !c.getName().isEmpty() ? c.getName() : c.getUuid();
                    collectorUUIDToName.put(c.getUuid(), collectorName);
                }
                collectorNames = new ConcurrentHashMap<>(collectorUUIDToName);
                logger.debug("Collector mapping refreshed: {} collectors", collectorUUIDToName.size());
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

                // Fetch properties for all assets. A failure (or time-out) here skips the swap
                // below, so asset browse keeps the previous assets and properties instead of
                // showing assets without properties until the next refresh succeeds.
                Map<String, List<AssetProperty>> freshProps = new HashMap<>();
                if (!assetUUIDs.isEmpty()) {
                    GetAssetPropertiesRequest propReq = GetAssetPropertiesRequest.newBuilder()
                            .addAllAssetUUIDs(assetUUIDs)
                            .setRecursive(true)
                            .build();
                    AssetProperties propsResponse = grpcClient.getAssetProperties(propReq);
                    for (AssetProperty prop : propsResponse.getAssetPropertiesList()) {
                        freshProps.computeIfAbsent(prop.getAssetUUID(), k -> new ArrayList<>()).add(prop);
                    }
                }
                assetNameToUUID = new ConcurrentHashMap<>(freshAssetNames);
                uuidToAsset = new ConcurrentHashMap<>(freshAssets);
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

        // Data type from the tag's metadata, else from the value; refuse to create without one
        String dataType = explicitDataType != null ? explicitDataType : dataTypeFor(tagPath, value);
        if (dataType == null) {
            logger.debug("Skipping measurement creation for '{}': value is null or unknown type", tagPath);
            return "";
        }

        // Not Map.of: value is null when only a data type is given (array measurements).
        Map<String, Object> pathToValue = new HashMap<>();
        pathToValue.put(tagPath, value);
        return resolveMissing(Map.of(tagPath, dataType), pathToValue, grpcClient).getOrDefault(tagPath, "");
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
                String dataType = dataTypeFor(path, entry.getValue());
                if (dataType != null) {
                    pathToDataType.put(path, dataType);
                }
                // No data type → no entry in result; the caller defers the point for S&F retry.
            }
        }

        if (!pathToDataType.isEmpty()) {
            result.putAll(resolveMissing(pathToDataType, tagPathToValue, grpcClient));
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
     * @param pathToValue a representative value per path, or none: a Date marks a new
     *                    measurement DateTime when the tag's metadata does not say so
     * @return map from tag path to UUID; absent entries were refused or could not be resolved
     */
    private Map<String, String> resolveMissing(Map<String, String> pathToDataType, Map<String, Object> pathToValue,
                                               FactryGrpcClient grpcClient) {
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
                    toCreate.remove(path);
                    pendingCreations.remove(path);
                }
            }

            // One CreateMeasurementsRequest for all new paths.
            if (!toCreate.isEmpty()) {
                CreateMeasurementsRequest.Builder req = CreateMeasurementsRequest.newBuilder();
                for (String path : toCreate) {
                    req.addMeasurements(buildCreateMeasurement(
                            path, pathToDataType.get(path), metadataFor(path, pathToValue.get(path))));
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
     * (to reuse one Factry already has), while polling for a just-created measurement to
     * become visible, and for a read of a name the cache does not have. Uses the server-side
     * keyword filter, bounded by the write deadline, instead of reloading the entire cache
     * (measurements, collectors, assets, asset properties), so a burst of new tags doesn't
     * trigger repeated full reloads on the store-and-forward thread. The keyword filter matches
     * substrings, so a common name can
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
        return lookUpByName(tagPath, grpcClient, GetMeasurementsByFilterRequest.newBuilder(),
                m -> writeRefusal(m, collectorUUID) == null || writeRefusal(m, collectorUUID) == WriteRefusal.PAUSED);
    }

    /**
     * {@link #lookUpByName(String, FactryGrpcClient)} within the databases or collectors of
     * {@code scope}, reading no further pages once a match is {@code enough}.
     */
    private List<Measurement> lookUpByName(String tagPath, FactryGrpcClient grpcClient,
                                           GetMeasurementsByFilterRequest.Builder scope,
                                           Predicate<Measurement> enough) {
        List<Measurement> matches = new ArrayList<>();
        try {
            long offset = 0;
            for (int page = 1; ; page++) {
                GetMeasurementsByFilterRequest request = scope.clone()
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
                if (!more || matches.stream().anyMatch(enough)) {
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

    /** Names a read looked up in Factry without finding them, with the time of that lookup. */
    private final ConcurrentHashMap<String, Long> readMisses = new ConcurrentHashMap<>();
    private final long readMissMillis = ModuleProperties.getMeasurementLookupMissSeconds() * 1000;

    /**
     * The Active measurement a read of {@code key} uses: a measurement name, which means the own
     * Site's, or {@code [<database name>]<name>} for another Site's (see {@link #readKeyToUUID}).
     * Not in the cache and not an asset's name: one exact-name lookup in Factry, within the own
     * Site's databases or the named one, so a measurement created by hand or by another collector
     * reads at once, without waiting for the next refresh. A name Factry does not have is not
     * asked for again for {@code measurement.lookup.miss.seconds}, so a trend with a misspelt or
     * deleted tag does not cost a lookup on every poll. A connection error is thrown.
     */
    public Measurement findForRead(String readKey, FactryGrpcClient grpcClient) {
        Measurement m = measurementForReadKey(readKey);
        if (m != null || assetNameToUUID.containsKey(readKey)) {
            return m;
        }
        long time = now.getAsLong();
        Long missed = readMisses.get(readKey);
        if (missed != null && time - missed < readMissMillis) {
            return null;
        }
        String collectorUUID = grpcClient.getCollectorUUID();
        // A real name that looks like "[database]name" wins, as in a full refresh: look it up
        // literally first.
        lookUpForRead(readKey, readKey, bareScope(collectorUUID), grpcClient);
        m = measurementForReadKey(readKey);
        String database = databaseUUID(readKey);
        if (m == null && database != null) {
            lookUpForRead(readKey, TagPathUtil.withoutDatabase(readKey),
                    GetMeasurementsByFilterRequest.newBuilder().addDatabaseUUIDs(database), grpcClient);
            m = measurementForReadKey(readKey);
        }
        if (m == null) {
            readMisses.values().removeIf(t -> time - t >= readMissMillis);
            readMisses.put(readKey, time);
        } else {
            readMisses.remove(readKey);
        }
        return m;
    }

    /** The read key of a cached measurement, or null when the cache does not know its Site. */
    public String getReadKey(String measurementUUID) {
        return uuidToReadKey.get(measurementUUID);
    }

    private Measurement measurementForReadKey(String readKey) {
        String uuid = readKeyToUUID.get(readKey);
        return uuid != null ? uuidToMeasurement.get(uuid) : null;
    }

    /** The database UUID a known database name in a read key names, or null. */
    private String databaseUUID(String readKey) {
        String name = TagPathUtil.database(readKey);
        if (name == null) {
            return null;
        }
        for (Map.Entry<String, String> e : databaseNames.entrySet()) {
            if (e.getValue().equals(name)) {
                return e.getKey();
            }
        }
        return null;
    }

    /**
     * Whether a tag path, which gives the bare name, may read {@code m}: when Sites are not told
     * apart, when it is this collector's, or when it is in the own Site's one database. A
     * measurement whose database Factry did not list only in an organisation of one database.
     */
    private static boolean mayReadByName(Measurement m, String database, Set<String> own, String collectorUUID,
                                         int databaseCount) {
        return own == null || isOwnCollector(m, collectorUUID)
                || own.size() == 1 && (database != null ? own.contains(database) : databaseCount <= 1);
    }

    /** Whether {@code m} takes a bare name {@code current} has: only this collector's from another's. */
    private static boolean takesName(Measurement current, Measurement m, String collectorUUID) {
        return current == null || current.getUuid().equals(m.getUuid())
                || !isOwnCollector(current, collectorUUID) && isOwnCollector(m, collectorUUID);
    }

    private static boolean isOwnCollector(Measurement m, String collectorUUID) {
        return collectorUUID == null || collectorUUID.isEmpty() || m.getCollectorUUID().equalsIgnoreCase(collectorUUID);
    }

    /**
     * Look {@code name} up within {@code scope} and give the Active matches {@code readKey}. The
     * lookup stops at the first page with a match that settles the key: when every collector's
     * measurement may have a bare name, only this collector's settles it.
     */
    private void lookUpForRead(String readKey, String name, GetMeasurementsByFilterRequest.Builder scope,
                               FactryGrpcClient grpcClient) {
        String collectorUUID = grpcClient.getCollectorUUID();
        boolean preferOwn = readKey.equals(name) && ownDatabases == null;
        Predicate<Measurement> enough = preferOwn
                ? found -> isActive(found) && isOwnCollector(found, collectorUUID)
                : MeasurementCache::isActive;
        String database = scope.getDatabaseUUIDsCount() == 1 ? scope.getDatabaseUUIDs(0) : null;
        for (Measurement found : lookUpByName(name, grpcClient, scope, enough)) {
            // The scope already keeps a bare name to what it may read (see bareScope).
            if (isActive(found)) {
                uuidToMeasurement.put(found.getUuid(), found);
                if (database != null) {
                    uuidToDatabase.put(found.getUuid(), database);
                }
                putReadKey(readKey, found, collectorUUID);
            }
        }
    }

    /**
     * Where a lookup for a bare name may look: everywhere when Sites are not told apart or the
     * organisation has one database, the own Site's one database, or else this collector only.
     */
    private GetMeasurementsByFilterRequest.Builder bareScope(String collectorUUID) {
        GetMeasurementsByFilterRequest.Builder scope = GetMeasurementsByFilterRequest.newBuilder();
        Set<String> own = ownDatabases;
        if (own == null || databaseNames.size() <= 1 || collectorUUID == null || collectorUUID.isEmpty()) {
            return scope;
        }
        return own.size() == 1 ? scope.addAllDatabaseUUIDs(own) : scope.addCollectorUUIDs(collectorUUID);
    }

    /**
     * Make {@code key} read {@code m}. A bare name goes to a measurement with that real name as
     * {@link #takesName} allows, and takes the key from a measurement that only had it as its
     * qualified key. A qualified key never displaces anything. The caller has checked
     * {@link #mayReadByName} for a bare name.
     */
    private void putReadKey(String key, Measurement m, String collectorUUID) {
        String uuid = m.getUuid();
        boolean bare = key.equals(m.getName());
        String current = readKeyToUUID.get(key);
        Measurement cached = current != null ? uuidToMeasurement.get(current) : null;
        boolean heldByName = cached != null && cached.getName().equals(key);
        if (bare ? heldByName && !takesName(cached, m, collectorUUID) : current != null && !current.equals(uuid)) {
            return;
        }
        readKeyToUUID.put(key, uuid);
        if (bare) {
            uuidToReadKey.put(uuid, key);
        } else {
            uuidToReadKey.putIfAbsent(uuid, key);
        }
        if (current != null && !current.equals(uuid) && key.equals(uuidToReadKey.get(current))) {
            // The one it replaces is browsable by its qualified key only, if that still reads it.
            String other = qualifiedKey(current);
            if (other != null && !other.equals(key) && current.equals(readKeyToUUID.get(other))) {
                uuidToReadKey.put(current, other);
            } else {
                uuidToReadKey.remove(current);
            }
        }
    }

    /** {@code [<database name>]<name>} of a cached measurement, or null when its database is unknown. */
    private String qualifiedKey(String uuid) {
        Measurement m = uuidToMeasurement.get(uuid);
        String database = uuidToDatabase.get(uuid);
        String name = database != null ? databaseNames.get(database) : null;
        return m != null && name != null ? TagPathUtil.qualify(name, m.getName()) : null;
    }

    /** Drop {@code uuid}'s keys: its browse key, its real name and its qualified key. */
    private void forgetReadKey(String uuid) {
        String browseKey = uuidToReadKey.remove(uuid);
        if (browseKey != null) {
            readKeyToUUID.remove(browseKey, uuid);
        }
        Measurement m = uuidToMeasurement.get(uuid);
        if (m != null) {
            readKeyToUUID.remove(m.getName(), uuid);
        }
        String qualified = qualifiedKey(uuid);
        if (qualified != null) {
            readKeyToUUID.remove(qualified, uuid);
        }
    }

    /** The Active measurement a read of {@code name} uses, from the cache only. */
    public Measurement cachedForRead(String name) {
        return measurementForReadKey(name);
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
            // This collector's measurements are the own Site's by definition; another
            // collector's, found by name for the write path, may be another Site's.
            if (m.getCollectorUUID().equalsIgnoreCase(collectorUUID)) {
                String qualified = qualifiedKey(m.getUuid());
                if (qualified != null) {
                    putReadKey(qualified, m, collectorUUID);
                }
                putReadKey(m.getName(), m, collectorUUID);
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
        forgetReadKey(uuid);
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

    /**
     * The name of the collector of a cached measurement, its UUID when Factry did not list that
     * collector, or null when the measurement has no collector.
     */
    public String getCollectorName(String measurementUUID) {
        Measurement m = uuidToMeasurement.get(measurementUUID);
        if (m == null || m.getCollectorUUID().isEmpty()) {
            return null;
        }
        return collectorNames.getOrDefault(m.getCollectorUUID(), m.getCollectorUUID());
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
        // Copy on write, so a create reading the map never sees it change underneath.
        tagMetadata.merge(tagPath, Collections.unmodifiableMap(new HashMap<>(properties)),
                (existing, incoming) -> {
                    Map<String, String> merged = new HashMap<>(existing);
                    merged.putAll(incoming);
                    return Collections.unmodifiableMap(merged);
                });
        logger.debug("Cached metadata for '{}': {}", tagPath, properties);
    }

    /** Drop the cached metadata of a tag that Ignition retired or renamed. */
    public void forgetMetadata(String tagPath) {
        tagMetadata.remove(tagPath);
    }

    /**
     * The cached metadata for a tag that is about to get a measurement. A Date value without a
     * known Ignition data type (for example one stored by a script) still marks the tag as DateTime,
     * so the read side can return Dates.
     */
    private Map<String, String> metadataFor(String tagPath, Object value) {
        Map<String, String> metadata = tagMetadata.get(tagPath);
        if (value instanceof java.util.Date && (metadata == null || !metadata.containsKey(IGNITION_DATA_TYPE))) {
            metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            metadata.put(IGNITION_DATA_TYPE, "DateTime");
        }
        return metadata;
    }

    /** Ignition tag property that holds the tag's data type (e.g. "DateTime"). */
    static final String IGNITION_DATA_TYPE = "dataType";

    /**
     * Measurement setting that keeps the tag's Ignition data type. Factry stores dates as plain
     * numbers, so this is how the read side knows to return Dates. Settings are used because the
     * gRPC read calls return them, while they leave the metadata map empty (Factry v8.2.3).
     */
    static final String IGNITION_DATA_TYPE_SETTING = "ignitionDataType";

    /** The Ignition data type recorded on a measurement at creation, or null when there is none. */
    static String ignitionDataType(Measurement m) {
        Value v = m.getSettings().getFieldsMap().get(IGNITION_DATA_TYPE_SETTING);
        return v != null && v.getKindCase() == Value.KindCase.STRING_VALUE ? v.getStringValue() : null;
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
     *
     * The tag's data type is also copied to the measurement settings, which the gRPC read calls do
     * return (see {@link #IGNITION_DATA_TYPE_SETTING}).
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

        String ignitionType = metadata.get(IGNITION_DATA_TYPE);
        if (ignitionType != null) {
            builder.setSettings(Struct.newBuilder()
                    .putFields(IGNITION_DATA_TYPE_SETTING, Value.newBuilder().setStringValue(ignitionType).build()));
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

    /**
     * Factry data type for a new measurement. The tag's Ignition data type, from the metadata
     * Ignition sends before the first value, wins over the value: Store and Forward hands Boolean
     * values over as Long 0/1 (its storage format has no boolean type), so the value alone would
     * turn a Boolean tag into a number measurement.
     */
    String dataTypeFor(String tagPath, Object value) {
        Map<String, String> metadata = tagMetadata.get(tagPath);
        String fromTag = fromIgnitionDataType(metadata != null ? metadata.get(IGNITION_DATA_TYPE) : null);
        return fromTag != null ? fromTag : toFactryDataType(value);
    }

    /**
     * Map an Ignition DataType name to a Factry data type, or null when there is no mapping.
     * Array types map to their element type.
     */
    private static String fromIgnitionDataType(String ignitionType) {
        if (ignitionType == null) {
            return null;
        }
        String type = ignitionType.endsWith("Array")
                ? ignitionType.substring(0, ignitionType.length() - "Array".length())
                : ignitionType;
        if (type.equals("Boolean")) {
            return "boolean";
        }
        if (type.startsWith("Int") || type.startsWith("Float") || type.equals("DateTime")) {
            return "number";
        }
        if (type.equals("String") || type.equals("Text")) {
            return "string";
        }
        return null;
    }

    static String toFactryDataType(Object value) {
        if (value instanceof Boolean) {
            return "boolean";
        } else if (value instanceof Number || value instanceof java.util.Date) {
            // Dates are stored as epoch milliseconds, see PointWriter.toProtoValue.
            return "number";
        } else if (value instanceof String) {
            return "string";
        }
        return null;
    }
}
