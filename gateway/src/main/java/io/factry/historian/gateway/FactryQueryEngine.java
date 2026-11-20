package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.TimeRange;
import com.inductiveautomation.historian.common.model.AggregationType;
import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.HistorianProperties;
import com.inductiveautomation.historian.common.model.data.AggregatedDataPoint;
import com.inductiveautomation.historian.common.model.data.AggregatedPoint;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.common.model.options.DataPointQueryKey;
import com.inductiveautomation.historian.common.model.options.MetadataQueryKey;
import com.inductiveautomation.historian.common.model.options.MetadataQueryOptions;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.AbstractQueryEngine;
import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.historian.gateway.api.query.browsing.BrowsePublisher;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.QueriedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.DefaultProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.MetadataPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.StringPath;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.config.BasicProperty;
import com.inductiveautomation.ignition.common.config.BasicPropertySet;
import com.inductiveautomation.ignition.common.config.Property;
import com.inductiveautomation.ignition.common.config.PropertySet;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataTypeClass;
import com.inductiveautomation.ignition.common.util.LoggerEx;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;

import io.factry.historian.proto.Aggregation;
import io.factry.historian.proto.Asset;
import io.factry.historian.proto.AssetProperty;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;

import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

public class FactryQueryEngine extends AbstractQueryEngine {

    private static final List<AggregationType> SUPPORTED_AGGREGATES = List.of(
            LegacyAggregateAdapter.of(AggregationMode.Average),
            LegacyAggregateAdapter.of(AggregationMode.SimpleAverage),
            LegacyAggregateAdapter.of(AggregationMode.MinMax),
            LegacyAggregateAdapter.of(AggregationMode.Minimum),
            LegacyAggregateAdapter.of(AggregationMode.Maximum),
            LegacyAggregateAdapter.of(AggregationMode.Sum),
            LegacyAggregateAdapter.of(AggregationMode.Count),
            LegacyAggregateAdapter.of(AggregationMode.LastValue),
            LegacyAggregateAdapter.of(AggregationMode.Range),
            LegacyAggregateAdapter.of(AggregationMode.Variance),
            LegacyAggregateAdapter.of(AggregationMode.StdDev)
    );

    /**
     * Points per series per raw-query page. About 20 bytes per numeric point on the wire, so a page
     * of 10 pens stays near 4 MB, well under the 64 MiB message limit.
     */
    static final int RAW_QUERY_PAGE_SIZE = 10_000;

    private volatile FactryHistorianSettings settings;
    private final FactryGrpcClient grpcClient;
    private final MeasurementCache measurementCache;
    private final HistorianMetrics metrics;
    /** The current time; tests replace it. */
    LongSupplier now = System::currentTimeMillis;

    /**
     * Most raw points one query reads over all pens; a range that holds more fails the read. Paging
     * lifts the gRPC message limit, so without this a long range over many pens could fill the heap.
     */
    long maxRawPoints = ModuleProperties.getRawQueryMaxPoints();

    public FactryQueryEngine(
        GatewayContext context,
        String historianName,
        FactryHistorianSettings settings,
        FactryGrpcClient grpcClient,
        MeasurementCache measurementCache,
        HistorianMetrics metrics
    ) {
        super(context, historianName, LoggerEx.newBuilder().build(FactryQueryEngine.class));
        this.settings = settings;
        this.grpcClient = grpcClient;
        this.measurementCache = measurementCache;
        this.metrics = metrics;
        logger.debug("Factry Query Engine initialized");
    }

    @Override
    protected void doBrowse(QualifiedPath root, BrowseFilter filter, BrowsePublisher publisher) {
        try {
            // From the cache only: the scheduled refresh keeps it current. Reloading every
            // measurement on each browse step cost a full download per click.

            // Extract browse prefix from root (AdaptedQualifiedPath)
            String prefix = extractBrowsePrefix(root);

            logger.debug("Browse: prefix='" + prefix + "'");

            String category = TagPathUtil.extractCategory(prefix);

            if (prefix.isEmpty()) {
                // Root level: show two category folders
                publisher.newNode("folder", TagPathUtil.CATEGORY_MEASUREMENTS).hasChildren(true).add();
                publisher.newNode("folder", TagPathUtil.CATEGORY_ASSETS).hasChildren(true).add();
                logger.debug("Browse: published 2 category folders at root");
                return;
            }

            if (TagPathUtil.CATEGORY_MEASUREMENTS.equals(category)) {
                String measPrefix = TagPathUtil.stripCategory(prefix);
                if (!measPrefix.isEmpty() && !measPrefix.endsWith("/")) {
                    measPrefix += "/";
                }
                // Tree vs flat is governed uniformly by the configured delimiter
                // (applied per-measurement in collectMeasurementDisplayToStoredMap),
                // not by which collector the measurement belongs to.
                browsePaths(collectMeasurementDisplayToStoredMap(), measPrefix, publisher);
            } else if (TagPathUtil.CATEGORY_ASSETS.equals(category)) {
                String assetPrefix = TagPathUtil.stripCategory(prefix);
                browseAssets(assetPrefix, publisher);
            } else {
                // Legacy fallback: browse measurements without category prefix
                browsePaths(collectMeasurementDisplayToStoredMap(), prefix, publisher);
            }

        } catch (Exception e) {
            logger.error("Error browsing historian at path '" + root + "'", e);
        }
    }

    private Map<String, String> collectMeasurementDisplayToStoredMap() {
        String delimiter = settings.getDelimiter();
        // Maps a full display path -> the leaf's browse name (the resolution key
        // used as the tag node id). The browse name is the stored measurement name
        // transformed by the configured delimiter; toStoredTagPath reverses it.
        Map<String, String> displayToBrowse = new HashMap<>();
        List<String[]> qualified = new ArrayList<>();
        for (Measurement m : measurementCache.getAllMeasurements()) {
            // The node id is the measurement's read key, so it reads this measurement and no
            // other Site's with the same name. Not listed while the cache does not know its Site.
            String readKey = measurementCache.getReadKey(m.getUuid());
            if (readKey == null) {
                continue;
            }
            // Prefix the display path with the collector name so measurements
            // are grouped under their collector in the browse tree.
            String collectorName = measurementCache.getCollectorName(m.getUuid());
            String browseName = TagPathUtil.toBrowseName(m.getName(), delimiter);
            String displayPath = collectorName != null
                    ? collectorName + "/" + browseName
                    : browseName;
            String database = TagPathUtil.database(readKey);
            if (database != null && !readKey.equals(m.getName())) {
                qualified.add(new String[] {displayPath, TagPathUtil.toBrowseId(readKey, delimiter), database});
            } else {
                displayToBrowse.put(displayPath, TagPathUtil.toBrowseId(readKey, delimiter));
            }
        }
        // A collector with the same name in two databases: every qualified one whose leaf is
        // taken gets its database after its leaf name.
        qualified.sort(java.util.Comparator.comparing((String[] q) -> q[1]));
        for (String[] q : qualified) {
            displayToBrowse.put(displayToBrowse.containsKey(q[0]) ? q[0] + " [" + q[2] + "]" : q[0], q[1]);
        }
        return displayToBrowse;
    }

    private void browsePaths(Map<String, String> displayToStored, String prefix, BrowsePublisher publisher) {
        Set<String> folders = new LinkedHashSet<>();
        // Map from leaf display name to full display path (for stored path lookup)
        Map<String, String> leafDisplayNameToFullPath = new HashMap<>();

        for (String path : displayToStored.keySet()) {
            if (!path.startsWith(prefix)) {
                continue;
            }
            String remaining = path.substring(prefix.length());
            int slashPos = remaining.indexOf('/');
            if (slashPos >= 0) {
                folders.add(remaining.substring(0, slashPos));
            } else if (!remaining.isEmpty()) {
                leafDisplayNameToFullPath.put(remaining, path);
            }
        }

        for (String folder : folders) {
            publisher.newNode("folder", folder).hasChildren(true).add();
        }
        for (Map.Entry<String, String> leaf : leafDisplayNameToFullPath.entrySet()) {
            String displayName = leaf.getKey();
            String storedPath = displayToStored.get(leaf.getValue());
            // Use stored path as tag identifier so queries can resolve it back;
            // displayPath controls what appears in the UI.
            publisher.newNode("tag", storedPath != null ? storedPath : displayName)
                    .displayPath(StringPath.of(displayName))
                    .hasChildren(false)
                    .add();
        }

        logger.debug("Browse: published " + folders.size() + " folders, " + leafDisplayNameToFullPath.size() + " tags");
    }

    /**
     * Browse the asset tree. Uses assetPath for hierarchy and shows asset properties
     * as leaf tags under each asset.
     *
     * @param prefix path segments already navigated (e.g. "" for root, "alma/" for inside asset alma)
     */
    private void browseAssets(String prefix, BrowsePublisher publisher) {
        Map<String, List<Asset>> childrenByParent = buildChildrenByParent(measurementCache.getAllAssets());
        Map<String, Asset> pathToAsset = new HashMap<>();
        computeAssetPaths(childrenByParent, "", "", pathToAsset);

        // Determine which assets are direct children at the current prefix level
        List<Asset> children;
        if (prefix.isEmpty()) {
            // Root level: show assets with no parent
            children = childrenByParent.getOrDefault("", Collections.emptyList());
        } else {
            // Find the asset matching this prefix, then show its children
            String assetPath = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
            Asset parentAsset = pathToAsset.get(assetPath);
            if (parentAsset != null) {
                children = childrenByParent.getOrDefault(parentAsset.getUuid(), Collections.emptyList());

                // Show properties of the current asset as leaf tags
                List<AssetProperty> props = measurementCache.getPropertiesForAsset(parentAsset.getUuid());
                for (AssetProperty prop : props) {
                    Measurement m = measurementCache.getMeasurementByUUID(prop.getMeasurementUUID());
                    String readKey = m != null ? measurementCache.getReadKey(m.getUuid()) : null;
                    String storedPath = readKey != null ? readKey : m != null ? m.getName() : prop.getMeasurementUUID();
                    publisher.newNode("tag", storedPath)
                            .displayPath(StringPath.of(prop.getName()))
                            .hasChildren(false)
                            .add();
                }
                logger.debug("Browse assets: published " + props.size() + " properties for asset '" + assetPath + "'");
            } else {
                children = Collections.emptyList();
            }
        }

        // Publish child assets as folders
        for (Asset child : children) {
            List<AssetProperty> props = measurementCache.getPropertiesForAsset(child.getUuid());
            List<Asset> grandChildren = childrenByParent.getOrDefault(child.getUuid(), Collections.emptyList());
            boolean hasChildren = !props.isEmpty() || !grandChildren.isEmpty();
            publisher.newNode("folder", child.getName()).hasChildren(hasChildren).add();
        }

        logger.debug("Browse assets: prefix='" + prefix + "', " + children.size() + " child assets");
    }

    /**
     * Recursively compute full paths for all assets using the parent-child hierarchy.
     */
    static void computeAssetPaths(
            Map<String, List<Asset>> childrenByParent,
            String parentUUID,
            String parentPath,
            Map<String, Asset> pathToAsset) {
        List<Asset> children = childrenByParent.getOrDefault(parentUUID, Collections.emptyList());
        for (Asset child : children) {
            String fullPath = parentPath.isEmpty() ? child.getName() : parentPath + "/" + child.getName();
            pathToAsset.put(fullPath, child);
            computeAssetPaths(childrenByParent, child.getUuid(), fullPath, pathToAsset);
        }
    }

    /**
     * Build a map from parentUUID → list of child assets.
     */
    static Map<String, List<Asset>> buildChildrenByParent(Collection<Asset> assets) {
        Map<String, List<Asset>> childrenByParent = new HashMap<>();
        for (Asset a : assets) {
            childrenByParent.computeIfAbsent(a.getParentUUID(), k -> new ArrayList<>()).add(a);
        }
        return childrenByParent;
    }

    /**
     * Extract the browse prefix from the root QualifiedPath.
     * Uses AdaptedQualifiedPath.getOriginalPath() via reflection to access
     * the full path including folder: components.
     */
    private String extractBrowsePrefix(QualifiedPath root) {
        if (root == null) {
            return "";
        }

        try {
            Object original = root.getClass().getMethod("getOriginalPath").invoke(root);
            if (original != null) {
                String fullPath = original.toString();
                if (fullPath != null && !fullPath.isEmpty()) {
                    return TagPathUtil.parseFolderPrefix(fullPath);
                }
            }
        } catch (Exception e) {
            logger.debug("getOriginalPath() not available: " + e.getMessage());
        }

        String rootStr = root.toString();
        if (rootStr != null && !rootStr.isEmpty()) {
            return TagPathUtil.parseFolderPrefix(rootStr);
        }

        return "";
    }

    @Override
    protected Optional<Integer> doQueryRaw(RawQueryOptions options, RawPointProcessor processor) {
        logger.debug("Raw query request: " + options);
        long startMs = System.currentTimeMillis();

        try {
            // Map each query key to its historian node AND initialize the processor with
            // a context that carries every key's data type (String / Float / Boolean).
            // This is what tells the framework how to type each result column.
            //
            // The previous code built an EMPTY context, so the framework had no type
            // information and defaulted every column to numeric. A string value then
            // failed to coerce to a number, and because that coercion happens while the
            // framework commits the shared result set, it aborted the ENTIRE query —
            // taking any numeric tags in the same query down with it (0 rows for all).
            // mapKeysToNodes() populates the context from queryForHistoricalNodes(); the
            // processor is initialized with it once Factry has been read (see HeldInit).
            HeldInit<AtomicPoint<?>, RawQueryKey> held = new HeldInit<>(processor);
            Map<RawQueryKey, ? extends HistoricalNode> keyToNode =
                    mapKeysToNodes(options, held, RawQueryKey::source);

            // Every tag unknown: mapKeysToNodes builds no columns, and Ignition would wait 5 s
            // for them. Build them as Bad_NotFound so the read answers at once.
            if (keyToNode.isEmpty()) {
                logger.debug("Raw query resolved no measurements; returning 0 rows");
                held.initialize(notFound(options.getQueryKeys()));
                processor.onComplete();
                return Optional.of(0);
            }

            // Map query keys to measurement UUIDs. Several keys can read one measurement: the
            // elements of an array tag (Arr[0], Arr[1], ...) are all stored in measurement Arr.
            Map<String, List<RawQueryKey>> keysByUuid = new LinkedHashMap<>();
            Map<RawQueryKey, QualityCode> failures = new HashMap<>();

            for (RawQueryKey key : options.getQueryKeys()) {
                QualifiedPath source = key.source();
                String tagPath = toStoredTagPath(source);
                // Only query keys that resolved to a node; mapKeysToNodes omits the rest.
                HistoricalNode node = keyToNode.get(key);
                String uuid = measurementUUID(node);

                if (node instanceof FactryRecord r && r.isArray()) {
                    failures.put(key, wholeArray(tagPath));
                } else if (uuid != null) {
                    keysByUuid.computeIfAbsent(uuid, k -> new ArrayList<>()).add(key);
                    if (settings.isDebugLogging()) {
                        logger.debug("Mapped " + source + " -> " + tagPath + " -> " + uuid);
                    }
                } else {
                    // Node/measurement not found — fail only this key so the framework
                    // emits nulls for its column instead of aborting the whole query.
                    logger.warn("No measurement UUID found for tag path: " + tagPath);
                    failures.put(key, QualityCode.Bad_NotFound);
                }
            }

            List<String> measurementUUIDs = new ArrayList<>(keysByUuid.keySet());
            if (measurementUUIDs.isEmpty()) {
                held.initialize(failures);
                processor.onComplete();
                return Optional.of(0);
            }

            // Build gRPC request using QueryTimeseries (no aggregation = raw). Group by the
            // per-point status tag so Factry returns one series per quality status; without this
            // every point comes back in a single series and reads back as Good, losing bad/
            // uncertain quality.
            QueryTimeseriesRequest.Builder reqBuilder = QueryTimeseriesRequest.newBuilder()
                    .addGroupBy("status");

            options.getTimeRange().ifPresent(tr -> {
                reqBuilder.setStart(Timestamps.fromMillis(tr.startTime().toEpochMilli()));
                reqBuilder.setEnd(Timestamps.fromMillis(tr.endTime().toEpochMilli()));
            });

            // Ignition can cap a raw read at returnSize points. No series can add more than that to
            // the first returnSize, so never fetch more per series.
            Integer returnSize = options.getReturnSize().filter(n -> n > 0).orElse(null);

            // A measurement now spans multiple status-series. Collect all of a pen's points
            // (each carrying its series' quality), then merge-sort by timestamp before emitting.
            Map<RawQueryKey, List<RawPt>> pointsByKey = new LinkedHashMap<>();

            // Read in pages: one answer for a long range does not fit in a gRPC message. Factry
            // applies limit and offset to each series (measurement and status) on its own, in time
            // order, so the same offset moves every series forward together. A series that returns
            // fewer points than the limit is done; only measurements with a full series go on.
            //
            // A pen Factry refuses on its own is marked Bad_Failure and the other pens still draw.
            Set<String> refused = new HashSet<>();
            int pageSize = RAW_QUERY_PAGE_SIZE;
            List<String> pending = measurementUUIDs;
            long offset = 0;
            // Points held: all that were read, or with returnSize only those that can still be
            // among the first returnSize. The cap is checked against this.
            long collected = 0;
            // Last timestamp read per series (measurement and status), to see that a page moved on.
            Map<String, Long> lastTsBySeries = new HashMap<>();
            while (!pending.isEmpty()) {
                long limit = returnSize != null ? Math.min(pageSize, returnSize - offset) : pageSize;
                if (limit <= 0) {
                    break;
                }
                // Ask each pen still paging for its share of what is left under maxRawPoints, plus
                // one. If every series comes back full, the range holds more than the cap and the
                // read fails below; if not, the pens that came back short are done. A point of an
                // array measurement gives a point to each of its element pens.
                long room = Math.max(0, maxRawPoints - collected);
                limit = Math.min(limit, room / pending.stream().mapToInt(u -> keysByUuid.get(u).size()).sum() + 1);
                QueryTimeseriesResponse reply;
                try {
                    reply = grpcClient.queryTimeseries(reqBuilder
                            .clearMeasurementUUIDs()
                            .addAllMeasurementUUIDs(pending)
                            .setLimit(limit)
                            .setOffset(offset)
                            .build());
                } catch (StatusRuntimeException e) {
                    // Large values (long strings) can still push one page over the message limit.
                    // Retry the same offset with smaller pages before giving up.
                    if (e.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED && pageSize > 1) {
                        pageSize /= 2;
                        logger.debug("Raw query page too large, retrying with page size " + pageSize);
                        continue;
                    }
                    Map<String, StatusRuntimeException> failed = penFailures(reqBuilder.build(), pending, e);
                    // A pen Factry refuses does not fail the read: Ignition runs one query per
                    // historian in a loop that the first error ends, so an error here would also
                    // drop other historians' pens from the chart. The read fails when Factry
                    // itself cannot be read, or when no pen can be blamed (the request for all pens
                    // failed, but each pen alone reads fine).
                    if (failed.isEmpty()) {
                        throw e;
                    }
                    failed.forEach((uuid, why) -> keysByUuid.get(uuid).forEach(k -> failures.put(k, penFailed(why))));
                    refused.addAll(failed.keySet());
                    // It also fails when every pen is refused, as when Factry's own database is
                    // down (UNKNOWN on every call). Raw rows come only from points, so Bad_Failure
                    // columns alone would be an empty, Good result: the same as no data.
                    if (refused.size() == measurementUUIDs.size()) {
                        throw failed.values().iterator().next();
                    }
                    // Start over without the refused pens. Their points must go (the column reads
                    // Bad_Failure from the context; old points would make it look partly good),
                    // and with returnSize they may have decided which other pens stopped paging.
                    // Rare, so reading again is cheaper than undoing that.
                    pointsByKey.clear();
                    lastTsBySeries.clear();
                    collected = 0;
                    offset = 0;
                    pending = new ArrayList<>(measurementUUIDs);
                    pending.removeAll(refused);
                    continue;
                }

                // Each page should start where the previous one ended. A Factry that ignores offset
                // answers the first page again, and paging on would loop and duplicate points.
                // Equal timestamps are allowed: series merged under one status can share one. A
                // Factry that repeats a page of equal timestamps slips past this, but the
                // maxRawPoints cap still fails the read.
                String stuck = null;
                for (Series series : reply.getSeriesList()) {
                    Long lastTs = series.getDataPointsCount() > 0
                            ? lastTsBySeries.get(series.getMeasurementUUID() + "|" + seriesStatus(series))
                            : null;
                    if (lastTs != null && series.getDataPoints(0).getTimestamp() < lastTs) {
                        stuck = series.getMeasurementUUID();
                        break;
                    }
                }
                if (stuck != null) {
                    throw new IllegalStateException("Factry answered offset " + offset
                            + " for measurement " + stuck + " with points from before the previous page, so it"
                            + " cannot be read in pages. Use a shorter time range or an aggregated query");
                }

                Set<String> more = new LinkedHashSet<>();
                // Per pen with a full series: the earliest point it would read next.
                Map<String, Long> nextFrom = new HashMap<>();
                for (Series series : reply.getSeriesList()) {
                    String uuid = series.hasMeasurementUUID() ? series.getMeasurementUUID() : "";
                    if (!keysByUuid.containsKey(uuid)) {
                        continue;
                    }
                    if (series.getDataPointsCount() >= limit) {
                        more.add(uuid);
                        nextFrom.merge(uuid,
                                series.getDataPoints(series.getDataPointsCount() - 1).getTimestamp(), Math::min);
                    }
                    QualityCode quality = statusToQuality(seriesStatus(series));
                    for (RawQueryKey key : keysByUuid.get(uuid)) {
                        List<RawPt> pts = pointsByKey.computeIfAbsent(key, k -> new ArrayList<>());
                        for (SeriesPoint pt : series.getDataPointsList()) {
                            pts.add(rawPoint(key, keyToNode.get(key), pt, quality));
                        }
                        collected += series.getDataPointsCount();
                    }
                    if (series.getDataPointsCount() > 0) {
                        lastTsBySeries.put(uuid + "|" + seriesStatus(series),
                                series.getDataPoints(series.getDataPointsCount() - 1).getTimestamp());
                    }
                }
                // With returnSize, only the first returnSize points over all pens are returned, so
                // drop the rest now and stop paging pens whose next points all come after them.
                // Otherwise every pen would hold up to returnSize points and many pens would hit
                // the cap although few points are wanted.
                if (returnSize != null && collected > returnSize) {
                    long[] times = pointsByKey.values().stream().flatMap(List::stream)
                            .mapToLong(RawPt::ts).sorted().toArray();
                    long last = times[returnSize - 1];
                    collected = 0;
                    for (List<RawPt> pts : pointsByKey.values()) {
                        pts.removeIf(p -> p.ts() > last);
                        collected += pts.size();
                    }
                    more.removeIf(uuid -> nextFrom.get(uuid) > last);
                }
                // Returning only part of the range would be worse than failing: Ignition repeats a
                // pen's last value, with Good quality, into every row after its data stops, so a
                // cut-short pen looks like a flat line. Fail like Ignition's own HistoryWriter does
                // at its row limit.
                if (collected > maxRawPoints) {
                    throw new IllegalStateException("the range holds more than "
                            + maxRawPoints + " points (query.raw.max.points). Use a shorter time range"
                            + " or an aggregated query");
                }
                pending = new ArrayList<>(more);
                offset += limit;
            }

            // Like Ignition's own historians (the Core Historian's seed values), each pen also gets
            // its last sample before the start and its first after the end, at their own times
            // and qualities, unless the query asks for none. An on-change tag then has a value at
            // the start of a trend, also when it did not change inside the range. Two requests for
            // all pens; a pen Factry refused has no column left to bound.
            //
            // Ignition 8.3.6 (historian 1.3.6) sets NO_SEED_VALUES on every raw read through its
            // legacy query path (queryTagHistory), so there an explicit includeBoundingValues=True
            // (YES_BOUND_VALUES) has to win over it, as in Ignition's legacy loaders.
            boolean yesBounds = options.getExtendedPropertyValue(HistorianProperties.YES_BOUND_VALUES).orElse(false);
            if (options.getTimeRange().isPresent()
                    && !options.getExtendedPropertyValue(HistorianProperties.NO_BOUND_VALUES).orElse(false)
                    && (yesBounds || !options.getExtendedPropertyValue(HistorianProperties.NO_SEED_VALUES).orElse(false))) {
                List<String> bounded = new ArrayList<>(measurementUUIDs);
                bounded.removeAll(refused);
                long rangeStart = options.getTimeRange().get().startTime().toEpochMilli();
                long rangeEnd = options.getTimeRange().get().endTime().toEpochMilli();
                List<Map<RawQueryKey, RawPt>> bounds = new ArrayList<>();
                bounds.add(boundingPoints(sampleBeforeRequest(bounded, rangeStart), true, keysByUuid, keyToNode));
                // Nothing is stored after now, so a range that ends later has no value after it.
                long nowMs = now.getAsLong();
                if (rangeEnd < nowMs) {
                    bounds.add(boundingPoints(QueryTimeseriesRequest.newBuilder()
                            .addAllMeasurementUUIDs(bounded).addGroupBy("status")
                            .setStart(Timestamps.fromMillis(rangeEnd)).setEnd(Timestamps.fromMillis(nowMs))
                            .setLimit(1).build(), false, keysByUuid, keyToNode));
                }
                for (Map<RawQueryKey, RawPt> byKey : bounds) {
                    for (Map.Entry<RawQueryKey, RawPt> e : byKey.entrySet()) {
                        List<RawPt> pts = pointsByKey.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
                        // The range end is exclusive, so the value after it is a new sample; a
                        // Factry that included the end would otherwise give that sample twice.
                        if (pts.stream().noneMatch(p -> p.ts() == e.getValue().ts())) {
                            pts.add(e.getValue());
                            collected++;
                        }
                    }
                }
            }

            // Ignition's raw reader builds wide rows from one stream: each point closes the rows up
            // to its timestamp, so every pen must be handed over in one time order. Pen by pen,
            // the second pen's points go back in time and the dataset gets rows out of order, with
            // the first pen's last value carried into them.
            List<RawPt> merged = new ArrayList<>((int) collected);
            // Each point carries its key, so merging adds no wrapper object per point, and each pen's
            // list is dropped once copied. A large read is held about once in the module.
            for (var it = pointsByKey.values().iterator(); it.hasNext(); ) {
                merged.addAll(it.next());
                it.remove();
            }
            merged.sort(Comparator.comparingLong(RawPt::ts));
            // Like Ignition's own historians, returnSize counts points across all pens. The value
            // before the start comes first and the one after the end last, as in the Core
            // Historian's limit over its seeded query.
            if (returnSize != null && merged.size() > returnSize) {
                merged = merged.subList(0, returnSize);
            }

            held.initialize(failures);
            int totalPoints = 0;
            for (RawPt p : merged) {
                AtomicPoint<?> atomicPoint = DataPointFactory.createAtomicPoint(
                        p.value(), p.quality(), Instant.ofEpochMilli(p.ts()), p.key().source());

                if (!processor.onPointAvailable(p.key(), atomicPoint)) {
                    processor.onComplete();
                    return Optional.of(totalPoints);
                }
                totalPoints++;
            }

            processor.onComplete();
            metrics.recordRawQuery(totalPoints, System.currentTimeMillis() - startMs);
            logger.debug("Query completed with " + totalPoints + " total points");
            return Optional.of(totalPoints);

        } catch (Exception e) {
            logger.error("Error querying raw data", e);
            processor.onError(e);
            throw readFailed(e);
        }
    }

    /** One raw point of a pen, with its series' quality. */
    private record RawPt(RawQueryKey key, long ts, Object value, QualityCode quality) {}

    /**
     * Per pen, the latest ({@code latest}) or earliest sample in a request with limit 1 grouped by
     * status: Factry applies the limit to each series, so it answers one sample per measurement
     * and status. None when Factry refuses the request: the range itself was read, and a missing
     * bound is better than a failed read.
     */
    private Map<RawQueryKey, RawPt> boundingPoints(QueryTimeseriesRequest request, boolean latest,
                                       Map<String, List<RawQueryKey>> keysByUuid,
                                       Map<RawQueryKey, ? extends HistoricalNode> keyToNode) {
        if (request.getMeasurementUUIDsCount() == 0) {
            return Map.of();
        }
        QueryTimeseriesResponse reply;
        try {
            reply = grpcClient.queryTimeseries(request);
        } catch (StatusRuntimeException e) {
            // Factry itself cannot be read: fail like the main read does, not with a range that
            // silently lost its bounds.
            if (failsEveryPen(e)) {
                throw e;
            }
            logger.warn("Could not read the values around the range, returning the range without them: "
                    + e.getMessage());
            return Map.of();
        }
        Map<RawQueryKey, RawPt> byKey = new LinkedHashMap<>();
        for (Series series : reply.getSeriesList()) {
            List<RawQueryKey> keys = keysByUuid.get(series.getMeasurementUUID());
            if (keys == null || series.getDataPointsCount() == 0) {
                continue;
            }
            for (RawQueryKey key : keys) {
                RawPt p = rawPoint(key, keyToNode.get(key), series.getDataPoints(0),
                        statusToQuality(seriesStatus(series)));
                byKey.merge(key, p, (a, b) -> (latest ? b.ts() > a.ts() : b.ts() < a.ts()) ? b : a);
            }
        }
        return byKey;
    }

    /**
     * A stored point as the pen's column takes it: a Date for a DateTime tag, one element for an
     * array element. An element the stored row does not hold is Bad; with the row's quality its
     * null would read as 0.0, Good.
     */
    private static RawPt rawPoint(RawQueryKey key, HistoricalNode node, SeriesPoint pt, QualityCode quality) {
        int element = element(node);
        Object value = storedValue(pt.getValue(), element);
        if (element >= 0 && value == null) {
            quality = QualityCode.Bad;
        }
        return new RawPt(key, pt.getTimestamp(), isDateTime(node) ? toDate(value) : value, quality);
    }

    /**
     * The exception a failed read leaves the query engine with. Ignition's history reader
     * (HistorianPointQueryExecutor) stores what onError gets and never reads it back, so on its own
     * a failed read shows as an empty, Good result. An exception out of the query engine reaches
     * HistoryWriter, which ends the dataset with that error: the Power Chart answers with an error
     * and the RPC to Vision and Designer clients carries it.
     */
    static IllegalStateException readFailed(Exception e) {
        String detail = e.getMessage();
        if (!(e instanceof StatusRuntimeException) && e.getCause() instanceof StatusRuntimeException s) {
            detail = detail + ": " + s.getMessage();
        }
        return new IllegalStateException("Factry Historian read failed: " + detail, e);
    }

    /**
     * The pens to blame for a raw page that failed as a whole, with the error each one got: each
     * pen asked for the same page on its own, and the ones that fail again. Factry answers one
     * request for all pens, so one pen it cannot read would otherwise fail every pen. None when
     * Factry itself cannot be read. A single pen is to blame without asking again.
     */
    private Map<String, StatusRuntimeException> penFailures(
            QueryTimeseriesRequest page, List<String> pens, StatusRuntimeException e) {
        if (failsEveryPen(e)) {
            return Map.of();
        }
        if (pens.size() == 1) {
            logger.error("Error querying raw data for measurement " + pens.get(0), e);
            return Map.of(pens.get(0), e);
        }
        Map<String, StatusRuntimeException> failed = new LinkedHashMap<>();
        for (String uuid : pens) {
            try {
                grpcClient.queryTimeseries(page.toBuilder().clearMeasurementUUIDs().addMeasurementUUIDs(uuid).build());
            } catch (StatusRuntimeException alone) {
                if (failsEveryPen(alone)) {
                    return Map.of();
                }
                logger.error("Error querying raw data for measurement " + uuid, alone);
                failed.put(uuid, alone);
            }
        }
        return failed;
    }

    private static <K> Map<K, QualityCode> notFound(Collection<K> keys) {
        Map<K, QualityCode> failures = new HashMap<>();
        keys.forEach(k -> failures.put(k, QualityCode.Bad_NotFound));
        return failures;
    }

    /** The quality a pen's column gets when Factry refuses that pen, with Factry's reason. */
    private static QualityCode penFailed(Exception e) {
        return QualityCode.Bad_Failure.derive("Factry Historian read failed: " + e.getMessage());
    }

    /**
     * Errors that say Factry itself cannot be read (down, too slow, token refused), not that one
     * measurement failed. Those fail the whole read: asking again pen by pen only multiplies the wait.
     */
    static boolean failsEveryPen(Exception e) {
        if (!(e instanceof StatusRuntimeException s)) {
            return false;
        }
        return switch (s.getStatus().getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED, UNAUTHENTICATED, CANCELLED -> true;
            default -> false;
        };
    }

    /**
     * Stands in for the processor while mapKeysToNodes resolves the query keys, and keeps the
     * context it would have initialized the processor with. The engine reads Factry first and then
     * initializes the processor itself, with each pen Factry refused given a bad quality in that
     * context. Ignition turns such a key into a column that reads null with that quality on every
     * row (ErroredResultNode). Once the columns exist there is no clean way left: onKeyFailure
     * shows as Bad_NotFound whatever the quality, and a point with no value shows as 0.0 in a
     * number column, which a script or chart that ignores quality takes for real data.
     * Initializing late is safe: HistorianPointQueryExecutor.startReading runs the whole query
     * before it waits for the columns, in the same thread.
     */
    private static final class HeldInit<P extends DataPoint<?>, K extends DataPointQueryKey>
            implements QueriedPointProcessor<P, K, DataPointType> {
        private final QueriedPointProcessor<P, K, DataPointType> processor;
        private ProcessingContext<K, DataPointType> context;

        HeldInit(QueriedPointProcessor<P, K, DataPointType> processor) {
            this.processor = processor;
        }

        @Override
        public boolean onInitialize(ProcessingContext<K, DataPointType> context) {
            this.context = context;
            return true;
        }

        /** Initializes the real processor, with each key in {@code failures} given that quality. */
        @SuppressWarnings("unchecked")
        void initialize(Map<K, QualityCode> failures) {
            // Rebuilt as a DefaultProcessingContext, the type mapKeysToNodes hands over, so code
            // that reads its properties (CountAsNumber, tests) keeps working. No context means
            // mapKeysToNodes resolved no key at all.
            DefaultProcessingContext.Builder<K, DataPointType> builder = DefaultProcessingContext.builder();
            if (context instanceof DefaultProcessingContext<K, DataPointType> given) {
                given.properties().forEach((key, props) -> {
                    for (var pv : props) {
                        builder.addPropertyValue(key, (Property<Object>) pv.getProperty(), pv.getValue());
                    }
                });
            } else if (context != null) {
                // Not a DefaultProcessingContext: answer from it, with the failures on top.
                ProcessingContext<K, DataPointType> resolved = context;
                processor.onInitialize(new ProcessingContext<>() {
                    @Override
                    public <V> Optional<V> getContextValue(K key, Property<V> property) {
                        QualityCode failure = failures.get(key);
                        if (failure != null && HistorianProperties.QUALITY_CODE.equals(property)) {
                            return Optional.of((V) failure);
                        }
                        return resolved.getContextValue(key, property);
                    }
                });
                return;
            }
            failures.forEach((key, quality) -> builder.addPropertyValue(key, HistorianProperties.QUALITY_CODE, quality));
            processor.onInitialize(builder.build());
        }

        /**
         * mapKeysToNodes reports "no nodes found" here when every key is unknown. The engine then
         * initializes the processor with Bad_NotFound columns instead, which ends the read at once;
         * passed on, the error builds no columns and Ignition waits 5 s for them.
         */
        @Override
        public void onError(QualityCode quality) {
        }

        @Override
        public void onError(Exception e) {
            processor.onError(e);
        }

        // mapKeysToNodes only initializes or fails the processor; points go to the real one.
        @Override
        public boolean onPointAvailable(K key, P point) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onKeyFailure(K key, QualityCode quality) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onComplete() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Collection<AggregationType> getNativeAggregates() {
        return SUPPORTED_AGGREGATES;
    }

    @Override
    protected Optional<Integer> doQueryAggregated(AggregatedQueryOptions options, AggregatedPointProcessor processor) {
        logger.debug("Aggregated query request: " + options);
        long startMs = System.currentTimeMillis();

        try {
            // Map each query key to its historian node AND initialize the processor with a
            // context that carries every key's data type (String / Float / Boolean). This is
            // the same fix applied to doQueryRaw: the previous code built an EMPTY context, so
            // the framework had no type information and defaulted every column to numeric. A
            // string value (the type the Perspective Power Chart uses aggregated queries for)
            // then failed to coerce to a number, and because that coercion happens while the
            // framework commits the shared result set, it aborted the ENTIRE query. mapKeysToNodes()
            // populates the context from queryForHistoricalNodes(); the processor is initialized
            // with it once Factry has been read (see HeldInit).
            HeldInit<AggregatedPoint<?, ?>, AggregatedQueryKey> held = new HeldInit<>(processor);
            Map<AggregatedQueryKey, ? extends HistoricalNode> keyToNode =
                    mapKeysToNodes(options, new CountAsNumber(held, options.getQueryKeys()),
                            AggregatedQueryKey::source);

            // Every tag unknown: build the columns as Bad_NotFound, as in doQueryRaw.
            if (keyToNode.isEmpty()) {
                logger.debug("Aggregated query resolved no measurements; returning 0 rows");
                held.initialize(notFound(options.getQueryKeys()));
                processor.onComplete();
                return Optional.of(0);
            }

            long periodMs = timeWindowToMillis(options.getTimeWindow());
            String period = toPeriod(periodMs);

            // Every pen's points are held until all pens are read (see HeldInit), and Average gives
            // a point for every interval. Bound that the same way as a raw read, before asking
            // Factry anything: fail rather than return part of the range.
            if (options.getTimeRange().isPresent() && periodMs > 0) {
                long rangeMs = options.getTimeRange().get().endTime().toEpochMilli()
                        - options.getTimeRange().get().startTime().toEpochMilli();
                long intervals = Math.max(1, (rangeMs + periodMs - 1) / periodMs);
                long expected = 0;
                for (AggregatedQueryKey key : options.getQueryKeys()) {
                    // MinMax gives two points per interval.
                    expected += "MinMax".equals(key.aggregationType().name()) ? 2 * intervals : intervals;
                }
                if (expected > maxRawPoints) {
                    throw new IllegalStateException("the read would return about " + expected
                            + " points (" + options.getQueryKeys().size() + " pen(s) x " + intervals
                            + " intervals), more than " + maxRawPoints + " (query.raw.max.points)."
                            + " Use a longer interval or a shorter time range");
                }
            }

            var queryKeys = options.getQueryKeys();
            int pointCount = 0;
            Map<AggregatedQueryKey, QualityCode> failures = new HashMap<>();
            // Every pen is read before the processor is initialized, so hold the points until then.
            Map<AggregatedQueryKey, List<AggregatedPoint<?, ?>>> pointsByKey = new LinkedHashMap<>();

            for (AggregatedQueryKey key : queryKeys) {
                QualifiedPath source = key.source();
                String tagPath = toStoredTagPath(source);
                // Only query keys that resolved to a node; mapKeysToNodes omits the rest.
                HistoricalNode node = keyToNode.get(key);
                String uuid = measurementUUID(node);

                if (uuid == null) {
                    logger.warn("No measurement UUID found for aggregated query: " + tagPath);
                    failures.put(key, QualityCode.Bad_NotFound);
                    continue;
                }
                if (node instanceof FactryRecord r && r.isArray()) {
                    failures.put(key, wholeArray(tagPath));
                    continue;
                }

                List<AggregatedPoint<?, ?>> pts = new ArrayList<>();
                try {
                    String aggName = key.aggregationType().name();
                    boolean dateTime = isDateTime(keyToNode.get(key));

                    DataType dataType = keyToNode.get(key).dataType().orElse(DataType.Float8);
                    int element = element(node);
                    if (element >= 0 && !ARRAY_AGGREGATES.contains(aggName)) {
                        // Factry refuses every other aggregate of an array measurement. Refused
                        // here, so no request fails and Average never reaches the twa path.
                        logger.debug("Aggregate " + aggName + " is not supported for array element: " + tagPath);
                        failures.put(key, QualityCode.Bad_Unsupported);
                        continue;
                    }
                    if (dataType == DataType.String && !STRING_AGGREGATES.contains(aggName)) {
                        // Factry refuses every other aggregate of a string measurement.
                        logger.debug("Aggregate " + aggName + " is not supported for String tag: " + tagPath);
                        failures.put(key, QualityCode.Bad_Unsupported);
                        continue;
                    }
                    if (dateTime && !DATE_AGGREGATES.contains(aggName)) {
                        // A sum or spread of dates is not a date; Count is the only number that fits.
                        logger.debug("Aggregate " + aggName + " is not supported for DateTime tag: " + tagPath);
                        // In the context, not onKeyFailure: the processor is not initialized yet,
                        // and once it is, Ignition shows any key failure as Bad_NotFound.
                        failures.put(key, QualityCode.Bad_Unsupported);
                        continue;
                    }
                    // Count stays a number; every other aggregate of a date is itself a date.
                    boolean asDate = dateTime && !"Count".equals(aggName);

                    String factryFunction = "MinMax".equals(aggName) ? null : toFactryAggregateFunction(aggName);
                    if (TIME_WEIGHTED_AVERAGE.equals(factryFunction) && options.getTimeRange().isPresent()) {
                        pointCount += queryTimeWeightedAverage(uuid, source, key, period,
                                periodMs, dataType == DataType.Boolean, asDate, options, pts);
                    } else if ("count".equals(factryFunction) && options.getTimeRange().isPresent()) {
                        pointCount += queryCount(uuid, source, key, period, periodMs, options, pts);
                    } else if ("count".equals(factryFunction) || TIME_WEIGHTED_AVERAGE.equals(factryFunction)) {
                        pointCount += querySingleAggregate(uuid, source, key, factryFunction, period, options,
                                pts, asDate);
                    } else {
                        // MinMax asks Factry for min and max, and gives each bucket both.
                        List<String> functions = factryFunction == null ? List.of("min", "max") : List.of(factryFunction);
                        pointCount += queryEveryBucket(uuid, element, source, key, functions, period, periodMs,
                                options, pts, asDate);
                    }
                    pointsByKey.put(key, pts);
                } catch (Exception e) {
                    // Factry itself cannot be read: the next pen would wait for the same timeout,
                    // so fail the read now. With FillMode.DERIVED each pen is its own query, and
                    // this exception also ends Ignition's loop over those queries.
                    if (failsEveryPen(e)) {
                        throw e;
                    }
                    // Mark only this pen, so the other pens still draw. This holds when it is the
                    // only pen too: with FillMode.DERIVED each pen is its own query.
                    logger.error("Error querying aggregated data for: " + tagPath, e);
                    failures.put(key, penFailed(e));
                }
            }

            held.initialize(failures);
            pointsByKey.forEach((key, pts) -> pts.forEach(p -> processor.onPointAvailable(key, p)));
            processor.onComplete();
            metrics.recordAggregatedQuery(pointCount, System.currentTimeMillis() - startMs);
            logger.debug("Aggregated query completed with " + pointCount + " points");
            return Optional.of(pointCount);

        } catch (Exception e) {
            logger.error("Error in aggregated query", e);
            processor.onError(e);
            throw readFailed(e);
        }
    }

    @Override
    protected Optional<Integer> doQueryMetadata(MetadataQueryOptions options, MetadataPointProcessor processor) {
        logger.debug("Metadata query request with " + options.getQueryKeys().size() + " keys");

        try {
            ProcessingContext context = DefaultProcessingContext.builder().build();
            if (!processor.onInitialize(context)) {
                logger.debug("Processor declined initialization for metadata query");
                processor.onComplete();
                return Optional.of(0);
            }

            int pointCount = 0;
            for (MetadataQueryKey key : options.getQueryKeys()) {
                String tagPath = toStoredTagPath(key.identifier());
                Measurement m = measurementCache.findForRead(tagPath, grpcClient);

                if (m == null) {
                    processor.onKeyFailure(key, QualityCode.Bad_NotFound);
                    continue;
                }

                PropertySet ps = measurementToPropertySet(m);
                MetadataPoint point = MetadataPoint.from(ps, key.identifier());
                processor.onPointAvailable(key, point);
                pointCount++;
            }

            processor.onComplete();
            return Optional.of(pointCount);

        } catch (Exception e) {
            logger.error("Error querying metadata", e);
            processor.onError(e);
            return Optional.empty();
        }
    }

    private static PropertySet measurementToPropertySet(Measurement m) {
        BasicPropertySet ps = new BasicPropertySet();

        if (m.getDatatype() != null && !m.getDatatype().isEmpty()) {
            ps.set(new BasicProperty<>("datatype", String.class), m.getDatatype());
        }
        if (m.getStatus() != null && !m.getStatus().isEmpty()) {
            ps.set(new BasicProperty<>("status", String.class), m.getStatus());
        }
        if (m.getName() != null && !m.getName().isEmpty()) {
            ps.set(new BasicProperty<>("name", String.class), m.getName());
        }

        if (m.hasEngineeringSpecs()) {
            var specs = m.getEngineeringSpecs();
            if (specs.hasUom()) {
                ps.set(new BasicProperty<>("engUnit", String.class), specs.getUom());
            }
            if (specs.hasValueMin()) {
                ps.set(new BasicProperty<>("engLow", Double.class), specs.getValueMin());
            }
            if (specs.hasValueMax()) {
                ps.set(new BasicProperty<>("engHigh", Double.class), specs.getValueMax());
            }
            if (specs.hasLimitLo()) {
                ps.set(new BasicProperty<>("limitLow", Double.class), specs.getLimitLo());
            }
            if (specs.hasLimitHi()) {
                ps.set(new BasicProperty<>("limitHigh", Double.class), specs.getLimitHi());
            }
        }

        // Properties stored via storeMetadata that Factry has no engineering spec for are written to
        // the measurement's metadata map. Surface any that weren't already provided above: the
        // Config-derived engineeringSpecs wins for engUnit/engLow/engHigh. Factry Historian v8.2.3
        // and v8.2.4 leave the metadata map empty on the gRPC read calls, so today this loop finds
        // nothing; it is kept for when Factry returns the map. (The read-side Measurement proto has
        // no 'description'/'attributes' fields, so there is no other channel for these values.)
        Set<String> existing = new HashSet<>();
        for (var pv : ps) {
            existing.add(pv.getProperty().getName());
        }
        for (Map.Entry<String, io.factry.historian.proto.MetadataProperty> e : m.getMetadataMap().entrySet()) {
            String name = e.getKey();
            if (existing.contains(name)) {
                continue; // don't clobber a value already provided by engineeringSpecs
            }
            Object val = protoValueToJava(e.getValue().getValue());
            if (val != null) {
                ps.set(new BasicProperty<>(name, String.class), val.toString());
            }
        }

        return ps;
    }

    private int querySingleAggregate(
            String uuid, QualifiedPath source, AggregatedQueryKey key,
            String factryFunction, String period,
            AggregatedQueryOptions options, List<AggregatedPoint<?, ?>> out, boolean asDate) {

        QueryTimeseriesRequest.Builder reqBuilder = aggregateRequest(uuid, factryFunction, period);

        options.getTimeRange().ifPresent(tr -> {
            reqBuilder.setStart(Timestamps.fromMillis(tr.startTime().toEpochMilli()));
            reqBuilder.setEnd(Timestamps.fromMillis(tr.endTime().toEpochMilli()));
        });

        QueryTimeseriesResponse reply = grpcClient.queryTimeseries(reqBuilder.build());

        int count = 0;
        for (Series series : reply.getSeriesList()) {
            if (!isGoodStatus(seriesStatus(series))) {
                continue; // drop Bad/Uncertain series so they don't blend into the aggregate
            }
            for (SeriesPoint pt : series.getDataPointsList()) {
                Instant timestamp = Instant.ofEpochMilli(pt.getTimestamp());
                Object value = protoValueToJava(pt.getValue());
                if (asDate) {
                    value = toDate(value);
                }

                AggregatedDataPoint<Object, AggregationType> point =
                        AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                                .value(value)
                                .quality(QualityCode.Good)
                                .timestamp(timestamp)
                                .build();

                out.add(point);
                count++;
            }
        }
        return count;
    }

    /**
     * Limits on the raw samples one Average pen may read (see {@link GoodTimeAverage}). A sample
     * is about 25 bytes on the wire, so 50,000 per request stays far below the 4 MiB gRPC
     * message limit, and 200,000 per pen (about 5 MB over at most 10 requests) keeps a trend
     * refresh quick. Buckets beyond this get Factry's twa over the Good samples instead. One-sample
     * reads find the status at a bucket start after a bucket that was not read: one per raw
     * request when needed, and up to 10 for buckets outside a request.
     */
    static final GoodTimeAverage.Budget RAW_BUDGET = new GoodTimeAverage.Budget(50_000, 200_000, 10, 10);

    /**
     * Ignition's "Average" over Good time only (see {@link GoodTimeAverage}). Factry's twa gives
     * the buckets that are Good throughout; a per-status {@code last} aggregate and the sample
     * before the range tell which buckets that is. Only buckets where the status changes are
     * fetched raw, within {@link #RAW_BUDGET}. Buckets that are not Good throughout get a Bad
     * point without a value, so they show as a gap.
     *
     * <p>A Boolean is averaged the same way, as the fraction of Good time it was true: a run bit
     * on for 1 minute of the hour averages 0.017, where Factry's sample {@code mean} says 0.5. Factry's twa refuses Booleans, so a
     * per-status {@code mean} stands in: a Good mean of exactly 0 or 1 means the bucket never
     * changes value, and the bucket needs no raw read when that value is the one held. Other
     * buckets with samples are read raw; beyond the budget a bucket falls back to that mean,
     * marked Uncertain.
     */
    private int queryTimeWeightedAverage(
            String uuid, QualifiedPath source, AggregatedQueryKey key, String period, long periodMs,
            boolean isBoolean, boolean asDate, AggregatedQueryOptions options, List<AggregatedPoint<?, ?>> out) {

        long startMs = options.getTimeRange().get().startTime().toEpochMilli();
        long rangeEndMs = options.getTimeRange().get().endTime().toEpochMilli();
        Map<Long, GoodTimeAverage.Result> values =
                timeWeightedAverage(uuid, period, periodMs, isBoolean, startMs, rangeEndMs);

        // Every bucket gets a point. Ignition fills a bucket without one with a neighbouring
        // value marked Good (checked through its own query path), so a bucket left out would
        // read as a held Good value: in an outage, before the first sample and after now. A
        // bucket without Good time is a Bad point instead, which the Power Chart draws as a gap.
        int count = 0;
        for (long b = startMs; b < rangeEndMs; b += periodMs) {
            GoodTimeAverage.Result r = values.getOrDefault(b, GoodTimeAverage.Result.BAD);
            out.add(
                    AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                            // A DateTime tag is stored as epoch ms; its average is a date too.
                            // A Bad bucket has no value. Not NaN: the Power Chart streams history
                            // as strict JSON, which throws at a NaN and cuts the response off. A
                            // null reads as 0.0 in a Float column, but its quality stays Bad.
                            .value(r.isBad() ? null : asDate ? toDate(r.value()) : r.value())
                            // A numeric fallback is Good: the Power Chart drops points that are
                            // not Good, and the twa over Good samples bridges Bad time inside the
                            // bucket, as Ignition's own average does there. A Boolean fallback is
                            // a sample mean, which can be far off for a Boolean stored on change
                            // (0.5 for a bit on 1 minute of the hour), so it is Uncertain.
                            .quality(r.isBad() ? QualityCode.Bad
                                    : r.exact() || !isBoolean ? QualityCode.Good : QualityCode.Uncertain)
                            .timestamp(Instant.ofEpochMilli(b))
                            .build());
            count++;
        }
        return count;
    }

    /** The average per bucket start; buckets without Good time are Bad or absent. */
    private Map<Long, GoodTimeAverage.Result> timeWeightedAverage(
            String uuid, String period, long periodMs, boolean isBoolean, long startMs, long rangeEndMs) {
        // Stop at now. Fill "previous" would carry the last value into future buckets and weight
        // it up to the bucket end, while Ignition's own Average has no value after now (its
        // interpolator returns nothing past the current time). Factry weights the last bucket
        // up to the query end, so clamping the end is enough; the buckets after now are Bad.
        long endMs = Math.min(rangeEndMs, now.getAsLong());
        if (endMs <= startMs) {
            return Map.of();
        }

        Map<Long, Double> twa = new LinkedHashMap<>();
        if (isBoolean) {
            // No twa: every bucket, without a value. Factry starts its buckets at the query
            // start (checked live on 8.2.3), so these line up with the other aggregates.
            for (long b = startMs; b < endMs; b += periodMs) {
                twa.put(b, null);
            }
        } else {
            for (Series series : grpcClient.queryTimeseries(withRange(
                    aggregateRequest(uuid, TIME_WEIGHTED_AVERAGE, period), startMs, endMs).build()).getSeriesList()) {
                for (SeriesPoint pt : series.getDataPointsList()) {
                    twa.put(pt.getTimestamp(), toDouble(pt.getValue()));
                }
            }
            if (twa.isEmpty()) {
                return Map.of();
            }
        }

        Map<Long, GoodTimeAverage.BucketStatus> statuses = new HashMap<>();
        for (Series series : grpcClient.queryTimeseries(withRange(
                bucketStatusRequest(uuid, period), startMs, endMs).build()).getSeriesList()) {
            boolean good = isGoodStatus(seriesStatus(series));
            for (SeriesPoint pt : series.getDataPointsList()) {
                statuses.computeIfAbsent(pt.getTimestamp(), t -> new GoodTimeAverage.BucketStatus())
                        .add(good, toDouble(pt.getValue()));
            }
        }

        Map<Long, Double> goodMean = new HashMap<>();
        if (isBoolean) {
            for (Series series : grpcClient.queryTimeseries(withRange(
                    perStatusRequest(uuid, "mean", period), startMs, endMs).build()).getSeriesList()) {
                if (isGoodStatus(seriesStatus(series))) {
                    for (SeriesPoint pt : series.getDataPointsList()) {
                        goodMean.put(pt.getTimestamp(), toDouble(pt.getValue()));
                    }
                }
            }
            goodMean.forEach((ts, mean) -> {
                GoodTimeAverage.BucketStatus st = statuses.get(ts);
                if (st != null && st.goodSeries == 1 && (mean == 0 || mean == 1)) {
                    st.sameGood = mean;
                }
            });
        }

        GoodTimeAverage.Sample seed = sampleBefore(uuid, startMs);

        GoodTimeAverage.Source factry = new GoodTimeAverage.Source() {
            @Override
            public Map<Long, Long> counts() {
                Map<Long, Long> counts = new HashMap<>();
                for (Series series : grpcClient.queryTimeseries(withRange(
                        perStatusRequest(uuid, "count", period), startMs, endMs).build()).getSeriesList()) {
                    for (SeriesPoint pt : series.getDataPointsList()) {
                        counts.merge(pt.getTimestamp(), (long) pt.getValue().getNumberValue(), Long::sum);
                    }
                }
                return counts;
            }

            @Override
            public GoodTimeAverage.Sample before(long ts) {
                return sampleBefore(uuid, ts);
            }

            @Override
            public List<GoodTimeAverage.Sample> raw(long from, long to) {
                return samples(grpcClient.queryTimeseries(withRange(
                        QueryTimeseriesRequest.newBuilder().addMeasurementUUIDs(uuid).addGroupBy("status"),
                        from, to).build()));
            }

            @Override
            public Map<Long, Double> fallback() {
                if (isBoolean) {
                    return goodMean;
                }
                Map<Long, Double> values = new HashMap<>();
                for (Series series : grpcClient.queryTimeseries(withRange(
                        goodOnlyTwaRequest(uuid, period), startMs, endMs).build()).getSeriesList()) {
                    for (SeriesPoint pt : series.getDataPointsList()) {
                        values.put(pt.getTimestamp(), toDouble(pt.getValue()));
                    }
                }
                return values;
            }
        };

        return GoodTimeAverage.compute(twa, statuses, seed, periodMs, endMs, RAW_BUDGET, factry);
    }

    private static QueryTimeseriesRequest.Builder withRange(QueryTimeseriesRequest.Builder b, long startMs, long endMs) {
        return b.setStart(Timestamps.fromMillis(startMs)).setEnd(Timestamps.fromMillis(endMs));
    }

    /** Per bucket and status, the last sample: says which statuses a bucket holds. */
    static QueryTimeseriesRequest.Builder bucketStatusRequest(String uuid, String period) {
        return perStatusRequest(uuid, "last", period);
    }

    /** One aggregate per bucket and status, without fill. */
    private static QueryTimeseriesRequest.Builder perStatusRequest(String uuid, String function, String period) {
        return QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
                .addGroupBy("status")
                .setAggregation(Aggregation.newBuilder().setName(function).setPeriod(period).setFillType("none").build());
    }

    /**
     * twa over the samples whose status is exactly "Good", for buckets the raw budget does not
     * cover. It leaves Bad values out but holds the last Good value through Bad time, and the
     * exact match misses Good sub-statuses. That is still closer than the unfiltered twa (which
     * averages Bad values in) and costs one request for the whole range.
     */
    static QueryTimeseriesRequest.Builder goodOnlyTwaRequest(String uuid, String period) {
        return aggregateRequest(uuid, TIME_WEIGHTED_AVERAGE, period)
                .setTags(com.google.protobuf.Struct.newBuilder()
                        .putFields("status", com.google.protobuf.Value.newBuilder().setStringValue("Good").build()));
    }

    /** The last sample before {@code startMs} (end is exclusive), one per status series. */
    static QueryTimeseriesRequest sampleBeforeRequest(String uuid, long startMs) {
        return sampleBeforeRequest(List.of(uuid), startMs);
    }

    /**
     * The same for several measurements in one request: Factry applies the limit per measurement
     * and status. It scans from epoch 0, so on a large database it can be the slowest request of
     * a read (the Average path sends the same one).
     */
    static QueryTimeseriesRequest sampleBeforeRequest(Collection<String> uuids, long startMs) {
        return QueryTimeseriesRequest.newBuilder()
                .addAllMeasurementUUIDs(uuids)
                .addGroupBy("status")
                .setStart(Timestamps.fromMillis(0))
                .setEnd(Timestamps.fromMillis(startMs))
                .setDesc(true)
                .setLimit(1)
                .build();
    }

    /** The last sample before {@code ts}, of any status, or null when there is none. */
    private GoodTimeAverage.Sample sampleBefore(String uuid, long ts) {
        List<GoodTimeAverage.Sample> before = samples(grpcClient.queryTimeseries(sampleBeforeRequest(uuid, ts)));
        return before.isEmpty() ? null : before.get(before.size() - 1);
    }

    /** The raw samples of a status-grouped reply, merged in time order. */
    private static List<GoodTimeAverage.Sample> samples(QueryTimeseriesResponse reply) {
        List<GoodTimeAverage.Sample> out = new ArrayList<>();
        for (Series series : reply.getSeriesList()) {
            boolean good = isGoodStatus(seriesStatus(series));
            for (SeriesPoint pt : series.getDataPointsList()) {
                out.add(new GoodTimeAverage.Sample(pt.getTimestamp(), toDouble(pt.getValue()), good));
            }
        }
        out.sort(Comparator.comparingLong(GoodTimeAverage.Sample::ts));
        return out;
    }

    /** A number as is, a Boolean as 1 or 0 (so its average is the fraction of time true). */
    private static double toDouble(com.google.protobuf.Value v) {
        switch (v.getKindCase()) {
            case NUMBER_VALUE:
                return v.getNumberValue();
            case BOOL_VALUE:
                return v.getBoolValue() ? 1 : 0;
            default:
                return Double.NaN;
        }
    }

    /** The Factry aggregate behind Ignition's (time-weighted) "Average". */
    static final String TIME_WEIGHTED_AVERAGE = "twa";

    /** The Factry request for one aggregate of one measurement, without the time range. */
    static QueryTimeseriesRequest.Builder aggregateRequest(String uuid, String factryFunction, String period) {
        if (TIME_WEIGHTED_AVERAGE.equals(factryFunction)) {
            // Factry's twa is only right with fill "previous": it then holds each value until the
            // next sample, also the last value from before the bucket or query start, and fills
            // empty buckets. With fill "none" it returns wrong values (100 instead of 1.67 for a
            // tag at 100 for 1 minute of the hour). twa refuses group by and its tag filter only
            // matches a status exactly, so it gets no status filter: queryTimeWeightedAverage
            // keeps its value only for buckets that are Good throughout, where the two agree.
            return QueryTimeseriesRequest.newBuilder()
                    .addMeasurementUUIDs(uuid)
                    .setAggregation(Aggregation.newBuilder()
                            .setName(factryFunction)
                            .setPeriod(period)
                            .setFillType("previous")
                            .build());
        }
        // Group by status so Factry aggregates each quality separately, then keep only the Good
        // series. Aggregating across all statuses blends bad samples into the value (e.g. a bad
        // x1000 reading drags the average up); Good-only is faithful and avoids the corruption.
        return QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
                .addGroupBy("status")
                .setAggregation(Aggregation.newBuilder()
                        .setName(factryFunction)
                        .setPeriod(period)
                        .setFillType("none")
                        .build());
    }

    /**
     * An aggregate other than Average and Count, with a point for every bucket (see
     * {@link BucketFill}). Factry gives the aggregate per bucket and status; only its Good series
     * count. A {@code last} per status says which statuses each bucket holds and the value held
     * after it, and the sample before a bucket fills the empty buckets at the start.
     *
     * @param element   for an array element, its index in the stored list (only LastValue gets
     *                  here for one); else -1
     * @param functions the Factry functions; each bucket gets one point per function, in order
     */
    private int queryEveryBucket(
            String uuid, int element, QualifiedPath source, AggregatedQueryKey key, List<String> functions,
            String period, long periodMs, AggregatedQueryOptions options,
            List<AggregatedPoint<?, ?>> out, boolean asDate) {
        TimeRange range = options.getTimeRange()
                .orElseThrow(() -> new IllegalArgumentException("an aggregated read needs a time range"));
        long startMs = range.startTime().toEpochMilli();
        long rangeEndMs = range.endTime().toEpochMilli();
        long nowMs = now.getAsLong();
        // Stop at now, as Average does: buckets after now are Bad, and Factry's buckets start at
        // the query start, so a shorter end does not move them.
        long endMs = Math.min(rangeEndMs, nowMs);

        Map<Long, BucketFill.Status> statuses = new HashMap<>();
        List<Map<Long, Object>> good = new ArrayList<>();
        if (endMs > startMs) {
            for (Series series : grpcClient.queryTimeseries(withRange(
                    bucketStatusRequest(uuid, period), startMs, endMs).build()).getSeriesList()) {
                boolean isGood = isGoodStatus(seriesStatus(series));
                for (SeriesPoint pt : series.getDataPointsList()) {
                    statuses.computeIfAbsent(pt.getTimestamp(), t -> new BucketFill.Status())
                            .add(isGood, storedValue(pt.getValue(), element));
                }
            }
            for (String function : functions) {
                if ("last".equals(function)) {
                    good.add(Map.of()); // LastValue takes its value from the buckets below.
                    continue;
                }
                if ("variance".equals(function) || "stddev".equals(function)) {
                    good.add(populationSpread(uuid, function, period, startMs, endMs));
                    continue;
                }
                good.add(goodAggregate(function, grpcClient.queryTimeseries(withRange(
                        perStatusRequest(uuid, function, period), startMs, endMs).build())));
            }
        }

        List<BucketFill.Bucket> buckets = BucketFill.fill(startMs, rangeEndMs, periodMs, nowMs, statuses,
                ts -> samplesBefore(uuid, element, ts), RAW_BUDGET.seedReadsPerPen(), functions.contains("last"));
        int count = 0;
        for (BucketFill.Bucket bucket : buckets) {
            for (int f = 0; f < functions.size(); f++) {
                String function = functions.get(f);
                Object value = switch (bucket.kind()) {
                    case GOOD_SAMPLES -> "last".equals(function) ? bucket.value() : good.get(f).get(bucket.start());
                    case HELD -> heldValue(function, bucket.value());
                    case BAD -> null;
                };
                // No value is a Bad point. Not NaN: the Power Chart streams history as strict
                // JSON, which throws at a NaN (a variance of one sample can be one). A null
                // reads as 0.0 in a Float column, but its quality stays Bad.
                if (value instanceof Double d && !Double.isFinite(d)) {
                    value = null;
                }
                out.add(AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                        .value(value == null ? null : asDate ? toDate(value) : value)
                        .quality(value == null ? QualityCode.Bad
                                : exact(function, bucket, statuses.get(bucket.start())) ? QualityCode.Good
                                : QualityCode.Uncertain)
                        .timestamp(Instant.ofEpochMilli(bucket.start()))
                        .build());
                count++;
            }
        }
        return count;
    }

    /**
     * Count of Good samples, with a point for every bucket: Ignition would copy a neighbour's
     * count into a bucket without one. A bucket without Good samples counts 0, as Ignition's own
     * Count does; buckets from now on are Bad, as for the other aggregates.
     */
    private int queryCount(
            String uuid, QualifiedPath source, AggregatedQueryKey key, String period, long periodMs,
            AggregatedQueryOptions options, List<AggregatedPoint<?, ?>> out) {
        long startMs = options.getTimeRange().get().startTime().toEpochMilli();
        long rangeEndMs = options.getTimeRange().get().endTime().toEpochMilli();
        long nowMs = now.getAsLong();
        long endMs = Math.min(rangeEndMs, nowMs);
        Map<Long, Object> counts = endMs > startMs
                ? goodAggregate("count", grpcClient.queryTimeseries(withRange(
                        perStatusRequest(uuid, "count", period), startMs, endMs).build()))
                : Map.of();
        int count = 0;
        for (long b = startMs; b < rangeEndMs; b += periodMs) {
            Object value = b < nowMs ? counts.getOrDefault(b, 0.0) : null;
            out.add(AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                    .value(value)
                    .quality(value == null ? QualityCode.Bad : QualityCode.Good)
                    .timestamp(Instant.ofEpochMilli(b))
                    .build());
            count++;
        }
        return count;
    }

    /**
     * Whether a bucket's value is right. With several Good statuses in one bucket ("Good",
     * "GoodLocalOverride"; only data from other collectors has them), Factry aggregates each on
     * its own: min, max, sum and count combine exactly, a mean, spread, variance or standard
     * deviation does not, and LastValue is exact only once the latest Good sample was read.
     */
    private static boolean exact(String function, BucketFill.Bucket bucket, BucketFill.Status status) {
        if (bucket.kind() != BucketFill.Kind.GOOD_SAMPLES) {
            return true;
        }
        return switch (function) {
            case "last" -> bucket.exact();
            case "min", "max", "sum", "count" -> true;
            default -> status == null || status.goodSeries <= 1;
        };
    }

    /**
     * What an empty bucket of a tag holding a Good value gets, as Ignition's own aggregates
     * answer for a block without samples: the value itself, a spread of 0, and no sum, variance
     * or standard deviation (null, so a Bad point).
     */
    private static Object heldValue(String function, Object held) {
        return switch (function) {
            case "spread" -> 0.0;
            case "sum", "variance", "stddev" -> null;
            // Factry's mean of a Boolean is a number (the fraction true), so hold it as one.
            case "mean" -> held instanceof Boolean b ? (b ? 1.0 : 0.0) : held;
            default -> held;
        };
    }

    /**
     * Ignition's Variance or StdDev per bucket, over the Good samples. Ignition divides by the
     * number of samples (population variance, VarianceAggregate); Factry's stddev divides by one
     * less (sample standard deviation) and has no value for a single sample, and Factry has no
     * variance at all (it refuses "variance", checked live on 8.2.3). So the Good samples are
     * counted too and the stddev scaled: variance = s^2 (n - 1) / n, stddev = s sqrt((n - 1) / n),
     * and 0 for one sample, as Ignition gives.
     */
    private Map<Long, Object> populationSpread(String uuid, String function, String period, long startMs, long endMs) {
        Map<Long, Object> stddev = goodAggregate("stddev", grpcClient.queryTimeseries(withRange(
                perStatusRequest(uuid, "stddev", period), startMs, endMs).build()));
        Map<Long, Object> counts = goodAggregate("count", grpcClient.queryTimeseries(withRange(
                perStatusRequest(uuid, "count", period), startMs, endMs).build()));
        Map<Long, Object> out = new HashMap<>();
        counts.forEach((ts, c) -> {
            double n = ((Number) c).doubleValue();
            Double value = n == 1 ? Double.valueOf(0.0)
                    : stddev.get(ts) instanceof Number s ? Double.valueOf("variance".equals(function)
                            ? s.doubleValue() * s.doubleValue() * (n - 1) / n
                            : s.doubleValue() * Math.sqrt((n - 1) / n))
                    : null;
            if (value != null) {
                out.put(ts, value);
            }
        });
        return out;
    }

    /**
     * The aggregate over the Good samples per bucket, from a reply grouped by status. Bad and
     * Uncertain series are left out so they don't blend into the value. A bucket with several
     * Good statuses ("Good", "GoodLocalOverride", ...) gets one value: the smallest minimum, the
     * largest maximum, the summed sum or count, else the value of one of them.
     */
    private static Map<Long, Object> goodAggregate(String function, QueryTimeseriesResponse reply) {
        Map<Long, Object> out = new HashMap<>();
        for (Series series : reply.getSeriesList()) {
            if (!isGoodStatus(seriesStatus(series))) {
                continue;
            }
            for (SeriesPoint pt : series.getDataPointsList()) {
                Object value = protoValueToJava(pt.getValue());
                // Factry answers some buckets without a value (stddev of one sample): no value
                // there, so the bucket is Bad.
                if (value != null) {
                    out.merge(pt.getTimestamp(), value, (a, b) -> combine(function, a, b));
                }
            }
        }
        return out;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object combine(String function, Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            return switch (function) {
                case "min" -> Math.min(x.doubleValue(), y.doubleValue());
                case "max" -> Math.max(x.doubleValue(), y.doubleValue());
                case "sum", "count" -> x.doubleValue() + y.doubleValue();
                default -> a;
            };
        }
        if (a instanceof Comparable x && b != null && a.getClass() == b.getClass()) {
            return switch (function) {
                case "min" -> x.compareTo(b) <= 0 ? a : b;
                case "max" -> x.compareTo(b) >= 0 ? a : b;
                default -> a;
            };
        }
        return a;
    }

    /** The last sample of each status before {@code ts}; for an array, that {@code element}. */
    private List<BucketFill.Sample> samplesBefore(String uuid, int element, long ts) {
        List<BucketFill.Sample> out = new ArrayList<>();
        for (Series series : grpcClient.queryTimeseries(sampleBeforeRequest(uuid, ts)).getSeriesList()) {
            boolean isGood = isGoodStatus(seriesStatus(series));
            for (SeriesPoint pt : series.getDataPointsList()) {
                out.add(new BucketFill.Sample(pt.getTimestamp(), storedValue(pt.getValue(), element), isGood));
            }
        }
        return out;
    }

    static String toFactryAggregateFunction(String name) {
        switch (name) {
            case "Average":
                return TIME_WEIGHTED_AVERAGE;
            case "SimpleAverage":
                return "mean";
            case "Minimum":
                return "min";
            case "Maximum":
                return "max";
            case "Sum":
                return "sum";
            case "Count":
                return "count";
            case "LastValue":
                return "last";
            case "Range":
                return "spread";
            case "Variance":
                return "variance";
            case "StdDev":
                return "stddev";
            default:
                return "mean";
        }
    }

    /**
     * The bucket length in milliseconds. The Factry period is built from this same number
     * ({@link #toPeriod}), so the buckets Factry returns and the ones the module computes itself
     * (GoodTimeAverage) have the same length.
     */
    static long timeWindowToMillis(TimeWindow tw) {
        // Work in milliseconds: Ignition asks for sub-second windows (e.g. 600 ms) when a
        // trend is zoomed in, and rounding them to whole seconds widens every bucket.
        // TimeWindow.toMillis() throws for any unit but MILLIS, and toDuration() throws for
        // date units (days and longer), so pick the conversion by unit.
        long totalMillis = tw.unit().isTimeBased()
                ? tw.toDuration().toMillis()
                : tw.toSeconds() * 1000;
        // TimeWindow refuses sizes below 1, so this can't happen today. Fail rather than send a
        // made-up period: a silent 1 minute was the original bug, and "0s" crashes Factry.
        if (totalMillis <= 0) {
            throw new IllegalArgumentException("Time window shorter than 1 ms: " + tw);
        }
        return totalMillis;
    }

    static String timeWindowToPeriod(TimeWindow tw) {
        return toPeriod(timeWindowToMillis(tw));
    }

    /**
     * A bucket length as a Go-style duration string (e.g. "2h30m", "45s", "600ms").
     * Factry uses Go's time.ParseDuration(), which does not accept ISO 8601.
     */
    static String toPeriod(long totalMillis) {
        StringBuilder sb = new StringBuilder();
        long hours = totalMillis / 3_600_000;
        long minutes = (totalMillis % 3_600_000) / 60_000;
        long seconds = (totalMillis % 60_000) / 1000;
        long millis = totalMillis % 1000;

        if (hours > 0) sb.append(hours).append("h");
        if (minutes > 0) sb.append(minutes).append("m");
        if (seconds > 0) sb.append(seconds).append("s");
        if (millis > 0) sb.append(millis).append("ms");

        return sb.toString();
    }

    @Override
    protected Optional<? extends HistoricalNode> lookupNode(QualifiedPath path) {
        logger.debug("Looking up node: " + path);
        String tagPath = toStoredTagPath(path);
        return findNode(tagPath, path);
    }

    @Override
    protected Map<QualifiedPath, ? extends HistoricalNode> queryForHistoricalNodes(
        Set<QualifiedPath> paths,
        TimeRange timeRange
    ) {
        logger.debug("Query for historical nodes: " + paths.size() + " paths");
        Map<QualifiedPath, HistoricalNode> result = new HashMap<>();
        for (QualifiedPath path : paths) {
            String tagPath = toStoredTagPath(path);
            findNode(tagPath, path).ifPresent(node -> result.put(path, node));
        }
        return result;
    }

    @Override
    protected boolean isEngineUnavailable() {
        return false;
    }

    private Optional<FactryRecord> findNode(String tagPath, QualifiedPath path) {
        // Try measurement
        // The cache first, for the tag and for the array it may be an element of; then Factry.
        Optional<FactryRecord> node = measurementNode(tagPath, path, false);
        if (node.isEmpty()) {
            node = measurementNode(tagPath, path, true);
        }
        if (node.isPresent()) {
            return node;
        }

        // Try asset
        Asset a = measurementCache.getAssetByName(tagPath);
        if (a != null) {
            Instant createdTime = a.hasCreatedAt()
                    ? Instant.ofEpochSecond(a.getCreatedAt().getSeconds(), a.getCreatedAt().getNanos())
                    : Instant.EPOCH;
            return Optional.of(new FactryRecord(a.getUuid(), path, "number", createdTime));
        }

        return Optional.empty();
    }

    /**
     * The node of a cached measurement, or of one element of an array measurement: Ignition
     * historizes {@code Arr[2]} as its own path, PointWriter stores it in the list of measurement
     * {@code Arr}. A measurement whose own name ends in {@code [i]} still wins. With
     * {@code lookUp}, a name the cache does not have is looked up in Factry
     * ({@link MeasurementCache#findForRead}); without, only the cache is read.
     */
    private Optional<FactryRecord> measurementNode(String tagPath, QualifiedPath path, boolean lookUp) {
        Measurement m = lookUp ? measurementCache.findForRead(tagPath, grpcClient) : measurementCache.cachedForRead(tagPath);
        if (m != null) {
            return Optional.of(new FactryRecord(m.getUuid(), -1, path, m.getDatatype(),
                    MeasurementCache.ignitionDataType(m), createdTime(m)));
        }
        ArrayPointMerger.ArrayRef ref = ArrayPointMerger.parseArrayPath(tagPath);
        Measurement base = ref == null ? null : lookUp
                ? measurementCache.findForRead(ref.basePath, grpcClient)
                : measurementCache.cachedForRead(ref.basePath);
        if (base != null && base.getDatatype().startsWith("[]")) {
            String ignitionType = MeasurementCache.ignitionDataType(base);
            return Optional.of(new FactryRecord(base.getUuid(), ref.index, path, base.getDatatype().substring(2),
                    ignitionType != null ? ignitionType.replaceFirst("Array$", "") : null, createdTime(base)));
        }
        return Optional.empty();
    }

    private static Instant createdTime(Measurement m) {
        return m.hasCreatedAt()
                ? Instant.ofEpochSecond(m.getCreatedAt().getSeconds(), m.getCreatedAt().getNanos())
                : Instant.EPOCH;
    }

    /** The Factry UUID a resolved query key reads from, or null when the key did not resolve. */
    private static String measurementUUID(HistoricalNode node) {
        return node instanceof FactryRecord r ? r.measurementUUID() : null;
    }

    private static int element(HistoricalNode node) {
        return node instanceof FactryRecord r ? r.element() : -1;
    }

    /**
     * The quality of the base path of an array tag. Ignition historizes the elements, not the
     * array, and a whole list has no place in one column; read as a value it was 0.0, Good.
     */
    private static QualityCode wholeArray(String tagPath) {
        return QualityCode.Bad_Unsupported.derive("'" + tagPath + "' is an array tag: query its elements,"
                + " such as '" + tagPath + "[0]'");
    }

    void updateSettings(FactryHistorianSettings newSettings) {
        this.settings = newSettings;
    }

    private String toStoredTagPath(QualifiedPath path) {
        String pathStr = path.toString();
        String stored = TagPathUtil.queryPathToStoredPath(pathStr);
        // Another Site's node id names its database first; the delimiter applies to the name only.
        String database = TagPathUtil.database(stored);
        String name = TagPathUtil.withoutDatabase(stored);
        if (TagPathUtil.isAssetQueryPath(pathStr)) {
            // Asset property tags carry the measurement's real name (real '/');
            // never apply the tag-path delimiter reverse to them.
            name = TagPathUtil.restoreFractionSlash(name);
        } else {
            name = TagPathUtil.fromBrowseName(name, settings.getDelimiter());
        }
        return database != null ? TagPathUtil.qualify(database, name) : name;
    }

    static QualityCode statusToQuality(String status) {
        // Factry status values are level names with optional subtype suffixes
        // (e.g. "BadFactryInvalidDataForDatatype", "UncertainInitialValue"). Match by PREFIX so
        // subtypes don't silently fall through to Good.
        if (status == null || status.isEmpty()) {
            return QualityCode.Good;
        }
        if (status.startsWith("Uncertain")) {
            return QualityCode.Uncertain;
        }
        if (status.startsWith("Bad") || status.startsWith("Error")) {
            return QualityCode.Bad;
        }
        return QualityCode.Good;
    }

    /** The per-series status tag from a {@code groupBy=["status"]} query, or "" if absent. */
    private static String seriesStatus(Series series) {
        var stv = series.getTags().getFieldsMap().get("status");
        return stv != null ? stv.getStringValue() : "";
    }

    /** A status that maps to Good quality (Good level, or an untagged series). */
    private static boolean isGoodStatus(String status) {
        return statusToQuality(status) == QualityCode.Good;
    }

    /**
     * Aggregates of a String tag: Factry 8.2.3 refuses every other aggregate of a string
     * measurement (checked live), so those read Bad_Unsupported, like a Sum of dates.
     */
    private static final Set<String> STRING_AGGREGATES = Set.of("LastValue", "Count");

    /**
     * Aggregates of an array element. Factry stores an array tag as one measurement holding a list
     * per row, and aggregates it only with first, last and count; it refuses every other
     * aggregate, twa included (checked live on 8.2.3). Count counts stored rows, and a row is
     * written whenever any element changes.
     */
    private static final Set<String> ARRAY_AGGREGATES = Set.of("LastValue", "Count");

    /**
     * Aggregates that still mean something for a DateTime tag. Factry stores dates as epoch
     * milliseconds, so the min, max, last value and mean of those numbers are dates too, and Count
     * is a count. Sum, Range, Variance and StdDev of dates are not dates, so they return Bad.
     */
    private static final Set<String> DATE_AGGREGATES = Set.of(
            "Average", "SimpleAverage", "MinMax", "Minimum", "Maximum", "LastValue", "Count");

    /** True for a measurement that holds an Ignition DateTime tag (stored as epoch milliseconds). */
    private static boolean isDateTime(HistoricalNode node) {
        return node != null && node.dataType().orElse(null) == DataType.DateTime;
    }

    /** Factry returns a date as its epoch milliseconds; Ignition expects a Date. */
    static Object toDate(Object value) {
        return value instanceof Number ? new java.util.Date(Math.round(((Number) value).doubleValue())) : value;
    }

    /**
     * Gives a Count of a tag that is not numeric (String, DateTime, ...) a number column. The
     * framework types a Count column after the tag (CountAggregate returns the tag's own type
     * class), then coerces each value to that type, so a count of 600 would come out as the text
     * "600" or the Date 1970-01-01 00:00:00.600. Everything else passes through to the real processor.
     */
    private static final class CountAsNumber implements AggregatedPointProcessor {
        private final QueriedPointProcessor<AggregatedPoint<?, ?>, AggregatedQueryKey, DataPointType> delegate;
        private final List<AggregatedQueryKey> keys;

        CountAsNumber(QueriedPointProcessor<AggregatedPoint<?, ?>, AggregatedQueryKey, DataPointType> delegate,
                      List<AggregatedQueryKey> keys) {
            this.delegate = delegate;
            this.keys = keys;
        }

        @Override
        public boolean onInitialize(ProcessingContext<AggregatedQueryKey, DataPointType> context) {
            if (!(context instanceof DefaultProcessingContext<AggregatedQueryKey, DataPointType> given)) {
                return delegate.onInitialize(context);
            }
            DefaultProcessingContext.Builder<AggregatedQueryKey, DataPointType> builder = DefaultProcessingContext.builder();
            given.properties().forEach((key, props) -> {
                for (var pv : props) {
                    builder.addPropertyValue(key, (Property<Object>) pv.getProperty(), pv.getValue());
                }
            });
            for (AggregatedQueryKey key : keys) {
                // Boolean tags have the Integer class, so their Count is already a number.
                boolean notNumeric = context.getContextValue(key, HistorianProperties.DATA_TYPE)
                        .filter(t -> t.getTypeClass() != DataTypeClass.Integer
                                && t.getTypeClass() != DataTypeClass.Float)
                        .isPresent();
                if (notNumeric && "Count".equals(key.aggregationType().name())) {
                    builder.addPropertyValue(key, HistorianProperties.DATA_TYPE, DataType.Float8);
                }
            }
            return delegate.onInitialize(builder.build());
        }

        @Override
        public boolean onPointAvailable(AggregatedQueryKey key, AggregatedPoint<?, ?> point) {
            return delegate.onPointAvailable(key, point);
        }

        @Override
        public void onKeyFailure(AggregatedQueryKey key, QualityCode quality) {
            delegate.onKeyFailure(key, quality);
        }

        @Override
        public void onError(QualityCode quality) {
            delegate.onError(quality);
        }

        @Override
        public void onError(Exception e) {
            delegate.onError(e);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }

    /**
     * The Java value of a stored value. For an array element ({@code element >= 0}) that element of
     * the stored list, or null when the list is shorter.
     */
    static Object storedValue(com.google.protobuf.Value value, int element) {
        if (element < 0) {
            return protoValueToJava(value);
        }
        return value.getKindCase() == com.google.protobuf.Value.KindCase.LIST_VALUE
                && element < value.getListValue().getValuesCount()
                ? protoValueToJava(value.getListValue().getValues(element)) : null;
    }

    static Object protoValueToJava(com.google.protobuf.Value value) {
        if (value == null) {
            return null;
        }
        switch (value.getKindCase()) {
            case BOOL_VALUE:
                return value.getBoolValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            default:
                return null;
        }
    }
}
