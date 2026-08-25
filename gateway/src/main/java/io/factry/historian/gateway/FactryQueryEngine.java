package io.factry.historian.gateway;

import com.inductiveautomation.historian.common.model.TimeRange;
import com.inductiveautomation.historian.common.model.AggregationType;
import com.inductiveautomation.historian.common.model.LegacyAggregateAdapter;
import com.inductiveautomation.historian.common.model.TimeWindow;
import com.inductiveautomation.historian.common.model.data.AggregatedDataPoint;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.DataPointFactory;
import com.inductiveautomation.historian.common.model.data.DataPointType;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryKey;
import com.inductiveautomation.historian.common.model.options.AggregatedQueryOptions;
import com.inductiveautomation.historian.common.model.options.MetadataQueryKey;
import com.inductiveautomation.historian.common.model.options.MetadataQueryOptions;
import com.inductiveautomation.historian.common.model.options.RawQueryKey;
import com.inductiveautomation.historian.common.model.options.RawQueryOptions;
import com.inductiveautomation.historian.gateway.api.query.AbstractQueryEngine;
import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.historian.gateway.api.query.browsing.BrowsePublisher;
import com.inductiveautomation.historian.gateway.api.query.processor.AggregatedPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.DefaultProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.MetadataPointProcessor;
import com.inductiveautomation.historian.gateway.api.query.processor.ProcessingContext;
import com.inductiveautomation.historian.gateway.api.query.processor.RawPointProcessor;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.StringPath;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.config.BasicProperty;
import com.inductiveautomation.ignition.common.config.BasicPropertySet;
import com.inductiveautomation.ignition.common.config.PropertySet;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
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

    private volatile FactryHistorianSettings settings;
    private final FactryGrpcClient grpcClient;
    private final MeasurementCache measurementCache;
    private final HistorianMetrics metrics;

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
            measurementCache.refresh(grpcClient);

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
        for (Measurement m : measurementCache.getAllMeasurements()) {
            // Prefix the display path with the collector name so measurements
            // are grouped under their collector in the browse tree.
            String collectorName = measurementCache.getCollectorName(m.getUuid());
            String browseName = TagPathUtil.toBrowseName(m.getName(), delimiter);
            String displayPath = collectorName != null
                    ? collectorName + "/" + browseName
                    : browseName;
            displayToBrowse.put(displayPath, browseName);
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
                    String storedPath = m != null ? m.getName() : prop.getMeasurementUUID();
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
            // mapKeysToNodes() populates the context from queryForHistoricalNodes() and
            // calls processor.onInitialize() for us.
            Map<RawQueryKey, ? extends HistoricalNode> keyToNode =
                    mapKeysToNodes(options, processor, RawQueryKey::source);

            // mapKeysToNodes only initializes the processor when it resolves at least one
            // node. If NOTHING resolved (every tag is unknown), the processor is not
            // initialized, so calling onKeyFailure/onComplete on it would throw
            // "Processor not initialized". Nothing to return in that case — report zero rows.
            if (keyToNode.isEmpty()) {
                logger.debug("Raw query resolved no measurements; returning 0 rows");
                return Optional.of(0);
            }

            // Map query keys to measurement UUIDs
            List<String> measurementUUIDs = new ArrayList<>();
            Map<String, RawQueryKey> uuidToKeyMap = new HashMap<>();

            for (RawQueryKey key : options.getQueryKeys()) {
                QualifiedPath source = key.source();
                String tagPath = toStoredTagPath(source);
                // Only query keys that resolved to a node; mapKeysToNodes omits the rest.
                String uuid = keyToNode.containsKey(key) ? lookupUUID(tagPath) : null;

                if (uuid != null) {
                    measurementUUIDs.add(uuid);
                    uuidToKeyMap.put(uuid, key);
                    if (settings.isDebugLogging()) {
                        logger.debug("Mapped " + source + " -> " + tagPath + " -> " + uuid);
                    }
                } else {
                    // Node/measurement not found — fail only this key so the framework
                    // emits nulls for its column instead of aborting the whole query.
                    logger.warn("No measurement UUID found for tag path: " + tagPath);
                    processor.onKeyFailure(key, QualityCode.Bad_NotFound);
                }
            }

            if (measurementUUIDs.isEmpty()) {
                processor.onComplete();
                return Optional.of(0);
            }

            // Build gRPC request using QueryTimeseries (no aggregation = raw). Group by the
            // per-point status tag so Factry returns one series per quality status; without this
            // every point comes back in a single series and reads back as Good, losing bad/
            // uncertain quality.
            QueryTimeseriesRequest.Builder reqBuilder = QueryTimeseriesRequest.newBuilder()
                    .addAllMeasurementUUIDs(measurementUUIDs)
                    .addGroupBy("status");

            options.getTimeRange().ifPresent(tr -> {
                reqBuilder.setStart(Timestamps.fromMillis(tr.startTime().toEpochMilli()));
                reqBuilder.setEnd(Timestamps.fromMillis(tr.endTime().toEpochMilli()));
            });

            QueryTimeseriesResponse reply = grpcClient.queryTimeseries(reqBuilder.build());

            // A measurement now spans multiple status-series. Collect all of a measurement's points
            // (each carrying its series' quality), then merge-sort by timestamp before emitting —
            // Ignition expects time-ordered points per key.
            record RawPt(long ts, Object value, QualityCode quality) {}
            Map<String, List<RawPt>> pointsByUuid = new LinkedHashMap<>();
            for (Series series : reply.getSeriesList()) {
                String uuid = series.hasMeasurementUUID() ? series.getMeasurementUUID() : "";
                if (!uuidToKeyMap.containsKey(uuid)) {
                    continue;
                }
                QualityCode quality = statusToQuality(seriesStatus(series));
                List<RawPt> pts = pointsByUuid.computeIfAbsent(uuid, k -> new ArrayList<>());
                for (SeriesPoint pt : series.getDataPointsList()) {
                    pts.add(new RawPt(pt.getTimestamp(), protoValueToJava(pt.getValue()), quality));
                }
            }

            int totalPoints = 0;
            for (Map.Entry<String, List<RawPt>> entry : pointsByUuid.entrySet()) {
                RawQueryKey key = uuidToKeyMap.get(entry.getKey());
                QualifiedPath path = key.source();
                List<RawPt> pts = entry.getValue();
                pts.sort(Comparator.comparingLong(RawPt::ts));

                for (RawPt p : pts) {
                    AtomicPoint<?> atomicPoint = DataPointFactory.createAtomicPoint(
                            p.value(), p.quality(), Instant.ofEpochMilli(p.ts()), path);

                    if (!processor.onPointAvailable(key, atomicPoint)) {
                        processor.onComplete();
                        return Optional.of(totalPoints);
                    }
                    totalPoints++;
                }
            }

            processor.onComplete();
            metrics.recordRawQuery(totalPoints, System.currentTimeMillis() - startMs);
            logger.debug("Query completed with " + totalPoints + " total points");
            return Optional.of(totalPoints);

        } catch (Exception e) {
            logger.error("Error querying raw data", e);
            processor.onError(e);
            return Optional.empty();
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
            // populates the context from queryForHistoricalNodes() and calls onInitialize() for us.
            Map<AggregatedQueryKey, ? extends HistoricalNode> keyToNode =
                    mapKeysToNodes(options, processor, AggregatedQueryKey::source);

            // mapKeysToNodes only initializes the processor when it resolves at least one node.
            // If NOTHING resolved, the processor is not initialized, so calling
            // onKeyFailure/onComplete on it would throw "Processor not initialized".
            if (keyToNode.isEmpty()) {
                logger.debug("Aggregated query resolved no measurements; returning 0 rows");
                return Optional.of(0);
            }

            TimeWindow timeWindow = options.getTimeWindow();
            String period = timeWindowToPeriod(timeWindow);

            var queryKeys = options.getQueryKeys();
            int pointCount = 0;

            for (AggregatedQueryKey key : queryKeys) {
                QualifiedPath source = key.source();
                String tagPath = toStoredTagPath(source);
                // Only query keys that resolved to a node; mapKeysToNodes omits the rest.
                String uuid = keyToNode.containsKey(key) ? lookupUUID(tagPath) : null;

                if (uuid == null) {
                    logger.warn("No measurement UUID found for aggregated query: " + tagPath);
                    processor.onKeyFailure(key, QualityCode.Bad_NotFound);
                    continue;
                }

                try {
                    String aggName = key.aggregationType().name();

                    if ("MinMax".equals(aggName)) {
                        // MinMax: query min and max separately, emit pairs
                        pointCount += queryMinMax(uuid, source, key, period, options, processor);
                    } else {
                        String factryFunction = toFactryAggregateFunction(aggName);
                        pointCount += querySingleAggregate(uuid, source, key, factryFunction, period, options, processor);
                    }
                } catch (Exception e) {
                    logger.error("Error querying aggregated data for: " + tagPath, e);
                    processor.onKeyFailure(key, QualityCode.Bad);
                }
            }

            processor.onComplete();
            metrics.recordAggregatedQuery(pointCount, System.currentTimeMillis() - startMs);
            logger.debug("Aggregated query completed with " + pointCount + " points");
            return Optional.of(pointCount);

        } catch (Exception e) {
            logger.error("Error in aggregated query", e);
            processor.onError(e);
            return Optional.empty();
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
                Measurement m = measurementCache.getMeasurementByName(tagPath);

                if (m == null) {
                    measurementCache.refresh(grpcClient);
                    m = measurementCache.getMeasurementByName(tagPath);
                }

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

        // Custom metadata properties stored via storeMetadata (engUnit, engLow, engHigh,
        // and any others) round-trip through the measurement's metadata map. Surface any
        // that weren't already provided above. (The read-side Measurement proto has no
        // 'description'/'attributes' fields, so the metadata map is the only channel that
        // round-trips storeMetadata values.)
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
            AggregatedQueryOptions options, AggregatedPointProcessor processor) {

        Aggregation aggregation = Aggregation.newBuilder()
                .setName(factryFunction)
                .setPeriod(period)
                .setFillType("none")
                .build();

        // Group by status so Factry aggregates each quality separately, then keep only the Good
        // series. Aggregating across all statuses blends bad samples into the value (e.g. a bad
        // x1000 reading drags the average up); Good-only is faithful and avoids the corruption.
        QueryTimeseriesRequest.Builder reqBuilder = QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
                .addGroupBy("status")
                .setAggregation(aggregation);

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

                AggregatedDataPoint<Object, AggregationType> point =
                        AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                                .value(value)
                                .quality(QualityCode.Good)
                                .timestamp(timestamp)
                                .build();

                processor.onPointAvailable(key, point);
                count++;
            }
        }
        return count;
    }

    private int queryMinMax(
            String uuid, QualifiedPath source, AggregatedQueryKey key,
            String period, AggregatedQueryOptions options,
            AggregatedPointProcessor processor) {

        // Query min and max separately, grouped by status and keeping only the Good series so
        // bad/uncertain samples don't skew the min/max (see querySingleAggregate).
        QueryTimeseriesRequest.Builder minReqBuilder = QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
                .addGroupBy("status")
                .setAggregation(Aggregation.newBuilder().setName("min").setPeriod(period).setFillType("none").build());
        QueryTimeseriesRequest.Builder maxReqBuilder = QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
                .addGroupBy("status")
                .setAggregation(Aggregation.newBuilder().setName("max").setPeriod(period).setFillType("none").build());

        options.getTimeRange().ifPresent(tr -> {
            long startMs = tr.startTime().toEpochMilli();
            long endMs = tr.endTime().toEpochMilli();
            minReqBuilder.setStart(Timestamps.fromMillis(startMs)).setEnd(Timestamps.fromMillis(endMs));
            maxReqBuilder.setStart(Timestamps.fromMillis(startMs)).setEnd(Timestamps.fromMillis(endMs));
        });

        QueryTimeseriesResponse minReply = grpcClient.queryTimeseries(minReqBuilder.build());
        QueryTimeseriesResponse maxReply = grpcClient.queryTimeseries(maxReqBuilder.build());

        // Collect min values by timestamp (Good series only)
        Map<Long, Object> minValues = new HashMap<>();
        for (Series series : minReply.getSeriesList()) {
            if (!isGoodStatus(seriesStatus(series))) {
                continue;
            }
            for (SeriesPoint pt : series.getDataPointsList()) {
                minValues.put(pt.getTimestamp(), protoValueToJava(pt.getValue()));
            }
        }

        // Emit min/max pairs for each bucket (Good series only)
        int count = 0;
        for (Series series : maxReply.getSeriesList()) {
            if (!isGoodStatus(seriesStatus(series))) {
                continue;
            }
            for (SeriesPoint pt : series.getDataPointsList()) {
                long ts = pt.getTimestamp();
                Instant timestamp = Instant.ofEpochMilli(ts);
                Object minVal = minValues.get(ts);
                Object maxVal = protoValueToJava(pt.getValue());

                // Emit min point
                if (minVal != null) {
                    processor.onPointAvailable(key,
                            AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                                    .value(minVal).quality(QualityCode.Good).timestamp(timestamp).build());
                    count++;
                }

                // Emit max point
                if (maxVal != null) {
                    processor.onPointAvailable(key,
                            AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                                    .value(maxVal).quality(QualityCode.Good).timestamp(timestamp).build());
                    count++;
                }
            }
        }
        return count;
    }

    static String toFactryAggregateFunction(String name) {
        switch (name) {
            case "Average":
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
     * Convert a TimeWindow to a Go-style duration string (e.g. "2h30m", "45s").
     * Factry uses Go's time.ParseDuration() which does not accept ISO 8601.
     */
    static String timeWindowToPeriod(TimeWindow tw) {
        long totalSeconds = tw.toSeconds();
        if (totalSeconds <= 0) {
            return "1m";
        }

        StringBuilder sb = new StringBuilder();
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        if (hours > 0) sb.append(hours).append("h");
        if (minutes > 0) sb.append(minutes).append("m");
        if (seconds > 0) sb.append(seconds).append("s");

        return sb.length() > 0 ? sb.toString() : "1m";
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
        Measurement m = measurementCache.getMeasurementByName(tagPath);
        if (m == null) {
            measurementCache.refresh(grpcClient);
            m = measurementCache.getMeasurementByName(tagPath);
        }
        if (m != null) {
            Instant createdTime = m.hasCreatedAt()
                    ? Instant.ofEpochSecond(m.getCreatedAt().getSeconds(), m.getCreatedAt().getNanos())
                    : Instant.EPOCH;
            return Optional.of(new FactryRecord(m.getUuid(), path, m.getDatatype(), createdTime));
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

    private String lookupUUID(String tagPath) {
        // Try measurement
        String uuid = measurementCache.getUUID(tagPath);
        if (uuid != null) return uuid;

        // Try asset
        uuid = measurementCache.getAssetUUID(tagPath);
        if (uuid != null) return uuid;

        // Refresh and retry
        measurementCache.refresh(grpcClient);
        uuid = measurementCache.getUUID(tagPath);
        if (uuid != null) return uuid;
        uuid = measurementCache.getAssetUUID(tagPath);
        return uuid;
    }

    void updateSettings(FactryHistorianSettings newSettings) {
        this.settings = newSettings;
    }

    private String toStoredTagPath(QualifiedPath path) {
        String pathStr = path.toString();
        String stored = TagPathUtil.queryPathToStoredPath(pathStr);
        if (TagPathUtil.isAssetQueryPath(pathStr)) {
            // Asset property tags carry the measurement's real name (real '/');
            // never apply the tag-path delimiter reverse to them.
            return TagPathUtil.restoreFractionSlash(stored);
        }
        return TagPathUtil.fromBrowseName(stored, settings.getDelimiter());
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
