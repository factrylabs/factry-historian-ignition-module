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
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.StringPath;
import com.inductiveautomation.ignition.common.browsing.BrowseFilter;
import com.inductiveautomation.ignition.common.config.BasicProperty;
import com.inductiveautomation.ignition.common.config.BasicPropertySet;
import com.inductiveautomation.ignition.common.config.PropertySet;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import com.inductiveautomation.ignition.common.tags.config.properties.TagHistoryProps;
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
import java.util.concurrent.atomic.AtomicReference;
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

    private volatile FactryHistorianSettings settings;
    private final FactryGrpcClient grpcClient;
    private final MeasurementCache measurementCache;
    private final HistorianMetrics metrics;
    /** The current time; tests replace it. */
    LongSupplier now = System::currentTimeMillis;

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

            long periodMs = timeWindowToMillis(options.getTimeWindow());
            String period = toPeriod(periodMs);

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
                    DataType dataType = keyToNode.get(key).dataType().orElse(DataType.Float8);
                    if (dataType == DataType.String && !STRING_AGGREGATES.contains(aggName)) {
                        // Factry refuses every other aggregate of a string measurement.
                        logger.debug("Aggregate " + aggName + " is not supported for String tag: " + tagPath);
                        processor.onKeyFailure(key, QualityCode.Bad_Unsupported);
                        continue;
                    }

                    boolean linear = fillMode(key, keyToNode.get(key)) == FillMode.LINEAR;

                    String factryFunction = "MinMax".equals(aggName) ? null : toFactryAggregateFunction(aggName);
                    if (TIME_WEIGHTED_AVERAGE.equals(factryFunction) && options.getTimeRange().isPresent()) {
                        pointCount += queryTimeWeightedAverage(uuid, source, key, period,
                                periodMs, dataType == DataType.Boolean, linear, options, processor);
                    } else if ("count".equals(factryFunction) || TIME_WEIGHTED_AVERAGE.equals(factryFunction)) {
                        pointCount += querySingleAggregate(uuid, source, key, factryFunction, period, options,
                                processor);
                    } else {
                        // MinMax asks Factry for min and max, and gives each bucket both.
                        List<String> functions = factryFunction == null ? List.of("min", "max") : List.of(factryFunction);
                        pointCount += queryEveryBucket(uuid, source, key, functions, period, periodMs, options,
                                processor, linear);
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

        // Properties stored via storeMetadata that Factry has no engineering spec for round-trip
        // through the measurement's metadata map. Surface any that weren't already provided above:
        // the Config-derived engineeringSpecs wins for engUnit/engLow/engHigh. (The read-side
        // Measurement proto has no 'description'/'attributes' fields, so the metadata map is the
        // only channel that round-trips arbitrary storeMetadata values.)
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
            boolean isBoolean, boolean linear, AggregatedQueryOptions options, AggregatedPointProcessor processor) {

        long startMs = options.getTimeRange().get().startTime().toEpochMilli();
        long rangeEndMs = options.getTimeRange().get().endTime().toEpochMilli();
        Set<Long> withSamples = new HashSet<>();
        AtomicReference<List<BucketFill.Sample>> before = new AtomicReference<>();
        Map<Long, GoodTimeAverage.Result> values =
                timeWeightedAverage(uuid, period, periodMs, isBoolean, startMs, rangeEndMs, withSamples, before);

        List<Long> starts = new ArrayList<>();
        for (long b = startMs; b < rangeEndMs; b += periodMs) {
            starts.add(b);
        }
        Object[] linearValues = new Object[starts.size()];
        boolean[] empty = new boolean[starts.size()];
        for (int i = 0; i < starts.size(); i++) {
            GoodTimeAverage.Result r = values.getOrDefault(starts.get(i), GoodTimeAverage.Result.BAD);
            linearValues[i] = r.isBad() ? null : r.value();
            empty[i] = !r.isBad() && !withSamples.contains(starts.get(i));
        }
        if (linear) {
            // Factry's twa holds the last value through buckets without samples; an analog tag
            // gets the straight line between its samples instead. The average of a line over a
            // bucket is its value at the middle of the part averaged: up to the range end or now.
            long endMs = Math.min(rangeEndMs, now.getAsLong());
            long[] at = new long[starts.size()];
            for (int i = 0; i < at.length; i++) {
                long b = starts.get(i);
                at[i] = b + (Math.min(b + periodMs, endMs) - b) / 2;
            }
            fillLinear(uuid, at, empty, startMs, rangeEndMs, periodMs, before.get(), new Object[][]{linearValues});
        }

        // Every bucket gets a point. Ignition fills a bucket without one with a neighbouring
        // value marked Good (checked through its own query path), so a bucket left out would
        // read as a held Good value: in an outage, before the first sample and after now. A
        // bucket without Good time is a Bad point instead, which the Power Chart draws as a gap.
        int count = 0;
        for (int i = 0; i < starts.size(); i++) {
            long b = starts.get(i);
            GoodTimeAverage.Result r = values.getOrDefault(b, GoodTimeAverage.Result.BAD);
            processor.onPointAvailable(key,
                    AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                            // A Bad bucket has no value. Not NaN: the Power Chart streams history
                            // as strict JSON, which throws at a NaN and cuts the response off. A
                            // null reads as 0.0 in a Float column, but its quality stays Bad.
                            .value(r.isBad() ? null : linearValues[i])
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
            String uuid, String period, long periodMs, boolean isBoolean, long startMs, long rangeEndMs,
            Set<Long> withSamples, AtomicReference<List<BucketFill.Sample>> before) {
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
        withSamples.addAll(statuses.keySet());

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
        // Read once: the line through the first empty buckets starts here too.
        before.set(seed == null ? List.of() : List.of(new BucketFill.Sample(seed.ts(), seed.value(), seed.good())));

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
        return QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid)
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
     * @param functions the Factry functions; each bucket gets one point per function, in order
     */
    private int queryEveryBucket(
            String uuid, QualifiedPath source, AggregatedQueryKey key, List<String> functions,
            String period, long periodMs, AggregatedQueryOptions options,
            AggregatedPointProcessor processor, boolean linear) {
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
                            .add(isGood, protoValueToJava(pt.getValue()));
                }
            }
            for (String function : functions) {
                // LastValue takes its value from the buckets below.
                good.add("last".equals(function) ? Map.of()
                        : goodAggregate(function, grpcClient.queryTimeseries(withRange(
                                perStatusRequest(uuid, function, period), startMs, endMs).build())));
            }
        }

        // The samples before the range, kept for the line through the first empty buckets.
        List<List<BucketFill.Sample>> beforeRange = new ArrayList<>();
        List<BucketFill.Bucket> buckets = BucketFill.fill(startMs, rangeEndMs, periodMs, nowMs, statuses, ts -> {
            List<BucketFill.Sample> before = samplesBefore(uuid, ts);
            if (ts == startMs) {
                beforeRange.add(before);
            }
            return before;
        }, RAW_BUDGET.seedReadsPerPen(), functions.contains("last"));
        Object[][] values = new Object[functions.size()][buckets.size()];
        for (int f = 0; f < functions.size(); f++) {
            String function = functions.get(f);
            for (int i = 0; i < buckets.size(); i++) {
                BucketFill.Bucket bucket = buckets.get(i);
                values[f][i] = switch (bucket.kind()) {
                    case GOOD_SAMPLES -> "last".equals(function) ? bucket.value() : good.get(f).get(bucket.start());
                    case HELD -> heldValue(function, bucket.value());
                    case BAD -> null;
                };
            }
        }
        if (linear) {
            // One value for every function: the line's at the bucket end (or now), stamped at the
            // bucket start, as Ignition's own historians give an empty analog block (its
            // LastValue and MinMax see the line at the block start and end, and keep the end).
            // The value at the start would kink the line at every sample.
            long blockEndMs = Math.min(rangeEndMs, nowMs);
            long[] at = new long[buckets.size()];
            boolean[] empty = new boolean[buckets.size()];
            for (int i = 0; i < buckets.size(); i++) {
                at[i] = Math.min(buckets.get(i).start() + periodMs, blockEndMs);
                empty[i] = buckets.get(i).kind() == BucketFill.Kind.HELD;
            }
            List<Object[]> interpolated = new ArrayList<>();
            for (int f = 0; f < functions.size(); f++) {
                if (INTERPOLATED.contains(functions.get(f))) {
                    interpolated.add(values[f]);
                }
            }
            fillLinear(uuid, at, empty, startMs, rangeEndMs, periodMs,
                    beforeRange.isEmpty() ? null : beforeRange.get(0), interpolated.toArray(new Object[0][]));
        }
        int count = 0;
        for (int i = 0; i < buckets.size(); i++) {
            BucketFill.Bucket bucket = buckets.get(i);
            for (int f = 0; f < functions.size(); f++) {
                String function = functions.get(f);
                Object value = values[f][i];
                // No value is a Bad point. Not NaN: the Power Chart streams history as strict
                // JSON, which throws at a NaN (a variance of one sample can be one). A null
                // reads as 0.0 in a Float column, but its quality stays Bad.
                if (value instanceof Double d && !Double.isFinite(d)) {
                    value = null;
                }
                processor.onPointAvailable(key,
                        AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                                .value(value)
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
     * The functions whose empty buckets an analog tag fills on the straight line between its
     * neighbours. A sum, spread, variance or standard deviation of an empty bucket stays as it is
     * (see {@link #heldValue}), as Ignition's own historians never interpolate those.
     */
    private static final Set<String> INTERPOLATED = Set.of("min", "max", "mean", "last");

    /**
     * Up to this many runs of empty buckets, each gets two one-sample reads; with more, the span
     * from the first run to the last is read raw once. Tests change it.
     */
    int linearEdgeReadRuns = 4;

    /**
     * Most samples per status in that one raw read; a span holding more (a dense tag with a few
     * gaps) falls back to the one-sample reads, capped by {@link #linearRunsPerPen}. Tests lower it.
     */
    int linearRawLimit = 10_000;

    /** Most runs read with one-sample reads per analog pen; the rest hold. Tests lower it. */
    int linearRunsPerPen = 10;

    /** Whether a failed read around empty buckets was logged; it only costs the line. */
    private volatile boolean linearReadFailureLogged;

    /**
     * Gives each empty bucket of an analog tag one value, for every function alike (a minimum,
     * maximum, mean or last value of an empty bucket are all the same), on the straight line
     * from the last sample before its run of empty buckets to the first sample after it, each at
     * its own time, as Ignition's own historians interpolate an analog tag.
     *
     * <p>Those samples: with up to {@link #linearEdgeReadRuns} runs, two small reads per run, the
     * last sample before it (descending, limit 1 per status) and the first after it (ascending,
     * limit 1 per status). With more (a tag stored once a minute in a chart of 300 points has a
     * run between every two samples), one raw read from the bucket before the first run to the
     * bucket after the last, limit {@link #linearRawLimit} per status; when it reaches the limit,
     * the small reads again, for at most {@link #linearRunsPerPen} runs. The first run reuses the
     * sample before the range when already read, and after a past range the first sample after
     * it is read up to now. The line needs the latest sample on each side to be Good (else Bad
     * time), and a sample after the run: toward now nothing is extrapolated. A run without that,
     * past the cap, or whose read fails keeps the value it had (held): the line is never worth
     * failing a pen.
     *
     * @param at      per bucket, the time whose value on the line it gets
     * @param empty   per bucket, whether it has no samples and holds a Good value
     * @param before  the samples before the range when already read, else null
     * @param columns per function to fill, its value per bucket; replaced in place
     */
    private void fillLinear(String uuid, long[] at, boolean[] empty, long startMs,
                            long rangeEndMs, long periodMs, List<BucketFill.Sample> before, Object[][] columns) {
        if (columns.length == 0) {
            return;
        }
        long nowMs = now.getAsLong();
        long endMs = Math.min(rangeEndMs, nowMs);
        List<int[]> runs = new ArrayList<>();
        for (int i = 0; i < empty.length; i++) {
            if (empty[i]) {
                int j = i;
                while (j + 1 < empty.length && empty[j + 1]) {
                    j++;
                }
                runs.add(new int[]{i, j});
                i = j;
            }
        }
        if (runs.isEmpty()) {
            return;
        }
        // The samples on each side of each run, as [before, after]; null where not read.
        List<BucketFill.Sample[]> sides;
        try {
            sides = runs.size() > linearEdgeReadRuns
                    ? spanSides(uuid, runs, empty.length, startMs, rangeEndMs, periodMs, before)
                    : null;
            if (sides == null) {
                sides = new ArrayList<>();
                for (int r = 0; r < runs.size(); r++) {
                    sides.add(r < linearRunsPerPen
                            ? edgeSides(uuid, runs.get(r), empty.length, startMs, rangeEndMs, periodMs, before)
                            : null);
                }
            }
        } catch (RuntimeException e) {
            if (!linearReadFailureLogged) {
                linearReadFailureLogged = true;
                logger.debug("Could not read the samples around empty buckets of " + uuid
                        + "; they hold their value instead (logged once)", e);
            }
            return;
        }
        for (int r = 0; r < runs.size(); r++) {
            BucketFill.Sample[] side = sides.get(r);
            if (side == null) {
                continue;
            }
            for (int k = runs.get(r)[0]; k <= runs.get(r)[1]; k++) {
                Double v = lineAt(at[k], side[0], side[1]);
                if (v != null) {
                    for (Object[] column : columns) {
                        column[k] = v;
                    }
                }
            }
        }
    }

    /** Where the samples after a run are looked for: up to this time (the run's end when none). */
    private static long afterEnd(int last, int buckets, long runEnd, long periodMs, long endMs, long rangeEndMs,
                                 long nowMs) {
        return last + 1 < buckets ? Math.min(runEnd + periodMs, endMs) : rangeEndMs < nowMs ? nowMs : runEnd;
    }

    /** The samples on each side of one run, from two one-sample reads. */
    private BucketFill.Sample[] edgeSides(String uuid, int[] run, int buckets, long startMs,
                                          long rangeEndMs, long periodMs, List<BucketFill.Sample> before) {
        long nowMs = now.getAsLong();
        long endMs = Math.min(rangeEndMs, nowMs);
        long runStart = startMs + run[0] * periodMs;
        long runEnd = Math.min(startMs + (run[1] + 1) * periodMs, endMs);
        long afterEnd = afterEnd(run[1], buckets, runEnd, periodMs, endMs, rangeEndMs, nowMs);
        // The bucket before a run has samples; before the range, the last one may be old.
        BucketFill.Sample a = latest(run[0] > 0 ? edgeSamples(uuid, runStart - periodMs, runStart, true)
                : before != null ? before : samplesBefore(uuid, runStart));
        BucketFill.Sample b = afterEnd > runEnd ? earliest(edgeSamples(uuid, runEnd, afterEnd, false)) : null;
        return new BucketFill.Sample[]{a, b};
    }

    /**
     * The samples on each side of every run, from one raw read of the span they cover; null when
     * the span holds more than {@link #linearRawLimit} samples of one status.
     */
    private List<BucketFill.Sample[]> spanSides(String uuid, List<int[]> runs, int buckets,
                                                long startMs, long rangeEndMs, long periodMs,
                                                List<BucketFill.Sample> before) {
        long nowMs = now.getAsLong();
        long endMs = Math.min(rangeEndMs, nowMs);
        int[] firstRun = runs.get(0);
        int[] lastRun = runs.get(runs.size() - 1);
        long from = startMs + Math.max(0, firstRun[0] - 1) * periodMs;
        long lastEnd = Math.min(startMs + (lastRun[1] + 1) * periodMs, endMs);
        long to = Math.min(lastEnd + periodMs, endMs);
        List<BucketFill.Sample> samples = new ArrayList<>();
        if (to > from) {
            QueryTimeseriesResponse reply = grpcClient.queryTimeseries(withRange(QueryTimeseriesRequest.newBuilder()
                    .addMeasurementUUIDs(uuid).addGroupBy("status").setLimit(linearRawLimit), from, to).build());
            for (Series series : reply.getSeriesList()) {
                if (series.getDataPointsCount() >= linearRawLimit) {
                    return null;
                }
                boolean isGood = isGoodStatus(seriesStatus(series));
                for (SeriesPoint pt : series.getDataPointsList()) {
                    samples.add(new BucketFill.Sample(pt.getTimestamp(), protoValueToJava(pt.getValue()), isGood));
                }
            }
        }
        if (firstRun[0] == 0) {
            samples.addAll(before != null ? before : samplesBefore(uuid, startMs));
        }
        if (lastRun[1] == buckets - 1 && rangeEndMs < nowMs) {
            samples.addAll(edgeSamples(uuid, rangeEndMs, nowMs, false));
        }
        samples.sort(Comparator.comparingLong(BucketFill.Sample::ts));
        List<BucketFill.Sample[]> out = new ArrayList<>();
        int next = 0;
        for (int[] run : runs) {
            long runStart = startMs + run[0] * periodMs;
            long runEnd = Math.min(startMs + (run[1] + 1) * periodMs, endMs);
            while (next < samples.size() && samples.get(next).ts() < runStart) {
                next++;
            }
            BucketFill.Sample a = next > 0 ? samples.get(next - 1) : null;
            // The run has no samples, so the first at or after its start is the first after it,
            // unless toward now (nothing after a range that ends at or after now).
            BucketFill.Sample b = next < samples.size()
                    && afterEnd(run[1], buckets, runEnd, periodMs, endMs, rangeEndMs, nowMs) > runEnd
                    ? samples.get(next) : null;
            out.add(new BucketFill.Sample[]{a, b});
        }
        return out;
    }

    /** The last ({@code desc}) or first sample of each status in [from, to). */
    private List<BucketFill.Sample> edgeSamples(String uuid, long fromMs, long toMs, boolean desc) {
        List<BucketFill.Sample> out = new ArrayList<>();
        for (Series series : grpcClient.queryTimeseries(withRange(QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(uuid).addGroupBy("status").setDesc(desc).setLimit(1), fromMs, toMs).build())
                .getSeriesList()) {
            boolean isGood = isGoodStatus(seriesStatus(series));
            for (SeriesPoint pt : series.getDataPointsList()) {
                out.add(new BucketFill.Sample(pt.getTimestamp(), protoValueToJava(pt.getValue()), isGood));
            }
        }
        return out;
    }

    private static BucketFill.Sample latest(List<BucketFill.Sample> samples) {
        BucketFill.Sample out = null;
        for (BucketFill.Sample s : samples) {
            if (out == null || s.ts() > out.ts()) {
                out = s;
            }
        }
        return out;
    }

    private static BucketFill.Sample earliest(List<BucketFill.Sample> samples) {
        BucketFill.Sample out = null;
        for (BucketFill.Sample s : samples) {
            if (out == null || s.ts() < out.ts()) {
                out = s;
            }
        }
        return out;
    }

    /**
     * The value at {@code ts} on the line from {@code a} to {@code b}; null when either is
     * missing, not Good or not a number.
     */
    static Double lineAt(long ts, BucketFill.Sample a, BucketFill.Sample b) {
        if (a == null || b == null || !a.good() || !b.good() || !(a.value() instanceof Number va)
                || !(b.value() instanceof Number vb) || b.ts() <= a.ts()) {
            return null;
        }
        double from = va.doubleValue();
        return from + (vb.doubleValue() - from) * (ts - a.ts()) / (b.ts() - a.ts());
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

    /** The last sample of each status before {@code ts}. */
    private List<BucketFill.Sample> samplesBefore(String uuid, long ts) {
        List<BucketFill.Sample> out = new ArrayList<>();
        for (Series series : grpcClient.queryTimeseries(sampleBeforeRequest(uuid, ts)).getSeriesList()) {
            boolean isGood = isGoodStatus(seriesStatus(series));
            for (SeriesPoint pt : series.getDataPointsList()) {
                out.add(new BucketFill.Sample(pt.getTimestamp(), protoValueToJava(pt.getValue()), isGood));
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
        Measurement m = measurementCache.getMeasurementByName(tagPath);
        if (m == null) {
            measurementCache.refresh(grpcClient);
            m = measurementCache.getMeasurementByName(tagPath);
        }
        if (m != null) {
            Instant createdTime = m.hasCreatedAt()
                    ? Instant.ofEpochSecond(m.getCreatedAt().getSeconds(), m.getCreatedAt().getNanos())
                    : Instant.EPOCH;
            return Optional.of(new FactryRecord(m.getUuid(), path, m.getDatatype(),
                    ignitionDataType(tagPath, m), deadbandStyle(tagPath, m), createdTime));
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

    /** The tag's Ignition data type: from the metadata this gateway has, else the measurement's setting. */
    private String ignitionDataType(String tagPath, Measurement m) {
        return measurementCache.tagProperty(tagPath, m, grpcClient.getCollectorUUID(),
                MeasurementCache.IGNITION_DATA_TYPE, MeasurementCache.IGNITION_DATA_TYPE_SETTING);
    }

    /**
     * The tag's Deadband Style: from the metadata, which is current on the gateway that
     * historizes the tag, else the measurement's setting, for other gateways and before the
     * metadata arrives.
     */
    private String deadbandStyle(String tagPath, Measurement m) {
        return measurementCache.tagProperty(tagPath, m, grpcClient.getCollectorUUID(),
                MeasurementCache.DEADBAND_STYLE, MeasurementCache.DEADBAND_STYLE_SETTING);
    }

    /**
     * Tells Ignition each tag's Deadband Style, as Core's historian does, so the aggregates
     * Ignition computes itself from raw points hold a discrete tag instead of interpolating it.
     */
    @Override
    protected Optional<PropertySet> getProcessorProperties(HistoricalNode node) {
        Optional<PropertySet> properties = super.getProcessorProperties(node);
        if (node instanceof FactryRecord r && r.deadbandStyle().isPresent()) {
            PropertySet set = properties.orElseGet(BasicPropertySet::new);
            set.set(TagHistoryProps.HistoricalDeadbandStyle, r.deadbandStyle().get());
            return Optional.of(set);
        }
        return properties;
    }

    /**
     * How a pen fills buckets without samples: as the query asks, else (FillMode.DERIVED, which
     * the charts and queryTagHistory send) by the tag, as Ignition's own historians do. Only
     * LINEAR changes what the module returns; every other mode holds as before.
     */
    static FillMode fillMode(AggregatedQueryKey key, HistoricalNode node) {
        FillMode asked = key.fillMode();
        if (node instanceof FactryRecord r) {
            DataType type = r.dataType().orElse(null);
            if (type == DataType.String || type == DataType.Boolean || type == DataType.DateTime) {
                return FillMode.PREV;
            }
            if (asked == null || asked == FillMode.DERIVED) {
                return r.derivedFill();
            }
        }
        return asked == null || asked == FillMode.DERIVED ? FillMode.PREV : asked;
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

    /**
     * Aggregates of a String tag: Factry 8.2.3 refuses every other aggregate of a string
     * measurement (checked live), so those read Bad_Unsupported, like a Sum of dates.
     */
    private static final Set<String> STRING_AGGREGATES = Set.of("LastValue", "Count");

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
