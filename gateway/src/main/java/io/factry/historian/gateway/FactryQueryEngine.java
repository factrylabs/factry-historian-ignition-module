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
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
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
    /** Limits on the reads around empty buckets of an analog tag; tests lower them. */
    int linearRawLimit = 10_000;
    int linearRunsPerPen = 10;

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

                    String factryFunction = "MinMax".equals(aggName) ? null : toFactryAggregateFunction(aggName);
                    if (TIME_WEIGHTED_AVERAGE.equals(factryFunction) && options.getTimeRange().isPresent()) {
                        pointCount += queryTimeWeightedAverage(uuid, source, key, period,
                                periodMs, dataType == DataType.Boolean, options, processor);
                    } else if ("count".equals(factryFunction) || TIME_WEIGHTED_AVERAGE.equals(factryFunction)) {
                        pointCount += querySingleAggregate(uuid, source, key, factryFunction, period, options,
                                processor);
                    } else {
                        // MinMax asks Factry for min and max, and gives each bucket both.
                        List<String> functions = factryFunction == null ? List.of("min", "max") : List.of(factryFunction);
                        pointCount += queryEveryBucket(uuid, source, key, functions, period, periodMs, options,
                                processor);
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
            boolean isBoolean, AggregatedQueryOptions options, AggregatedPointProcessor processor) {

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
            processor.onPointAvailable(key,
                    AggregatedDataPoint.<Object, AggregationType>builder(source, key.aggregationType())
                            // A Bad bucket has no value. Not NaN: the Power Chart streams history
                            // as strict JSON, which throws at a NaN and cuts the response off. A
                            // null reads as 0.0 in a Float column, but its quality stays Bad.
                            .value(r.isBad() ? null : r.value())
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
            AggregatedPointProcessor processor) {
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

        List<BucketFill.Bucket> buckets = BucketFill.fill(startMs, rangeEndMs, periodMs, nowMs, statuses,
                ts -> samplesBefore(uuid, ts), RAW_BUDGET.seedReadsPerPen(), functions.contains("last"));
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

    @Override
    protected Optional<PropertySet> getProcessorProperties(HistoricalNode node) {
        return super.getProcessorProperties(node);
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
