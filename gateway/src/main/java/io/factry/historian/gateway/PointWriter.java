package io.factry.historian.gateway;

import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.model.values.QualityCode;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.Point;
import io.factry.historian.proto.Points;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.Series;
import io.factry.historian.proto.SeriesPoint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns a flush of Ignition points into one CreatePoints call: resolves their measurements
 * through the {@link MeasurementCache}, merges array elements into whole arrays, sends them and
 * says what store and forward should do with the flush. Used by {@link FactryStorageEngine};
 * kept apart from it, without Ignition gateway classes, so it can be unit tested.
 */
class PointWriter {
    private static final Logger logger = LoggerFactory.getLogger(PointWriter.class);

    private final FactryGrpcClient grpcClient;
    private final MeasurementCache measurementCache;
    private final HistorianMetrics metrics;
    private final RefusalLog refusalLog = new RefusalLog();

    PointWriter(FactryGrpcClient grpcClient, MeasurementCache measurementCache, HistorianMetrics metrics) {
        this.grpcClient = grpcClient;
        this.measurementCache = measurementCache;
        this.metrics = metrics;
    }

    /**
     * Write a flush of points. Connection errors (UNAVAILABLE, DEADLINE_EXCEEDED) and server
     * rejections that remain after the retry are thrown, for the engine to report.
     */
    StorageResult<AtomicPoint<?>> write(List<AtomicPoint<?>> points) {
        return sendPoints(points, true);
    }

    /**
     * Forget which refusals were logged and which create attempts were made, for new
     * settings (the collector may have changed): they are judged and logged afresh.
     */
    void clearRefusals() {
        refusalLog.clear();
        measurementCache.clearWriteRefusals();
    }

    RefusalLog refusalLog() {
        return refusalLog;
    }

    /**
     * The flush is done: everything was sent, except points that were skipped (null value,
     * unsupported type), discarded because Factry has no measurement the module may write
     * them to, or dropped because the measurement is Paused (see
     * {@link MeasurementCache.WriteRefusal}). Those count as handled, so store and forward
     * neither retries nor quarantines them.
     */
    private StorageResult<AtomicPoint<?>> stored(List<AtomicPoint<?>> points, BuildResult built) {
        metrics.recordDiscarded(built.discardedPoints);
        metrics.recordPausedDropped(built.pausedPoints);
        return StorageResult.success(points);
    }

    /**
     * Build and send points to Factry. If the call fails with a server rejection
     * (e.g. deleted measurement) and {@code retryOnRejection} is true, evict the
     * affected measurement UUIDs from the cache and retry once. The retry resolves
     * them again through {@link MeasurementCache#batchGetOrCreateUUIDs} (array
     * measurements through {@link MeasurementCache#getOrCreateUUID}), which looks
     * each measurement up in Factry first and only re-creates it if Factry no
     * longer has it. If Factry does not answer one of those lookups in time, the
     * rest is left unresolved, so the retry costs one write deadline, not one per
     * evicted measurement.
     */
    private StorageResult<AtomicPoint<?>> sendPoints(List<AtomicPoint<?>> points, boolean retryOnRejection) {
        BuildResult built = buildPoints(points);

        if (built.pointsBuilder.getPointsCount() == 0) {
            if (built.hasSkippedArrays) {
                // Array measurements are being created asynchronously — tell S&F to retry
                logger.debug("No points to send, array measurements pending creation");
                return StorageResult.exception(
                        new RuntimeException("Array measurement(s) being created, retry later"), points);
            }
            if (built.hasFailedCreations) {
                // Measurement creation failed for supported-type points (Factry likely
                // unreachable). Return an exception so S&F keeps them in pending and retries
                // instead of silently dropping them.
                logger.warn("Could not create measurement(s) for new tag(s) — S&F will retry");
                return StorageResult.exception(
                        new RuntimeException("Measurement creation failed, retry later"), points);
            }
            if (measurementCache.size() == 0 && built.discardedPoints + built.pausedPoints == 0) {
                // All points skipped because measurement cache is empty (Factry likely down).
                // Return exception so S&F keeps them in pending for retry.
                logger.warn("No points to send, measurement cache empty — S&F will retry");
                return StorageResult.exception(
                        new RuntimeException("Measurement cache empty, cannot resolve UUIDs"), points);
            }
            logger.debug("No points to send (all skipped or discarded)");
            return stored(points, built);
        }

        try {
            logger.debug("Sending " + built.pointsBuilder.getPointsCount() + " points via createPoints");
            long beforeMs = System.currentTimeMillis();
            grpcClient.createPoints(built.pointsBuilder.build());
            long elapsedMs = System.currentTimeMillis() - beforeMs;
            metrics.recordStore(built.pointsBuilder.getPointsCount(), elapsedMs);
            logger.debug("createPoints succeeded for " + built.pointsBuilder.getPointsCount() + " points");

            if (built.hasSkippedArrays) {
                // Non-array points sent successfully, but array points were skipped.
                // Return exception so S&F retries the whole batch — the non-array points
                // will be re-sent (idempotent) and the array points will succeed next time.
                logger.debug("Array measurements pending, requesting S&F retry for array points");
                return StorageResult.exception(
                        new RuntimeException("Array measurement(s) being created, retry later"), points);
            }
            if (built.hasFailedCreations) {
                // Some points were sent, but others have no measurement yet (Factry did not
                // answer in time, or the creation failed). Return exception so S&F retries the
                // whole batch — the sent points will be re-sent (idempotent) — instead of
                // acknowledging the deferred points and losing them.
                logger.warn("Could not create measurement(s) for new tag(s) — S&F will retry the flush");
                return StorageResult.exception(
                        new RuntimeException("Measurement creation failed, retry later"), points);
            }
            return stored(points, built);

        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();

            // Connection errors — let caller handle (S&F retry)
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                throw e;
            }

            // Server rejection — likely deleted measurements. Evict and retry once.
            if (retryOnRejection && !built.usedUUIDs.isEmpty()) {
                logger.warn("Server rejected points (" + code + "), evicting "
                        + built.usedUUIDs.size() + " measurement UUIDs and retrying: " + e.getMessage());
                for (String uuid : built.usedUUIDs) {
                    measurementCache.evictByUUID(uuid);
                }
                return sendPoints(points, false);
            }

            throw e;
        }
    }

    private static class BuildResult {
        final Points.Builder pointsBuilder;
        final Set<String> usedUUIDs;
        final boolean hasSkippedArrays;
        final boolean hasFailedCreations;
        /** Points not sent because their measurement refuses them (see {@link #refusal}). */
        final int discardedPoints;
        /** Points not sent because their measurement is Paused. */
        final int pausedPoints;

        BuildResult(Points.Builder pointsBuilder, Set<String> usedUUIDs, boolean hasSkippedArrays,
                    boolean hasFailedCreations, int discardedPoints, int pausedPoints) {
            this.pointsBuilder = pointsBuilder;
            this.usedUUIDs = usedUUIDs;
            this.hasSkippedArrays = hasSkippedArrays;
            this.hasFailedCreations = hasFailedCreations;
            this.discardedPoints = discardedPoints;
            this.pausedPoints = pausedPoints;
        }
    }

    /** Tracks array base paths for which measurement creation is in progress on a background thread. */
    private final Set<String> asyncArrayCreations = ConcurrentHashMap.newKeySet();

    /**
     * Last-known complete array value per base path (index → value). Ignition historizes
     * array elements independently and, thanks to per-element deadbands, only sends the
     * indices that actually changed — e.g. changing [1,2,3,4] to [1,2,4,5] arrives as just
     * {2:4, 3:5}. We merge those partial updates into this map so the full array [1,2,4,5]
     * is stored instead of the truncated [4,5]. On a cache miss (first write, or the first
     * write after a gateway restart) the map is seeded from Factry's last stored array
     * value. This is not the most efficient approach, but array tags are relatively rare.
     */
    private final Map<String, java.util.TreeMap<Integer, Object>> lastArrayValues = new ConcurrentHashMap<>();

    private BuildResult buildPoints(List<AtomicPoint<?>> points) {
        Points.Builder pointsBuilder = Points.newBuilder();
        Set<String> usedUUIDs = new HashSet<>();
        boolean hasFailedCreations = false;
        int discardedPoints = 0;
        int pausedPoints = 0;
        Map<MeasurementCache.WriteRefusal, List<String>> newlyRefused =
                new EnumMap<>(MeasurementCache.WriteRefusal.class);
        // Array element changes are collected here and merged into full snapshots below.
        Map<String, List<ArrayPointMerger.ArrayElement>> arrayElementsByPath = new HashMap<>();

        // First pass: collect unique tag paths so all missing measurements can be
        // created in a single gRPC call instead of one per tag.
        Map<String, Object> tagPathToValue = new LinkedHashMap<>();
        for (AtomicPoint<?> point : points) {
            String tagPath = TagPathUtil.storagePathToStoredPath(point.source().toString());
            Object value = point.value();

            logger.debug("Point: tagPath=" + tagPath
                    + ", value=" + value
                    + ", valueType=" + (value != null ? value.getClass().getName() : "null"));

            ArrayPointMerger.ArrayRef ref = ArrayPointMerger.parseArrayPath(tagPath);
            if (ref != null) {
                long ts = point.timestamp().toEpochMilli();
                arrayElementsByPath
                        .computeIfAbsent(ref.basePath, k -> new ArrayList<>())
                        .add(new ArrayPointMerger.ArrayElement(ref.index, ts, value, point.quality().getCode()));
                continue;
            }

            if (value != null) {
                // putIfAbsent: only the first value is used for data-type inference.
                tagPathToValue.putIfAbsent(tagPath, value);
            }
        }

        // Batch-resolve UUIDs — at most one CreateMeasurementsRequest for the whole flush.
        Map<String, String> uuidByPath = measurementCache.batchGetOrCreateUUIDs(tagPathToValue, grpcClient);

        // Second pass: build point protos using the resolved UUIDs.
        for (AtomicPoint<?> point : points) {
            String tagPath = TagPathUtil.storagePathToStoredPath(point.source().toString());
            Object value = point.value();

            if (value == null) {
                logger.debug("Skipping point for '" + tagPath + "': null value");
                continue;
            }
            if (ArrayPointMerger.parseArrayPath(tagPath) != null) {
                continue; // handled in the array block below
            }

            String measurementUUID = uuidByPath.get(tagPath);
            if (measurementUUID == null || measurementUUID.isEmpty()) {
                if (MeasurementCache.toFactryDataType(value) == null) {
                    logger.debug("Skipping point for '" + tagPath + "': unsupported value type "
                            + value.getClass().getName());
                    continue;
                }
                MeasurementCache.WriteRefusal refusal = refusal(tagPath,
                        measurementCache.writeTarget(tagPath, grpcClient.getCollectorUUID()), newlyRefused);
                if (refusal == MeasurementCache.WriteRefusal.PAUSED) {
                    pausedPoints++;
                } else if (refusal != null) {
                    discardedPoints++;
                } else {
                    logger.debug("Deferring point for '" + tagPath + "': measurement not created yet");
                    hasFailedCreations = true;
                }
                continue;
            }

            written(tagPath);
            Point.Builder pb = Point.newBuilder()
                    .setMeasurementUUID(measurementUUID)
                    .setTimestamp(Timestamps.fromMillis(point.timestamp().toEpochMilli()))
                    .setStatus(qualityToStatus(point.quality().getCode()));

            if (value instanceof Boolean) {
                pb.setValue(Value.newBuilder().setBoolValue((Boolean) value).build());
            } else if (value instanceof Number) {
                pb.setValue(Value.newBuilder().setNumberValue(((Number) value).doubleValue()).build());
            } else {
                pb.setValue(Value.newBuilder().setStringValue(value.toString()).build());
            }

            pointsBuilder.addPoints(pb.build());
            usedUUIDs.add(measurementUUID);
        }

        // Process grouped array points — combine into single points with ListValue.
        // Array measurement creation can block for seconds (polling for visibility),
        // which would stall the S&F forwarding thread. Instead, we only use the cache
        // for instant lookup and trigger creation asynchronously if missing.
        boolean hasSkippedArrays = false;
        for (Map.Entry<String, List<ArrayPointMerger.ArrayElement>> entry : arrayElementsByPath.entrySet()) {
            String basePath = entry.getKey();
            java.util.TreeMap<Long, java.util.TreeMap<Integer, ArrayPointMerger.ArrayElement>> byTimestamp =
                    ArrayPointMerger.groupByTimestamp(entry.getValue());
            // Any element serves to detect the array's element type for async creation.
            ArrayPointMerger.ArrayElement firstElement = byTimestamp.firstEntry().getValue().firstEntry().getValue();

            // Non-blocking: only check the cache, don't create. The target is looked up once,
            // so a cache refresh in between cannot change it under us.
            String collectorUUID = grpcClient.getCollectorUUID();
            Measurement target = measurementCache.writeTarget(basePath, collectorUUID);
            MeasurementCache.WriteRefusal refusal = refusal(basePath, target, newlyRefused);
            if (refusal == MeasurementCache.WriteRefusal.PAUSED) {
                // Keep the merged array current, so the first value after the resume is the
                // whole array as it is then; only the rows are dropped.
                mergeArray(basePath, target.getUuid(), byTimestamp);
                pausedPoints += entry.getValue().size();
                continue;
            }
            if (refusal != null) {
                discardedPoints += entry.getValue().size();
                continue;
            }
            String measurementUUID = MeasurementCache.writeRefusal(target, collectorUUID) == null
                    ? (target != null ? target.getUuid() : null) : null;
            if (measurementUUID == null || measurementUUID.isEmpty()) {
                // Trigger async measurement creation if not already in progress
                if (asyncArrayCreations.add(basePath)) {
                    String elementType = MeasurementCache.toFactryDataType(firstElement.value);
                    String arrayDataType = "[]" + (elementType != null ? elementType : "string");
                    logger.info("Array measurement '" + basePath + "' not in cache, creating asynchronously (" + arrayDataType + ")");
                    Thread creator = new Thread(() -> {
                        try {
                            measurementCache.getOrCreateUUID(basePath, grpcClient, arrayDataType);
                        } finally {
                            asyncArrayCreations.remove(basePath);
                        }
                    }, "array-measurement-creator-" + basePath);
                    creator.setDaemon(true);
                    creator.start();
                }
                hasSkippedArrays = true;
                continue;
            }

            written(basePath);
            List<ArrayPointMerger.ArraySnapshot> snapshots = mergeArray(basePath, measurementUUID, byTimestamp);

            for (ArrayPointMerger.ArraySnapshot snap : snapshots) {
                // Build a ListValue from the full merged array, in index order.
                com.google.protobuf.ListValue.Builder listBuilder = com.google.protobuf.ListValue.newBuilder();
                for (Object val : snap.values) {
                    appendArrayValue(listBuilder, val);
                }

                Point.Builder pb = Point.newBuilder()
                        .setMeasurementUUID(measurementUUID)
                        .setTimestamp(Timestamps.fromMillis(snap.timestampMillis))
                        .setStatus(qualityToStatus(snap.qualityCode))
                        .setValue(Value.newBuilder().setListValue(listBuilder));

                pointsBuilder.addPoints(pb.build());
                logger.debug("Built array point for '" + basePath + "' at " + snap.timestampMillis
                        + " with " + listBuilder.getValuesCount() + " elements");
            }
            usedUUIDs.add(measurementUUID);
        }

        newlyRefused.forEach((refusal, tagPaths) -> logger.warn(summary(refusal),
                tagPaths.size(), RefusalLog.names(tagPaths, 5)));
        return new BuildResult(pointsBuilder, usedUUIDs, hasSkippedArrays, hasFailedCreations,
                discardedPoints, pausedPoints);
    }

    /**
     * Merge the incoming (possibly partial) element changes into the last-known full array so
     * unchanged elements are preserved, and return one row per timestamp (see
     * {@link ArrayPointMerger#mergeUpdates}).
     */
    private List<ArrayPointMerger.ArraySnapshot> mergeArray(String basePath, String measurementUUID,
            java.util.TreeMap<Long, java.util.TreeMap<Integer, ArrayPointMerger.ArrayElement>> byTimestamp) {
        java.util.TreeMap<Integer, Object> state =
                lastArrayValues.computeIfAbsent(basePath, k -> new java.util.TreeMap<>());
        synchronized (state) {
            if (state.isEmpty()) {
                // Cache miss (first store for this array, or first after a restart) —
                // seed from Factry's last stored value so a partial update from Ignition
                // doesn't truncate the array.
                seedArrayStateFromFactry(basePath, measurementUUID, state);
            }
            return ArrayPointMerger.mergeUpdates(state, byTimestamp);
        }
    }

    /**
     * Why values for {@code tagPath} are not written to {@code target}, its measurement in the
     * cache, or null when they may be (or the measurement is still to be resolved). A refusal
     * the log has not reported yet is added to {@code newlyRefused}, for one warning per kind
     * and flush.
     */
    private MeasurementCache.WriteRefusal refusal(String tagPath, Measurement target,
            Map<MeasurementCache.WriteRefusal, List<String>> newlyRefused) {
        String collectorUUID = grpcClient.getCollectorUUID();
        MeasurementCache.WriteRefusal refusal = measurementCache.writeRefusal(tagPath, target, collectorUUID);
        if (refusal != null && refusalLog.refused(tagPath, refusal)) {
            newlyRefused.computeIfAbsent(refusal, k -> new ArrayList<>()).add(tagPath);
            logger.debug("Not writing values for '{}' ({}): measurement {} has status '{}' and belongs to"
                    + " collector {}; this collector is {}", tagPath, refusal, target.getUuid(),
                    target.getStatus(), target.getCollectorUUID(), collectorUUID);
        }
        return refusal;
    }

    /** The warning for the tags newly refused in a flush; takes their count and names. */
    private static String summary(MeasurementCache.WriteRefusal refusal) {
        switch (refusal) {
            case PAUSED:
                return "The Factry measurement of {} tag(s) is Paused: {}. Values are dropped while it is"
                        + " Paused; from the first cache refresh after it is Active again, new values are"
                        + " stored. Logged once per tag.";
            case OTHER_COLLECTOR:
                return "The Factry measurement of {} tag(s) belongs to another collector: {}. Factry would"
                        + " answer OK and drop their values, so they are discarded. Rename the tags, or move"
                        + " the measurements to this collector in Factry. Logged once per tag.";
            case NOT_ACTIVE:
            default:
                return "The Factry measurement of {} tag(s) exists but is not Active (for example"
                        + " Discovered): {}. Their values are discarded until it is Active, from the first"
                        + " cache refresh after that; it is not created again, which would only add a"
                        + " duplicate. Logged once per tag.";
        }
    }

    /** Record that {@code tagPath} was written, so a later refusal is logged again. */
    private void written(String tagPath) {
        if (refusalLog.written(tagPath) == MeasurementCache.WriteRefusal.PAUSED) {
            logger.info("The Factry measurement of '{}' is Active again; its new values are stored", tagPath);
        }
    }

    /** Append a Java value to a protobuf ListValue as the matching Value kind. */
    private static void appendArrayValue(com.google.protobuf.ListValue.Builder listBuilder, Object val) {
        if (val instanceof Boolean) {
            listBuilder.addValues(Value.newBuilder().setBoolValue((Boolean) val));
        } else if (val instanceof Number) {
            listBuilder.addValues(Value.newBuilder().setNumberValue(((Number) val).doubleValue()));
        } else if (val != null) {
            listBuilder.addValues(Value.newBuilder().setStringValue(val.toString()));
        } else {
            listBuilder.addValues(Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE));
        }
    }

    /**
     * Seed {@code state} with the elements of the most recent array value stored in Factry
     * for the given measurement. Used on a cache miss so that a partial element update from
     * Ignition is merged onto the full array rather than truncating it. Best-effort: on any
     * error (or when Factry has no prior value) the state is left unchanged.
     */
    private void seedArrayStateFromFactry(String basePath, String measurementUUID,
                                          java.util.TreeMap<Integer, Object> state) {
        try {
            QueryTimeseriesRequest req = QueryTimeseriesRequest.newBuilder()
                    .addMeasurementUUIDs(measurementUUID)
                    .setStart(Timestamps.fromMillis(0))
                    .setEnd(Timestamps.fromMillis(System.currentTimeMillis() + 86_400_000L))
                    .setDesc(true)
                    .setLimit(1)
                    .build();
            QueryTimeseriesResponse reply = grpcClient.queryTimeseries(req);
            for (Series series : reply.getSeriesList()) {
                for (SeriesPoint pt : series.getDataPointsList()) {
                    Value v = pt.getValue();
                    if (v.getKindCase() == Value.KindCase.LIST_VALUE) {
                        List<Value> list = v.getListValue().getValuesList();
                        for (int i = 0; i < list.size(); i++) {
                            state.put(i, FactryQueryEngine.protoValueToJava(list.get(i)));
                        }
                        logger.debug("Seeded array state for '" + basePath + "' with "
                                + list.size() + " elements from Factry");
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not seed array state for '" + basePath + "' from Factry: " + e.getMessage());
        }
    }

    static String qualityToStatus(int qualityCode) {
        // Ignition 8 encodes the quality LEVEL in the top two bits of the 32-bit code, NOT the
        // legacy 0..255 scale. Decode via the QualityCode API so an Uncertain code (~1.07e9) is
        // recognised as Uncertain instead of tripping a numeric >=192 "Good" threshold. Bad and
        // Error both map to Factry's "Bad" status.
        switch (QualityCode.getLevel(qualityCode)) {
            case Good:
                return "Good";
            case Uncertain:
                return "Uncertain";
            default:
                return "Bad";
        }
    }
}
