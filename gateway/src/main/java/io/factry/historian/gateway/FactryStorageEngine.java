package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.storage.AbstractStorageEngine;
import com.inductiveautomation.historian.gateway.api.paths.QualifiedPathAdapter;
import com.inductiveautomation.historian.gateway.api.storage.strategy.ImmediateStorageStrategy;
import com.inductiveautomation.historian.common.model.data.AtomicPoint;
import com.inductiveautomation.historian.common.model.data.MetadataPoint;
import com.inductiveautomation.historian.common.model.data.SourceChangePoint;
import com.inductiveautomation.historian.common.model.data.StorageResult;
import com.inductiveautomation.ignition.common.util.LoggerEx;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;

import io.grpc.StatusRuntimeException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FactryStorageEngine extends AbstractStorageEngine {
    private volatile FactryHistorianSettings settings;
    private final MeasurementCache measurementCache;
    private final HistorianMetrics metrics;
    private final PointWriter pointWriter;

    public FactryStorageEngine(
        GatewayContext context,
        String historianName,
        FactryHistorianSettings settings,
        FactryGrpcClient grpcClient,
        MeasurementCache measurementCache,
        HistorianMetrics metrics
    ) {
        this(context, historianName, settings, grpcClient, measurementCache, metrics, new WriteHealth());
    }

    public FactryStorageEngine(
        GatewayContext context,
        String historianName,
        FactryHistorianSettings settings,
        FactryGrpcClient grpcClient,
        MeasurementCache measurementCache,
        HistorianMetrics metrics,
        WriteHealth writeHealth
    ) {
        super(context, historianName, LoggerEx.newBuilder().build(FactryStorageEngine.class),
                QualifiedPathAdapter.DEFAULT, ImmediateStorageStrategy.create(historianName));
        this.settings = settings;
        this.measurementCache = measurementCache;
        this.metrics = metrics;
        this.pointWriter = new PointWriter(grpcClient, measurementCache, metrics, writeHealth);
        logger.debug("Factry Storage Engine initialized with gRPC target: " +
                settings.getGrpcHost() + ":" + settings.getGrpcPort());
    }

    @Override
    protected StorageResult<AtomicPoint<?>> doStoreAtomic(List<AtomicPoint<?>> points) {
        if (settings.isDebugLogging()) {
            StringBuilder sb = new StringBuilder("doStoreAtomic called with " + points.size() + " points: ");
            for (AtomicPoint<?> p : points) {
                sb.append(p.source()).append(" ");
            }
            logger.info(sb.toString());
        }

        try {
            return pointWriter.write(points);
        } catch (StatusRuntimeException e) {
            metrics.recordStoreError();
            // Logged by WriteHealth, once per answer rather than once per flush.
            // Always attach the error. TagHistoryDataSinkBridge only keeps a batch in S&F when the
            // result carries one; a failure without an error counts as handled and the points are lost.
            return StorageResult.exception(e, points);
        } catch (Exception e) {
            metrics.recordStoreError();
            logger.error("Unexpected error storing points via gRPC", e);
            return StorageResult.exception(e, points);
        }
    }

    @Override
    protected StorageResult<MetadataPoint> doStoreMetadata(List<MetadataPoint> metadataPoints) {
        // Factry doesn't support updating metadata after creation, but we cache the
        // properties so they can be applied as initial values when a measurement is
        // first created via MeasurementCache.getOrCreateUUID().
        logger.debug("doStoreMetadata called with " + metadataPoints.size() + " points");
        for (MetadataPoint point : metadataPoints) {
            String tagPath = TagPathUtil.storagePathToStoredPath(point.source().toString());
            com.inductiveautomation.ignition.common.config.PropertySet ps = point.value();
            if (ps != null) {
                Map<String, String> properties = new HashMap<>();
                for (var pv : ps) {
                    Object val = pv.getValue();
                    if (val != null) {
                        properties.put(pv.getProperty().getName(), val.toString());
                    }
                }
                if (!properties.isEmpty()) {
                    measurementCache.storeMetadata(tagPath, properties);
                }
            }
        }
        return StorageResult.success(metadataPoints);
    }

    @Override
    protected StorageResult<SourceChangePoint> applySourceChanges(List<SourceChangePoint> changes) {
        // Measurement lifecycle is managed by the Factry platform, not by this collector. Only the
        // cached metadata of retired or renamed tags is dropped, so the cache does not grow with them.
        logger.debug("applySourceChanges called with " + changes.size() + " changes");
        for (SourceChangePoint change : changes) {
            // A retirement has no new source; a rename has a different one.
            if (change.oldSource() != null && !change.oldSource().equals(change.newSource())) {
                measurementCache.forgetMetadata(TagPathUtil.storagePathToStoredPath(change.oldSource().toString()));
            }
        }
        return StorageResult.success(changes);
    }

    @Override
    protected boolean isEngineUnavailable() {
        // WARNING: DO NOT change this to return true when the connection is down.
        //
        // When isEngineUnavailable() returns true, the SDK's AbstractStorageEngine.processPoints()
        // returns StorageResult.failure(UNAVAILABLE_QUALITY, points). The TagHistoryDataSinkBridge
        // then silently drops the points (failure has no error, so no DataStorageException is thrown).
        //
        // Buffering during downtime is handled by toggling the S&F sink's accepting state
        // in FactryHistoryProvider.refreshConnectionStatus(). When the sink is not accepting, S&F keeps
        // points in pending without attempting to forward them.
        //
        // Verified by decompiling AbstractStorageEngine and TagHistoryDataSinkBridge
        // (historian-gateway-api 1.3.3). See git history: commit e7a5c84 broke this.
        return false;
    }

    void updateSettings(FactryHistorianSettings newSettings) {
        this.settings = newSettings;
    }

    /** Forget the refusals of writes that were logged, for new settings. */
    void clearRefusals() {
        pointWriter.clearRefusals();
    }

    public void shutdown() {
        logger.info("Shutting down Factry Storage Engine");
    }
}
