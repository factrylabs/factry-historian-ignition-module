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

import io.grpc.Status;
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
        super(context, historianName, LoggerEx.newBuilder().build(FactryStorageEngine.class),
                QualifiedPathAdapter.DEFAULT, ImmediateStorageStrategy.create(historianName));
        this.settings = settings;
        this.measurementCache = measurementCache;
        this.metrics = metrics;
        this.pointWriter = new PointWriter(grpcClient, measurementCache, metrics);
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
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                logger.warn("Connection error storing points, S&F will retry: " + e.getMessage());
                return StorageResult.exception(e, points);
            }
            logger.error("Server rejected points (" + code + "): " + e.getMessage());
            return StorageResult.failure(points);
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
        // Measurement lifecycle is managed by the Factry platform, not by this collector.
        // No retirement action needed — just acknowledge the changes.
        logger.debug("applySourceChanges called with " + changes.size() + " changes (no-op for Factry)");
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
        // in FactryHistoryProvider.getStatus(). When the sink is not accepting, S&F keeps
        // points in pending without attempting to forward them.
        //
        // Verified by decompiling AbstractStorageEngine and TagHistoryDataSinkBridge
        // (historian-gateway-api 1.3.3). See git history: commit e7a5c84 broke this.
        return false;
    }

    void updateSettings(FactryHistorianSettings newSettings) {
        this.settings = newSettings;
    }

    public void shutdown() {
        logger.info("Shutting down Factry Storage Engine");
    }
}
