package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.AbstractHistorian;
import com.inductiveautomation.historian.gateway.api.query.QueryEngine;
import com.inductiveautomation.historian.gateway.api.storage.StorageEngine;
import com.inductiveautomation.historian.gateway.interop.TagHistoryDataSinkBridge;
import com.inductiveautomation.historian.gateway.interop.TagHistoryStorageEngineBridge;
import com.inductiveautomation.ignition.common.resourcecollection.ChangeOperation;
import com.inductiveautomation.ignition.common.resourcecollection.Resource;
import com.inductiveautomation.ignition.common.resourcecollection.ResourceBuilder;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import com.inductiveautomation.ignition.gateway.storeforward.StorageKey;
import com.inductiveautomation.ignition.gateway.storeforward.engine.EngineInformation;
import com.inductiveautomation.ignition.gateway.storeforward.quarantine.QuarantineInterface;
import com.inductiveautomation.ignition.gateway.storeforward.quarantine.QuarantinedDataInfo;
import com.inductiveautomation.ignition.gateway.storeforward.resource.StoreAndForwardEngineSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FactryHistoryProvider extends AbstractHistorian<FactryHistorianSettings> {
    private static final Logger logger = LoggerFactory.getLogger(FactryHistoryProvider.class);

    public static final String HISTORIAN_NAME = "FactryHistorian";
    public static final String HISTORIAN_ID = "factry-historian";

    private final GatewayContext context;
    private final FactryQueryEngine queryEngine;
    private final FactryStorageEngine storageEngine;
    private final HistorianMetrics metrics;
    private volatile FactryHistorianSettings settings;
    private final FactryGrpcClient grpcClient;
    private final MeasurementCache measurementCache;

    private TagHistoryStorageEngineBridge storageBridge;
    private TagHistoryDataSinkBridge dataSinkBridge;
    private ScheduledExecutorService scheduledExecutor;
    private volatile String sfEngineName;

    /** Cached status to avoid hitting gRPC on every gateway UI poll. */
    private volatile ProfileStatus cachedStatus = ProfileStatus.UNKNOWN;
    private volatile long statusCheckedAt = 0;
    private static final long STATUS_CACHE_MS = ModuleProperties.getStatusCacheMs();

    public FactryHistoryProvider(GatewayContext context, String historianName, FactryHistorianSettings settings) {
        super(context, historianName);
        this.context = context;
        this.settings = settings;
        settings.applyTokenDefaults(settings.getToken());
        settings.validate();
        this.metrics = new HistorianMetrics();

        this.grpcClient = new FactryGrpcClient(
                settings.getGrpcHost(),
                settings.getGrpcPort(),
                settings.getCollectorUUID(),
                settings.getToken(),
                settings.isUseTls(),
                settings.isSkipTlsVerification(),
                settings.getCustomCaCert()
        );
        this.measurementCache = new MeasurementCache();

        this.queryEngine = new FactryQueryEngine(context, historianName, settings, grpcClient, measurementCache, metrics);
        this.storageEngine = new FactryStorageEngine(context, historianName, settings, grpcClient, measurementCache, metrics);

        logger.info("Factry Historian created: name={}, grpcTarget={}:{}, collectorUUID={}",
                historianName, settings.getGrpcHost(), settings.getGrpcPort(), settings.getCollectorUUID());
    }

    public FactryHistoryProvider(GatewayContext context, String historianName) {
        this(context, historianName, new FactryHistorianSettings());
    }

    @Override
    protected void onStartup() throws Exception {
        logger.info("Factry Historian - Starting Up");
        logger.debug("Name: {}", historianName);
        logger.debug("Settings: {}", settings);

        // Register this collector with Factry so it shows as active
        try {
            var schema = io.factry.historian.proto.RegisterCollectorSchema.newBuilder()
                    .setCollectorType("ignition")
                    .setBuildVersion(FactryHistorianModule.MODULE_VERSION)
                    .setBuildOs(System.getProperty("os.name", "unknown"))
                    .setBuildArch(System.getProperty("os.arch", "unknown"))
                    .build();
            var collector = grpcClient.registerCollector(schema);
            logger.info("Collector registered with Factry: uuid={}, type={}, status={}",
                    collector.getUuid(), collector.getType(), collector.getStatus());

            // Set collector state to active/collecting
            var state = com.google.protobuf.Struct.newBuilder()
                    .putFields("status", com.google.protobuf.Value.newBuilder()
                            .setStringValue("Active").build())
                    .putFields("health", com.google.protobuf.Value.newBuilder()
                            .setStringValue("Collecting").build())
                    .build();
            grpcClient.updateCollectorState(state);
        } catch (Exception e) {
            logger.warn("Failed to register collector with Factry: " + e.getMessage());
        }

        try {
            measurementCache.refresh(grpcClient);
            logger.info("Measurement cache pre-populated with {} entries", measurementCache.size());
        } catch (Exception e) {
            logger.warn("Initial measurement cache refresh failed (Factry may still be starting): {}. " +
                    "Will retry via scheduled refresh.", e.getMessage());
        }

        // Set up Store & Forward — always enabled.
        // The S&F engine name matches the historian profile name because
        // system.tag.storeTagHistory() looks up the engine by historian name.
        final String sfEngine = historianName;

        // Ensure the S&F engine exists — create it if missing
        ensureStoreAndForwardEngine(sfEngine);

        StorageKey storageKey = StorageKey.of(sfEngine, historianName);

        // Sink bridge: wraps our storage engine, receives data from S&F
        dataSinkBridge = TagHistoryDataSinkBridge.getOrCreate(
                context, storageEngine, storageKey);
        context.getStoreAndForwardManager().registerSink(dataSinkBridge);

        // Workaround for Ignition 8.3.x: registerSink() only transitions the sink
        // to STARTED state. The S&F engine doesn't call initialize() on sinks
        // registered after the engine has started, leaving it stuck in "Storage Only".
        // Force initialization via reflection so the sink reaches ACCEPTING state.
        if (!dataSinkBridge.isAccepting()) {
            try {
                Method initMethod = dataSinkBridge.getClass().getSuperclass()
                        .getDeclaredMethod("initialize");
                initMethod.setAccessible(true);
                initMethod.invoke(dataSinkBridge);
            } catch (Exception e) {
                logger.error("Failed to initialize S&F data sink bridge", e);
            }
        }

        // Storage bridge: replaces direct storage, routes data into S&F
        storageBridge = TagHistoryStorageEngineBridge.getOrCreate(
                context, historianName, sfEngine);

        // Schedule automatic quarantine retry every 30 seconds.
        // Quarantined data is never retried automatically by S&F,
        // so we periodically move it back to pending for re-forwarding.
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "factry-historian-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduledExecutor.scheduleWithFixedDelay(
                () -> retryQuarantinedData(sfEngine), 30, 30, TimeUnit.SECONDS);

        // Periodic measurement cache refresh to detect deleted measurements
        long refreshInterval = ModuleProperties.getMeasurementCacheRefreshSeconds();
        scheduledExecutor.scheduleWithFixedDelay(
                this::refreshMeasurementCache, refreshInterval, refreshInterval, TimeUnit.SECONDS);

        this.sfEngineName = sfEngine;

        logger.info("Store-and-forward enabled via engine '{}', sink accepting: {}",
                sfEngine, dataSinkBridge.isAccepting());

        // Send periodic health updates to Factry (heartbeat)
        scheduledExecutor.scheduleWithFixedDelay(
                this::sendHealthUpdate, 0, 30, TimeUnit.SECONDS);

        // Prime the status cache now that the connection has just been exercised
        // (registerCollector + cache refresh above), so the gateway UI shows the
        // real status on its first poll instead of waiting for the cache to expire.
        refreshConnectionStatus();

        logger.info("Factry Historian - Startup Complete");
    }

    @Override
    protected void onShutdown() {
        logger.info("Factry Historian - Shutting Down");
        logger.debug("Name: {}", historianName);

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
        }

        if (dataSinkBridge != null) {
            context.getStoreAndForwardManager().unregisterSink(dataSinkBridge);
            dataSinkBridge.onShutdown();
            dataSinkBridge = null;
        }
        storageBridge = null;

        storageEngine.shutdown();
        grpcClient.shutdown();
        logger.info("Factry Historian - Shutdown Complete");
    }

    @Override
    public FactryHistorianSettings getSettings() {
        return settings;
    }

    @Override
    public Optional<QueryEngine> getQueryEngine() {
        return Optional.of(queryEngine);
    }

    @Override
    public Optional<StorageEngine> getStorageEngine() {
        return Optional.of(storageBridge != null ? storageBridge : storageEngine);
    }

    @Override
    public boolean handleNameChange(String newName) {
        logger.info("Historian name changed: {} -> {}", historianName, newName);
        // Measurement names in Factry don't contain the historian profile name,
        // so no data migration is needed.
        // The framework updates the historianName field in AbstractHistorian.
        return true;
    }

    @Override
    public boolean handleSettingsChange(FactryHistorianSettings newSettings) {
        logger.info("Historian settings change: {}", newSettings);

        try {
            newSettings.applyTokenDefaults(newSettings.getToken());
            newSettings.validate();

            // Reconfigure the gRPC client (shuts down old channel, creates new one)
            grpcClient.reconfigure(
                    newSettings.getGrpcHost(),
                    newSettings.getGrpcPort(),
                    newSettings.getCollectorUUID(),
                    newSettings.getToken(),
                    newSettings.isUseTls(),
                    newSettings.isSkipTlsVerification(),
                    newSettings.getCustomCaCert()
            );

            // Refresh measurement cache from the new endpoint
            measurementCache.refresh(grpcClient);

            // Update settings references in the engines
            queryEngine.updateSettings(newSettings);
            storageEngine.updateSettings(newSettings);
            this.settings = newSettings;

            // Re-test against the new endpoint and refresh the cached status so the
            // gateway UI reflects the change on its next poll, not up to STATUS_CACHE_MS later.
            refreshConnectionStatus();

            logger.info("Settings change applied successfully");
            return true;
        } catch (Exception e) {
            logger.error("Failed to apply settings change", e);
            // Reflect the failure immediately rather than serving a stale cached status.
            cachedStatus = ProfileStatus.ERRORED;
            statusCheckedAt = System.currentTimeMillis();
            return false;
        }
    }

    @Override
    public ProfileStatus getStatus() {
        if (!started) {
            return ProfileStatus.UNKNOWN;
        }

        long now = System.currentTimeMillis();
        if (now - statusCheckedAt < STATUS_CACHE_MS) {
            return cachedStatus;
        }

        return refreshConnectionStatus();
    }

    /**
     * Actively test the Factry connection, update the cached status and its
     * timestamp, and toggle the S&F sink to match the connection state.
     * <p>
     * Called lazily from {@link #getStatus()} when the cache expires, and
     * eagerly right after startup and settings changes so the gateway UI
     * reflects the new state on its very next poll instead of waiting out the
     * {@link #STATUS_CACHE_MS} window (or showing a stale value for up to that long).
     */
    private ProfileStatus refreshConnectionStatus() {
        statusCheckedAt = System.currentTimeMillis();
        boolean connected = grpcClient.testConnection();
        cachedStatus = connected ? ProfileStatus.RUNNING : ProfileStatus.ERRORED;

        // Toggle S&F sink: when Factry is down, stop accepting so points stay in pending.
        // When Factry is back, re-initialize so forwarding resumes.
        if (dataSinkBridge != null) {
            if (connected && !dataSinkBridge.isAccepting()) {
                logger.info("Factry connection restored, re-enabling S&F sink");
                try {
                    java.lang.reflect.Method initMethod = dataSinkBridge.getClass().getSuperclass()
                            .getDeclaredMethod("initialize");
                    initMethod.setAccessible(true);
                    initMethod.invoke(dataSinkBridge);
                } catch (Exception e) {
                    logger.error("Failed to re-initialize S&F sink", e);
                }
            } else if (!connected && dataSinkBridge.isAccepting()) {
                logger.info("Factry connection lost, pausing S&F sink to buffer points");
                try {
                    java.lang.reflect.Method uninitMethod = dataSinkBridge.getClass().getSuperclass()
                            .getDeclaredMethod("uninitialize");
                    uninitMethod.setAccessible(true);
                    uninitMethod.invoke(dataSinkBridge);
                } catch (Exception e) {
                    logger.error("Failed to uninitialize S&F sink", e);
                }
            }
        }

        return cachedStatus;
    }

    /**
     * Ensure that the named S&F engine exists in the gateway configuration.
     * If not, creates one with sensible defaults (SQLite-backed, matching
     * Ignition's default engine settings).
     */
    private void ensureStoreAndForwardEngine(String engineName) {
        Optional<EngineInformation> existing =
                context.getStoreAndForwardManager().getEngineInformation(engineName, true);
        if (existing.isPresent()) {
            logger.debug("S&F engine '{}' already exists", engineName);
            return;
        }

        // Also check if a resource already exists (covers engines not yet started)
        List<Resource> sfResources = context.getConfigurationManager()
                .getResources(StoreAndForwardEngineSettings.getType());
        for (Resource r : sfResources) {
            if (r.getResourceName().equalsIgnoreCase(engineName)) {
                logger.debug("S&F engine resource '{}' already exists (as '{}')",
                        engineName, r.getResourceName());
                return;
            }
        }

        logger.info("S&F engine '{}' does not exist — creating it automatically", engineName);
        try {
            StoreAndForwardEngineSettings engineSettings = new StoreAndForwardEngineSettings();

            ResourceBuilder builder = Resource.newBuilder();
            builder.setResourceCollectionName("core");
            builder.setResourcePath(StoreAndForwardEngineSettings.getType().childPath(engineName));
            builder.setApplicationScope("G");
            StoreAndForwardEngineSettings.getMeta().getCodec().encode(engineSettings, builder);
            Resource resource = builder.build();

            ChangeOperation createOp = ChangeOperation.newCreateOp(resource);
            context.getConfigurationManager().push(List.of(createOp)).get(10, TimeUnit.SECONDS);

            logger.info("S&F engine '{}' created successfully", engineName);
        } catch (Exception e) {
            logger.error("Failed to create S&F engine '{}' — store-and-forward may not work", engineName, e);
        }
    }

    private void retryQuarantinedData(String engineName) {
        try {
            // Test connection and re-enable forwarding if the server is back
            if (!grpcClient.isConnected()) {
                if (grpcClient.testConnection()) {
                    logger.info("Factry server is reachable again, re-enabling S&F forwarding");
                } else {
                    logger.debug("Factry server still unreachable, S&F will keep buffering");
                    return;
                }
            }

            Optional<QuarantineInterface<?>> qi =
                    context.getStoreAndForwardManager().getQuarantineInterface(engineName);
            if (qi.isPresent()) {
                int count = qi.get().getQuarantinedCount();
                if (count > 0) {
                    List<Integer> ids = qi.get().getQuarantinedDataInfo().stream()
                            .map(QuarantinedDataInfo::id)
                            .collect(Collectors.toList());
                    qi.get().retryQuarantined(ids);
                    logger.info("Retrying {} quarantined data entries for S&F engine '{}'",
                            ids.size(), engineName);
                }
            }
        } catch (Exception e) {
            logger.debug("Error retrying quarantined data", e);
        }
    }

    private void sendHealthUpdate() {
        try {
            var update = io.factry.historian.proto.HealthUpdate.newBuilder()
                    .setHealth("Collecting")
                    .setTimestamp(com.google.protobuf.util.Timestamps.fromMillis(System.currentTimeMillis()))
                    .build();
            var updates = io.factry.historian.proto.HealthUpdates.newBuilder()
                    .addHealthUpdates(update)
                    .build();
            grpcClient.updateHealth(updates);
        } catch (Exception e) {
            logger.warn("Failed to send health update: " + e.getMessage());
        }
    }

    private void refreshMeasurementCache() {
        try {
            int before = measurementCache.size();
            measurementCache.refresh(grpcClient);
            int after = measurementCache.size();
            if (before != after) {
                logger.info("Measurement cache refreshed: {} -> {} entries", before, after);
            } else {
                logger.debug("Measurement cache refreshed: {} entries (unchanged)", after);
            }
        } catch (Exception e) {
            logger.warn("Failed to refresh measurement cache: " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "FactryHistoryProvider{" +
                "name='" + historianName + '\'' +
                ", settings=" + settings +
                '}';
    }
}
