package io.factry.historian.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads operational settings from factry-historian.properties bundled in the module.
 * These are tuning values not exposed in the Ignition UI.
 */
public final class ModuleProperties {
    private static final Logger logger = LoggerFactory.getLogger(ModuleProperties.class);
    private static final Properties props = new Properties();

    static {
        try (InputStream in = ModuleProperties.class.getClassLoader()
                .getResourceAsStream("factry-historian.properties")) {
            if (in != null) {
                props.load(in);
                logger.debug("Loaded factry-historian.properties");
            } else {
                logger.warn("factry-historian.properties not found, using defaults");
            }
        } catch (IOException e) {
            logger.warn("Failed to load factry-historian.properties, using defaults", e);
        }
    }

    private ModuleProperties() {
    }

    public static long getWriteDeadlineSeconds() {
        return getLong("grpc.write.deadline.seconds", 3);
    }

    public static long getWriteDeadlineMillisPerPoint() {
        return getLong("grpc.write.deadline.ms.per.point", 1);
    }

    public static long getWriteDeadlineMaxSeconds() {
        return getLong("grpc.write.deadline.max.seconds", 30);
    }

    /**
     * Largest CreatePoints message the module sends; bigger flushes go in several calls. Well
     * under Factry's 64 MiB limit. Values under 64 KiB are taken as 64 KiB.
     */
    public static long getWriteMaxMessageBytes() {
        return getLong("grpc.write.max.message.bytes", 4L * 1024 * 1024);
    }

    /**
     * The link speed a write deadline allows for, in bytes per second of the uncompressed
     * message: a 4 MiB message gets 16 s on top of the base deadline. At least 1.
     */
    public static long getWriteMinBytesPerSecond() {
        return Math.max(1, getLong("grpc.write.min.bytes.per.second", 256 * 1024));
    }

    public static boolean isWriteGzip() {
        return Boolean.parseBoolean(props.getProperty("grpc.write.gzip", "true").trim());
    }

    public static long getReadDeadlineSeconds() {
        return getLong("grpc.read.deadline.seconds", 30);
    }

    public static long getCallDeadlineSeconds() {
        return getLong("grpc.call.deadline.seconds", 10);
    }

    public static long getShutdownGraceSeconds() {
        return getLong("grpc.shutdown.grace.seconds", 3);
    }

    public static long getStatusCacheMs() {
        return getLong("status.cache.ms", 30_000);
    }

    /** How often the cache asks Factry for this collector's changed measurements. */
    public static long getMeasurementCacheRefreshSeconds() {
        return getLong("measurement.cache.refresh.seconds", 30);
    }

    /** How often the cache reloads every measurement, collector and asset. */
    public static long getMeasurementCacheFullRefreshSeconds() {
        return getLong("measurement.cache.full.refresh.seconds", 600);
    }

    /** How long a read remembers that Factry has no measurement with a name. */
    public static long getMeasurementLookupMissSeconds() {
        return getLong("measurement.lookup.miss.seconds", 60);
    }

    public static int getMeasurementCachePageSize() {
        return (int) getLong("measurement.cache.page.size", 5000);
    }

    public static long getRawQueryMaxPoints() {
        return getLong("query.raw.max.points", 500_000);
    }

    private static long getLong(String key, long defaultValue) {
        String val = props.getProperty(key);
        if (val != null) {
            try {
                return Long.parseLong(val.trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid value for '{}': '{}', using default {}", key, val, defaultValue);
            }
        }
        return defaultValue;
    }
}
