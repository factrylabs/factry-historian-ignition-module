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

    public static long getStatusCacheMs() {
        return getLong("status.cache.ms", 30_000);
    }

    public static long getMeasurementCacheRefreshSeconds() {
        return getLong("measurement.cache.refresh.seconds", 30);
    }

    public static int getMeasurementCachePageSize() {
        return (int) getLong("measurement.cache.page.size", 5000);
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
