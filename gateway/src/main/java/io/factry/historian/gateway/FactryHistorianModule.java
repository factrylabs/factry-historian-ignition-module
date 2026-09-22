package io.factry.historian.gateway;

import java.io.InputStream;
import java.util.Properties;

public class FactryHistorianModule {
    public static final String MODULE_ID = "io.factry.FactryHistorian";

    public static final String MODULE_VERSION;

    static {
        String version = "unknown";
        try (InputStream is = FactryHistorianModule.class.getResourceAsStream("/version.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                version = props.getProperty("module.version", "unknown");
            }
        } catch (Exception ignored) {
        }
        MODULE_VERSION = version;
    }
}
