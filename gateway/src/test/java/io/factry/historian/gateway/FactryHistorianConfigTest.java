package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Guards the (positional) mapping between the config record and the settings bean, so fields
 * — including the custom CA certificate — survive a UI save/load round trip.
 */
class FactryHistorianConfigTest {

    private static final String PEM =
            "-----BEGIN CERTIFICATE-----\nMIIB...test...\n-----END CERTIFICATE-----\n";

    @Test
    void roundTrip_preservesAllFieldsIncludingCustomCaCert() {
        FactryHistorianConfig original = new FactryHistorianConfig(
                "the-token", true, false, PEM, true, "/");

        FactryHistorianSettings settings = original.toSettings();
        assertEquals(PEM, settings.getCustomCaCert());

        FactryHistorianConfig back = FactryHistorianConfig.fromSettings(settings);
        assertEquals(original, back, "record round trip through settings must be lossless");
    }

    @Test
    void toSettings_mapsCustomCaCert() {
        FactryHistorianConfig config = new FactryHistorianConfig(
                "t", true, false, PEM, false, "");
        assertEquals(PEM, config.toSettings().getCustomCaCert());
    }
}
