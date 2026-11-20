package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.gson.JsonObject;
import com.inductiveautomation.ignition.common.gson.JsonParser;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.SchemaUtil;
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
                FakeSecrets.embedded("the-token"), "historian", 8001, false, PEM, true, "/");

        FactryHistorianSettings settings = original.toSettings();
        assertEquals(PEM, settings.getCustomCaCert());

        FactryHistorianConfig back = FactryHistorianConfig.fromSettings(settings);
        assertEquals(original, back, "record round trip through settings must be lossless");
    }

    @Test
    void toSettings_mapsCustomCaCert() {
        FactryHistorianConfig config = new FactryHistorianConfig(
                FakeSecrets.embedded("t"), "", null, false, PEM, false, "");
        assertEquals(PEM, config.toSettings().getCustomCaCert());
    }

    /**
     * A host and port stored by an older version (the form had no field for them) show in the form
     * as overrides, and the next save stores them under the new names (#37). "useTls" is gone:
     * Factry's gRPC port always uses TLS (#55).
     */
    @Test
    void olderProfileShowsItsHostAndPortAsOverrides() throws Exception {
        FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
        FactryHistorianSettings settings = ep.decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"grpcHost\":\"historian\",\"grpcPort\":8001,\"useTls\":false}"));

        FactryHistorianConfig form = FactryHistorianConfig.fromSettings(settings);
        assertEquals("historian", form.hostOverride());
        assertEquals(8001, form.portOverride());

        JsonObject stored = ep.encode(settings).getAsJsonObject();
        assertEquals("historian", stored.get("hostOverride").getAsString());
        assertEquals(8001, stored.get("portOverride").getAsInt());
        assertFalse(stored.has("grpcHost") || stored.has("grpcPort") || stored.has("useTls"), stored.toString());
    }

    /** "localhost" and 9876 were defaults written with every save, not choices: the token decides. */
    @Test
    void olderDefaultsAreNoOverride() throws Exception {
        FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
        FactryHistorianSettings settings = ep.decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"grpcHost\":\"localhost\",\"grpcPort\":9876,\"useTls\":true}"));

        assertEquals("", FactryHistorianConfig.fromSettings(settings).hostOverride());
        assertEquals(0, FactryHistorianConfig.fromSettings(settings).portOverride());
        JsonObject stored = ep.encode(settings).getAsJsonObject();
        assertFalse(stored.has("portOverride") || stored.has("grpcHost") || stored.has("grpcPort"), stored.toString());
    }

    @Test
    void formShowsTheOverridesAndNoTlsSwitch() {
        JsonObject properties = SchemaUtil.fromType(FactryHistorianConfig.class).getAsJsonObject("properties");
        assertTrue(properties.has("hostOverride"));
        assertTrue(properties.has("portOverride"));
        assertTrue(properties.has("collectorToken"));
        assertFalse(properties.has("useTls"));
    }
}
