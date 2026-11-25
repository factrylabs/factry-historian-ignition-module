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
                FakeSecrets.embedded("the-token"), "historian", 8001, false, false, PEM, true, "/");

        FactryHistorianSettings settings = original.toSettings();
        assertEquals(PEM, settings.getCustomCaCert());

        FactryHistorianConfig back = FactryHistorianConfig.fromSettings(settings);
        assertEquals(original, back, "record round trip through settings must be lossless");
    }

    @Test
    void toSettings_mapsCustomCaCert() {
        FactryHistorianConfig config = new FactryHistorianConfig(
                FakeSecrets.embedded("t"), "", null, true, false, PEM, false, "");
        assertEquals(PEM, config.toSettings().getCustomCaCert());
    }

    /**
     * A host and port stored by an older version (the form had no field for them) show in the form
     * as overrides, and the next save stores them under the new names (#37). Its "Use TLS" off
     * becomes Plain Text on: right for a Factry whose gRPC TLS is off, and the token's claim still
     * means TLS against a default Factry.
     */
    @Test
    void olderProfileShowsItsHostAndPortAsOverrides() throws Exception {
        FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
        FactryHistorianSettings settings = ep.decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"grpcHost\":\"historian\",\"grpcPort\":8001,\"useTls\":false}"));

        FactryHistorianConfig form = FactryHistorianConfig.fromSettings(settings);
        assertEquals("historian", form.hostOverride());
        assertEquals(8001, form.portOverride());
        assertTrue(form.plainText());

        JsonObject stored = ep.encode(settings).getAsJsonObject();
        assertEquals("historian", stored.get("hostOverride").getAsString());
        assertEquals(8001, stored.get("portOverride").getAsInt());
        assertTrue(stored.get("plainText").getAsBoolean(), stored.toString());
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
        assertFalse(FactryHistorianConfig.fromSettings(settings).plainText());
        JsonObject stored = ep.encode(settings).getAsJsonObject();
        assertFalse(stored.has("portOverride") || stored.has("grpcHost") || stored.has("grpcPort")
                || stored.has("useTls"), stored.toString());
    }

    /**
     * Ignition's create form shows no defaults, so every checkbox must default to off: Plain Text,
     * not a "Use TLS" that would look off while it saves on.
     */
    @Test
    void formShowsTheOverridesAndPlainTextOffByDefault() {
        JsonObject properties = SchemaUtil.fromType(FactryHistorianConfig.class).getAsJsonObject("properties");
        assertTrue(properties.has("hostOverride"));
        assertTrue(properties.has("portOverride"));
        assertTrue(properties.has("collectorToken"));
        assertTrue(properties.has("plainText"));
        assertFalse(properties.has("useTls"));
        // Every checkbox, and the settings a new profile gets when it is saved untouched.
        FactryHistorianConfig untouched = FactryHistorianConfig.fromSettings(new FactryHistorianSettings());
        int checkboxes = 0;
        for (String name : properties.keySet()) {
            JsonObject property = properties.getAsJsonObject(name);
            if (property.has("type") && "boolean".equals(property.get("type").getAsString())) {
                checkboxes++;
                assertTrue(property.has("default") && !property.get("default").getAsBoolean(),
                        name + " must default to false: " + property);
            }
        }
        assertEquals(3, checkboxes, properties.toString());
        assertFalse(untouched.plainText() || untouched.skipTlsVerification() || untouched.debugLogging());
    }

    /** A stored plainText wins over the legacy useTls, also when both are there (hand-edited config). */
    @Test
    void storedPlainTextWinsOverUseTls() throws Exception {
        FactryHistorianSettings settings = new FactryHistorianExtensionPoint().decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"useTls\":false,\"plainText\":false}"));
        assertFalse(settings.isPlainText());
        settings = new FactryHistorianExtensionPoint().decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"useTls\":true,\"plainText\":true}"));
        assertTrue(settings.isPlainText());
    }

    /** A profile stored without either field (a new one, or saved by #42's first version) uses TLS. */
    @Test
    void profileWithoutTlsFieldsIsNotPlainText() throws Exception {
        FactryHistorianSettings settings = new FactryHistorianExtensionPoint().decode(JsonParser.parseString(
                "{\"token\":\"a.b.c\",\"hostOverride\":\"historian\"}"));
        assertFalse(settings.isPlainText());
        assertFalse(FactryHistorianConfig.fromSettings(settings).plainText());
    }
}
