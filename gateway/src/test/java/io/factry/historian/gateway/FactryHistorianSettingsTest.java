package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class FactryHistorianSettingsTest {

    private FactryHistorianSettings validSettings() {
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.applyTokenDefaults(jwt("http://historian.example.com", "8001"));
        return s;
    }

    /** Settings with a Port Override, resolved against a token naming port 8001. */
    private FactryHistorianSettings withPortOverride(Integer port) {
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.setPortOverride(port);
        s.applyTokenDefaults(jwt("http://historian.example.com", "8001"));
        return s;
    }

    static String jwt(String aud, String grpcPort) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String payload = "{\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\",\"name\":\"Ignition\","
                + "\"aud\":\"" + aud + "\",\"grpc-port\":\"" + grpcPort + "\"}";
        return "e30." + enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig";
    }

    // --- valid settings ---

    @Test
    void validate_validSettings_noException() {
        assertDoesNotThrow(() -> validSettings().validate());
    }

    @Test
    void validate_defaults_failsOnMissingCollectorUUID() {
        FactryHistorianSettings s = new FactryHistorianSettings();
        // defaults have empty collectorUUID
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    // --- collectorUUID ---

    @Test
    void validate_nullCollectorUUID() {
        FactryHistorianSettings s = validSettings();
        s.setCollectorUUID(null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, s::validate);
        assertTrue(e.getMessage().contains("Collector ID"));
    }

    @Test
    void validate_emptyCollectorUUID() {
        FactryHistorianSettings s = validSettings();
        s.setCollectorUUID("");
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    @Test
    void validate_blankCollectorUUID() {
        FactryHistorianSettings s = validSettings();
        s.setCollectorUUID("   ");
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    // --- host and port ---

    @Test
    void hostAndPortComeFromTheTokenWithoutOverrides() {
        FactryHistorianSettings s = validSettings();
        assertEquals("historian.example.com", s.getHost());
        assertEquals(8001, s.getPort());
    }

    @Test
    void overridesWinOverTheToken() {
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.setHostOverride("127.0.0.1");
        s.setPortOverride(8101);
        s.applyTokenDefaults(jwt("http://localhost", "8001"));
        assertEquals("127.0.0.1", s.getHost());
        assertEquals(8101, s.getPort());
        assertEquals("127.0.0.1", s.getHostOverride());
    }

    /** Before, the first token's host was kept as if it were an override. */
    @Test
    void aNewTokenMovesHostAndPortWhenNotOverridden() {
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.applyTokenDefaults(jwt("http://old.example.com", "8001"));
        s.applyTokenDefaults(jwt("http://new.example.com", "9001"));
        assertEquals("new.example.com", s.getHost());
        assertEquals(9001, s.getPort());
        assertEquals("", s.getHostOverride());
        assertEquals(0, s.getPortOverride());
    }

    @Test
    void noPortOverrideShowsAsZeroInTheForm() throws Exception {
        // Ignition's form refuses a cleared number field, so "use the token's port" must be 0, not empty.
        assertEquals(0, FactryHistorianConfig.fromSettings(new FactryHistorianSettings()).portOverride());
        var defaultValue = FactryHistorianConfig.class.getDeclaredField("portOverride").getAnnotation(
                com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.DefaultValue.class);
        assertEquals("0", defaultValue.value());
    }

    @Test
    void emptyPortOverrideMeansTheTokensPort() {
        assertEquals(8001, withPortOverride(null).getPort());
        assertEquals(8001, withPortOverride(0).getPort());
    }

    @Test
    void validate_portOverrideOutOfRange() {
        for (int port : new int[]{-1, 65536}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, withPortOverride(port)::validate);
            assertTrue(e.getMessage().contains("Port"));
        }
    }

    @Test
    void validate_portOverrideBoundaries() {
        assertDoesNotThrow(withPortOverride(1)::validate);
        assertDoesNotThrow(withPortOverride(65535)::validate);
    }

    @Test
    void theTokensCertificateClaimIsRead() {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String payload = "{\"uuid\":\"u\",\"name\":\"n\",\"aud\":\"http://localhost\",\"grpc-port\":\"8001\","
                + "\"use-self-signed-certificate\":true}";
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.applyTokenDefaults("e30." + enc.encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig");
        assertTrue(s.usesFactryCertificate());
        assertFalse(validSettings().usesFactryCertificate());
    }

    // --- custom CA certificate ---

    @Test
    void validate_blankCustomCaCert_ok() {
        FactryHistorianSettings s = validSettings();
        s.setCustomCaCert("");
        assertDoesNotThrow(s::validate);
    }

    @Test
    void validate_invalidCustomCaCert_fails() {
        FactryHistorianSettings s = validSettings();
        s.setCustomCaCert("-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, s::validate);
        assertTrue(e.getMessage().toLowerCase().contains("custom ca certificate"), e.getMessage());
    }

    // --- token ---

    @Test
    void applyTokenDefaults_tokenThatIsNotAJwt_failsWithoutShowingTheToken() {
        for (String token : new String[]{"eyJhbGciOi\"quoted\".eyJ1dWlkIjoi.secretPart", "onlyOnePart", "a.b", "a.b.c.d"}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> new FactryHistorianSettings().applyTokenDefaults(token));
            assertTrue(e.getMessage().startsWith("Invalid token: not a valid JWT"));
            assertFalse(e.getMessage().contains("secretPart") || e.getMessage().contains("onlyOnePart"));
        }
    }

    @Test
    void applyTokenDefaults_blankToken_fails() {
        for (String token : new String[]{null, "", " \n "}) {
            assertThrows(IllegalArgumentException.class, () -> new FactryHistorianSettings().applyTokenDefaults(token));
        }
    }

    @Test
    void toString_leavesTheTokenOut() {
        FactryHistorianSettings s = validSettings();
        s.setLegacyToken("header.payload.signature");
        assertFalse(s.toString().contains("signature"));
    }
}
