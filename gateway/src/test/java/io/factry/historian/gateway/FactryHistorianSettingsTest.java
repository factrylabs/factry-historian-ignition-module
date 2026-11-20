package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FactryHistorianSettingsTest {

    private FactryHistorianSettings validSettings() {
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.setCollectorUUID("550e8400-e29b-41d4-a716-446655440000");
        s.setCollectorName("Ignition");
        s.setGrpcHost("historian.example.com");
        s.setGrpcPort(8001);
        return s;
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

    // --- grpcHost ---

    @Test
    void validate_nullHost() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcHost(null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, s::validate);
        assertTrue(e.getMessage().contains("Host"));
    }

    @Test
    void validate_emptyHost() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcHost("");
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    // --- grpcPort ---

    @Test
    void validate_portZero() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcPort(0);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, s::validate);
        assertTrue(e.getMessage().contains("Port"));
    }

    @Test
    void validate_portNegative() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcPort(-1);
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    @Test
    void validate_portTooHigh() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcPort(65536);
        assertThrows(IllegalArgumentException.class, s::validate);
    }

    @Test
    void validate_portBoundaryLow() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcPort(1);
        assertDoesNotThrow(s::validate);
    }

    @Test
    void validate_portBoundaryHigh() {
        FactryHistorianSettings s = validSettings();
        s.setGrpcPort(65535);
        assertDoesNotThrow(s::validate);
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
