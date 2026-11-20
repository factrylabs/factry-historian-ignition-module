package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JwtTokenParserTest {

    // -- parsePayload ---------------------------------------------------------

    @Test
    void parsePayload_validToken_returnsJsonObject() {
        String token = buildToken("{\"uuid\":\"abc-123\",\"aud\":\"http://historian\",\"grpc-port\":\"8001\"}");
        Optional<JsonObject> result = JwtTokenParser.parsePayload(token);
        assertTrue(result.isPresent());
        assertEquals("abc-123", result.get().get("uuid").getAsString());
    }

    @Test
    void parsePayload_nullToken_returnsEmpty() {
        assertTrue(JwtTokenParser.parsePayload(null).isEmpty());
    }

    @Test
    void parsePayload_emptyToken_returnsEmpty() {
        assertTrue(JwtTokenParser.parsePayload("").isEmpty());
    }

    @Test
    void parsePayload_blankToken_returnsEmpty() {
        assertTrue(JwtTokenParser.parsePayload("   ").isEmpty());
    }

    @Test
    void parsePayload_noDotSeparator_returnsEmpty() {
        assertTrue(JwtTokenParser.parsePayload("notajwt").isEmpty());
    }

    @Test
    void parsePayload_invalidBase64_returnsEmpty() {
        assertTrue(JwtTokenParser.parsePayload("header.!!!invalid!!!.signature").isEmpty());
    }

    @Test
    void parsePayload_tokenWithTwoParts_works() {
        String payload = base64Encode("{\"uuid\":\"two-part\"}");
        String token = "header." + payload;
        Optional<JsonObject> result = JwtTokenParser.parsePayload(token);
        assertTrue(result.isPresent());
        assertEquals("two-part", result.get().get("uuid").getAsString());
    }

    // -- getCollectorUUID -----------------------------------------------------

    @Test
    void getCollectorUUID_present_returnsValue() {
        JsonObject payload = parseJson("{\"uuid\":\"collector-uuid-123\"}");
        assertEquals(Optional.of("collector-uuid-123"), JwtTokenParser.getCollectorUUID(payload));
    }

    @Test
    void getCollectorUUID_missing_returnsEmpty() {
        JsonObject payload = parseJson("{\"aud\":\"http://historian\"}");
        assertTrue(JwtTokenParser.getCollectorUUID(payload).isEmpty());
    }

    @Test
    void getCollectorUUID_blank_returnsEmpty() {
        JsonObject payload = parseJson("{\"uuid\":\"  \"}");
        assertTrue(JwtTokenParser.getCollectorUUID(payload).isEmpty());
    }

    @Test
    void getCollectorUUID_nullPayload_returnsEmpty() {
        assertTrue(JwtTokenParser.getCollectorUUID(null).isEmpty());
    }

    // -- getCollectorName -----------------------------------------------------

    @Test
    void getCollectorName_present_returnsValue() {
        JsonObject payload = parseJson("{\"name\":\"Ignition\"}");
        assertEquals(Optional.of("Ignition"), JwtTokenParser.getCollectorName(payload));
    }

    @Test
    void getCollectorName_missing_returnsEmpty() {
        JsonObject payload = parseJson("{\"uuid\":\"x\"}");
        assertTrue(JwtTokenParser.getCollectorName(payload).isEmpty());
    }

    @Test
    void getCollectorName_blank_returnsEmpty() {
        JsonObject payload = parseJson("{\"name\":\"  \"}");
        assertTrue(JwtTokenParser.getCollectorName(payload).isEmpty());
    }

    // -- getHost --------------------------------------------------------------

    @Test
    void getHost_httpUrl_extractsHostname() {
        JsonObject payload = parseJson("{\"aud\":\"http://historian\"}");
        assertEquals(Optional.of("historian"), JwtTokenParser.getHost(payload));
    }

    @Test
    void getHost_httpsUrl_extractsHostname() {
        JsonObject payload = parseJson("{\"aud\":\"https://my-host.example.com\"}");
        assertEquals(Optional.of("my-host.example.com"), JwtTokenParser.getHost(payload));
    }

    @Test
    void getHost_plainHostname_returnsEmpty() {
        // URI.create("not-a-url").getHost() returns null → map produces empty
        JsonObject payload = parseJson("{\"aud\":\"not-a-url\"}");
        Optional<String> host = JwtTokenParser.getHost(payload);
        assertTrue(host.isEmpty());
    }

    @Test
    void getHost_missing_returnsEmpty() {
        JsonObject payload = parseJson("{\"uuid\":\"x\"}");
        assertTrue(JwtTokenParser.getHost(payload).isEmpty());
    }

    // -- getGrpcPort ----------------------------------------------------------

    @Test
    void getGrpcPort_validPort_returnsInteger() {
        JsonObject payload = parseJson("{\"grpc-port\":\"8001\"}");
        assertEquals(Optional.of(8001), JwtTokenParser.getGrpcPort(payload));
    }

    @Test
    void getGrpcPort_nonNumeric_returnsEmpty() {
        JsonObject payload = parseJson("{\"grpc-port\":\"notanumber\"}");
        assertTrue(JwtTokenParser.getGrpcPort(payload).isEmpty());
    }

    @Test
    void getGrpcPort_missing_returnsEmpty() {
        JsonObject payload = parseJson("{\"uuid\":\"x\"}");
        assertTrue(JwtTokenParser.getGrpcPort(payload).isEmpty());
    }

    // -- Helpers --------------------------------------------------------------

    private static String buildToken(String payloadJson) {
        String header = base64Encode("{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        String payload = base64Encode(payloadJson);
        return header + "." + payload + ".signature";
    }

    private static String base64Encode(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    // -- normalize --------------------------------------------------------------

    @Test
    void normalize_badCharacterOnlyInTheSignature_isRefused() {
        // Header and payload decode fine, so only the character check can catch this.
        String token = buildToken("{\"uuid\":\"abc-123\"}") + "\"x";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> JwtTokenParser.normalize(token));
        assertFalse(e.getMessage().contains("abc"));
    }

    @Test
    void normalize_invisibleCharactersFromACopy_areRemoved() {
        String token = buildToken("{\"uuid\":\"abc-123\"}");
        assertEquals(token, JwtTokenParser.normalize("\uFEFF" + token + "\u200B\n"));
    }

    private static JsonObject parseJson(String json) {
        return com.inductiveautomation.ignition.common.gson.JsonParser.parseString(json).getAsJsonObject();
    }
}
