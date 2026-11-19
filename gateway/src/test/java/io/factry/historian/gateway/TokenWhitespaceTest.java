package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.gson.JsonObject;
import io.factry.historian.gateway.IgnitionHistoryQuery.Dataset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A token copied from Factry's web UI or an e-mail often comes with a trailing newline or space,
 * or wrapped over two lines. Factry refuses such a header (UNAUTHENTICATED), so the profile saved
 * fine and then never connected. The module sends the token without the whitespace.
 */
class TokenWhitespaceTest {

    private static final long START = 1_700_000_000_000L;

    private FakeFactryServer factry;
    private String token;
    private FactryHistoryProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        factry = new FakeFactryServer();
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);
        token = jwt(factry.port());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (provider != null) {
            provider.onShutdown();
        }
        factry.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"\n", " ", "\r\n", "\t"})
    void newProfileSendsTheTokenWithoutTrailingWhitespace(String suffix) throws Exception {
        provider = new FactryHistoryProvider(FakeFactryServer.gatewayContext(), "Factry", saved(token + suffix));

        Dataset ds = query();

        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
        assertOnlyCleanTokenSent();
    }

    @ParameterizedTest
    @ValueSource(strings = {"\n", "\r\n", " "})
    void editedTokenWrappedOverTwoLinesIsSentInOnePiece(String lineBreak) throws Exception {
        provider = new FactryHistoryProvider(FakeFactryServer.gatewayContext(), "Factry", saved(token));
        int half = token.indexOf('.') + 5;

        assertTrue(provider.handleSettingsChange(saved(token.substring(0, half) + lineBreak + token.substring(half))));

        assertFalse(factry.authorizations.isEmpty());
        assertOnlyCleanTokenSent();
    }

    private void assertOnlyCleanTokenSent() {
        // Compare without printing: a failure message must not carry the token.
        for (String sent : factry.authorizations) {
            assertTrue(sent.equals("Bearer " + token), "the authorization header is not the clean token");
        }
    }

    private Dataset query() throws Exception {
        return IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
    }

    /** Settings as Ignition decodes them from the saved profile. */
    private static FactryHistorianSettings saved(String pastedToken) throws Exception {
        JsonObject config = new JsonObject();
        config.addProperty("token", pastedToken);
        config.addProperty("delimiter", "/");
        return new FactryHistorianExtensionPoint().decode(config);
    }

    /** A collector token for the fake Factry; the signature is not checked. */
    private static String jwt(int port) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"" + port + "\"}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".fake-signature_x";
    }
}
