package io.factry.historian.gateway;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.inductiveautomation.ignition.common.gson.JsonElement;
import com.inductiveautomation.ignition.common.gson.JsonObject;
import com.inductiveautomation.ignition.gateway.config.ValidationErrors;
import com.inductiveautomation.ignition.gateway.secrets.SecretConfig;
import io.factry.historian.gateway.IgnitionHistoryQuery.Dataset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The collector token is a secret (#14): Embedded (encrypted with the gateway's key) or Referenced
 * (read from a secret provider), never stored or shown in clear. Profiles saved before keep their
 * plain {@code token} working until the next save, which removes it.
 */
class TokenSecretTest {

    private static final long START = 1_700_000_000_000L;

    private final FakeSecrets secrets = new FakeSecrets();
    private FakeFactryServer factry;
    private String token;
    private FactryHistoryProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        factry = new FakeFactryServer();
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);
        token = jwt(factry.port(), "sig-one");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (provider != null) {
            provider.onShutdown();
        }
        factry.close();
    }

    @Test
    void embeddedTokenConnects() throws Exception {
        provider = start(saved("collectorToken", FakeSecrets.embedded(token).toJsonTree()));

        assertReads();
        assertOnlySent(token);
    }

    @Test
    void referencedTokenConnects() throws Exception {
        provider = start(saved("collectorToken", secrets.referenced("vault", "factry", token + "\n").toJsonTree()));

        assertReads();
        assertOnlySent(token);
    }

    @Test
    void plainTokenOfAnOlderProfileStillConnectsWithOneWarning() throws Exception {
        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            provider = start(saved("token", new com.inductiveautomation.ignition.common.gson.JsonPrimitive(token)));

            List<ILoggingEvent> warnings = log.events().stream().filter(e -> e.getLevel() == Level.WARN).toList();
            assertEquals(1, warnings.size(), log.text());
            assertTrue(warnings.get(0).getFormattedMessage().contains("plain text"));
            assertFalse(log.text().contains("sig-one"), "the log must not contain the token");
        }
        assertReads();
        assertOnlySent(token);
    }

    @Test
    void savingWithTheSecretRemovesThePlainCopy() throws Exception {
        // What the form sends for an older profile once the token is entered as a secret: the old
        // key may come along.
        JsonObject posted = new JsonObject();
        posted.addProperty("token", token);
        posted.add("collectorToken", FakeSecrets.embedded(token).toJsonTree());

        FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
        JsonElement stored = ep.encode(ep.decode(posted));

        assertFalse(stored.getAsJsonObject().has("token"), "the plain token must not be stored again");
        assertEquals("Embedded", stored.getAsJsonObject().getAsJsonObject("collectorToken").get("type").getAsString());
        assertFalse(stored.toString().contains("sig-one"));
    }

    @Test
    void tokenRotatedInTheSecretProviderIsPickedUpWhenFactryRefusesTheOldOne() throws Exception {
        provider = start(saved("collectorToken", secrets.referenced("vault", "factry", token).toJsonTree()));
        String rotated = jwt(factry.port(), "sig-two");

        // Factry revokes the old token; the operator puts the new one in the provider.
        factry.acceptedToken = rotated;
        secrets.providers.get("vault").put("factry", rotated);
        provider.checkConnection();

        factry.authorizations.clear();
        assertReads();
        assertOnlySent(rotated);
    }

    /** An older profile with a blank or bad plain token still loads, and shows Errored when it starts. */
    @Test
    void olderProfileWithABlankOrBadPlainTokenStillLoads() throws Exception {
        for (String plain : new String[]{"", "not a token"}) {
            JsonObject settings = new JsonObject();
            settings.addProperty("token", plain);
            assertTrue(validate(settings).isEmpty(), "'" + plain + "'");
        }
        assertTrue(validate(new JsonObject()).isEmpty());
    }

    /**
     * The value in the provider is a JWT but no collector token. Before, the reload took it, failed
     * in applyTokenDefaults with an ERROR, and skipped the status and the sink pause, every 30 s.
     */
    @Test
    void reloadedValueThatIsNotACollectorTokenLeavesTheRefusalVisible() throws Exception {
        provider = start(saved("collectorToken", secrets.referenced("vault", "factry", token).toJsonTree()));
        factry.acceptedToken = "revoked-everything";
        secrets.providers.get("vault").put("factry", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln");

        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            provider.checkConnection();
            provider.checkConnection();

            assertEquals("Factry refused the collector token", provider.statusOnceStarted().getMessage().toString());
            assertTrue(log.events().stream().noneMatch(e -> e.getLevel() == Level.ERROR), log.text());
            assertFalse(log.text().contains("now holds another one"), log.text());
            assertEquals(1, log.events().stream().filter(e -> e.getFormattedMessage().contains("does not hold a usable"))
                    .count(), log.text());
            assertFalse(log.text().contains("c2ln"), "the log must not contain the value");
        }
    }

    /** A rotated token that names another Factry (host or port) is followed, not the first one's. */
    @Test
    void reloadedTokenForAnotherPortIsFollowed() throws Exception {
        try (FakeFactryServer other = new FakeFactryServer()) {
            String uuid = other.addMeasurement("default/a", "number");
            other.addPoint(uuid, START, 1.0);
            provider = start(saved("collectorToken", secrets.referenced("vault", "factry", token).toJsonTree()));
            String forOther = jwt(other.port(), "sig-other");

            factry.acceptedToken = forOther;
            secrets.providers.get("vault").put("factry", forOther);
            provider.checkConnection();

            assertReads();
            assertFalse(other.authorizations.isEmpty(), "reads must go to the Factry the new token names");
        }
    }

    /**
     * A provider of another module, or a remote one, may not be ready when the gateway starts the
     * profile. Before, the constructor threw and nothing tried again.
     */
    @Test
    void providerNotReadyAtStartIsReadOnALaterCheck() throws Exception {
        provider = start(saved("collectorToken", SecretConfig.referenced("later", "factry").toJsonTree()));

        provider.checkConnection();
        var status = provider.statusOnceStarted();
        assertEquals("Errored", status.getState().name());
        assertEquals("Collector token cannot be read", status.getMessage().toString());
        assertTrue(status.getErrorDetails().contains("'later'") && status.getErrorDetails().contains("'factry'"),
                status.getErrorDetails());

        secrets.referenced("later", "factry", token);
        provider.checkConnection();

        assertEquals(com.inductiveautomation.ignition.gateway.model.ProfileStatus.RUNNING, provider.statusOnceStarted());
        assertReads();
        assertOnlySent(token);
    }

    /** A late provider that holds no collector token: one WARN, not one per 30 s check. */
    @Test
    void lateProviderWithAValueThatIsNotACollectorTokenWarnsOnce() throws Exception {
        provider = start(saved("collectorToken", SecretConfig.referenced("later", "factry").toJsonTree()));
        secrets.referenced("later", "factry", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln");

        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            for (int i = 0; i < 5; i++) {
                provider.checkConnection();
            }
            assertEquals(1, log.events().stream().filter(e -> e.getLevel() == Level.WARN).count(), log.text());
        }
        String details = provider.statusOnceStarted().getErrorDetails();
        assertTrue(details.contains("not usable"), details);
        assertFalse(details.contains(".."), details);
    }

    /** onStartup skips the registration while the token cannot be read; the first read does it. */
    @Test
    void lateReadRegistersTheCollector() throws Exception {
        provider = start(saved("collectorToken", SecretConfig.referenced("later", "factry").toJsonTree()));
        provider.checkConnection();
        assertTrue(factry.registrations.isEmpty());

        secrets.referenced("later", "factry", token);
        provider.checkConnection();

        assertEquals(List.of("register", "Active"), factry.registrations);
    }

    /** Saving a token that cannot be read must not keep reading the previous Factry with the previous token. */
    @Test
    void saveWithAnUnreadableTokenStopsUsingThePreviousOne() throws Exception {
        provider = start(saved("collectorToken", FakeSecrets.embedded(token).toJsonTree()));
        assertReads();

        assertTrue(provider.handleSettingsChange(saved("collectorToken",
                SecretConfig.referenced("gone", "factry").toJsonTree())));
        factry.authorizations.clear();

        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertTrue(factry.authorizations.isEmpty(), "nothing may reach Factry without a token");
        assertEquals("Collector token cannot be read", provider.statusOnceStarted().getMessage().toString());
    }

    /**
     * The REST call for the default database (#78) carries the same cleaned token as gRPC, also
     * after a reload from the secret provider, and the token never reaches the log.
     */
    @Test
    void restCallUsesTheCleanTokenAlsoAfterAReload() throws Exception {
        List<String> restAuth = new java.util.concurrent.CopyOnWriteArrayList<>();
        com.sun.net.httpserver.HttpServer rest = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0), 0);
        rest.createContext("/", exchange -> {
            restAuth.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            byte[] answer = "{\"DefaultDatabaseUUID\":\"db-a\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, answer.length);
            exchange.getResponseBody().write(answer);
            exchange.close();
        });
        rest.start();
        // The module's own loggers; the fake server's Netty handler logs its headers at DEBUG.
        try (LogCapture log = new LogCapture("io.factry", Level.DEBUG)) {
            factry.database = "db-a";
            String first = jwt(factry.port(), rest.getAddress().getPort(), "sig-rest-one");
            provider = start(saved("collectorToken", secrets.referenced("vault", "factry", first + "\n").toJsonTree()));

            String second = jwt(factry.port(), rest.getAddress().getPort(), "sig-rest-two");
            factry.acceptedToken = second;
            secrets.providers.get("vault").put("factry", second);
            restAuth.clear();
            provider.checkConnection();

            assertFalse(restAuth.isEmpty());
            assertTrue(restAuth.stream().allMatch(("Bearer " + second)::equals), "the REST call must use the new token");
            assertFalse(log.text().contains("sig-rest"), "the log must not contain the token");
        } finally {
            rest.stop(0);
        }
    }

    /** SchemaUtil warns about any field named like a secret that is not a SecretConfig. */
    @Test
    void legacyTokenFieldGivesNoSecretWarning() {
        try (LogCapture log = new LogCapture("OpenAPI.SchemaUtil", Level.WARN)) {
            new FactryHistorianExtensionPoint().settingsSchema();
            assertFalse(log.text().contains("'token'"), log.text());
        }
    }

    @Test
    void saveRefusesAnEmbeddedValueThatIsNotACollectorToken() throws Exception {
        JsonObject settings = new JsonObject();
        settings.add("collectorToken", FakeSecrets.embedded("not-a-token secretValue").toJsonTree());

        Optional<ValidationErrors> errors = validate(settings);

        assertEquals(List.of("collectorToken"), fieldsInError(settings));
        assertFalse(errors.orElseThrow().toString().contains("secretValue"), "the message must not show the token");
    }

    @Test
    void saveAcceptsAReferencedTokenWithoutCallingTheProvider() throws Exception {
        JsonObject settings = new JsonObject();
        settings.add("collectorToken", SecretConfig.referenced("nowhere", "factry").toJsonTree());

        assertTrue(validate(settings).isEmpty());
    }

    private Optional<ValidationErrors> validate(JsonObject settings) throws Exception {
        FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
        ep.setGatewayContext(secrets.gatewayContext());
        return ep.getSettingsValidator().orElseThrow().validate(ep.decode(settings));
    }

    private List<String> fieldsInError(JsonObject settings) throws Exception {
        return validate(settings).orElseThrow().fieldMessages().stream()
                .map(ValidationErrors.FieldValidationErrors::fieldName).toList();
    }

    private FactryHistoryProvider start(FactryHistorianSettings settings) {
        return new FactryHistoryProvider(secrets.gatewayContext(), "Factry", settings);
    }

    private void assertReads() throws Exception {
        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
    }

    private void assertOnlySent(String expected) {
        assertFalse(factry.authorizations.isEmpty());
        // Compare without printing: a failure message must not carry the token.
        for (String sent : factry.authorizations) {
            assertTrue(sent.equals("Bearer " + expected), "the authorization header is not the expected token");
        }
    }

    /** Settings as Ignition decodes them from the saved profile. */
    private static FactryHistorianSettings saved(String key, JsonElement tokenValue) throws Exception {
        JsonObject config = new JsonObject();
        config.add(key, tokenValue);
        config.addProperty("delimiter", "/");
        return new FactryHistorianExtensionPoint().decode(config);
    }

    /** A collector token that also names Factry's REST port and organisation. */
    private static String jwt(int port, int restPort, String signature) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector\",\"name\":\"Ignition\",\"organization-uuid\":\"org-1\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"" + port + "\",\"rest-port\":\"" + restPort + "\"}")
                .getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + "." + signature;
    }

    /** A collector token for the fake Factry; the signature is not checked. */
    private static String jwt(int port, String signature) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"" + port + "\"}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + "." + signature;
    }
}
