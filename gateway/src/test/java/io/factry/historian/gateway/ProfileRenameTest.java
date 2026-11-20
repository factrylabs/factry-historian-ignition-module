package io.factry.historian.gateway;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.ignition.common.resourcecollection.ChangeOperation;
import com.inductiveautomation.historian.gateway.api.HistorianExtensionPoint;
import com.inductiveautomation.ignition.common.gson.JsonObject;
import com.inductiveautomation.ignition.common.resourcecollection.Resource;
import com.inductiveautomation.ignition.common.resourcecollection.ResourceBuilder;
import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import com.inductiveautomation.ignition.gateway.config.ConfigurationManager;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.storeforward.StoreAndForwardManager;
import com.inductiveautomation.ignition.gateway.storeforward.engine.EngineInformation;
import com.inductiveautomation.ignition.gateway.storeforward.engine.EngineStatus;
import com.inductiveautomation.ignition.gateway.storeforward.resource.StoreAndForwardEngineSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Renaming a profile leaves the data store and forward still holds for it in the engine named
 * after the old name (#36). The module keeps the ordinary engine (a third-party one would drop
 * tag data whenever the profile is stopped), so it must say what is left behind and what to do.
 */
class ProfileRenameTest {

    private final List<Resource> engines = new ArrayList<>();
    private final java.util.Map<String, Resource> profiles = new java.util.HashMap<>();
    private int pending = 7;
    private int quarantined = 2;
    private FactryHistoryProvider provider;

    @AfterEach
    void tearDown() {
        if (provider != null) {
            provider.shutdown();
        }
    }

    @Test
    void renameWarnsWhatTheOldEngineHolds() throws Exception {
        provider = started("factry-r");

        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            assertTrue(provider.handleNameChange("factry-r2"), "keeps running, and forwarding from the old engine");

            assertTrue(log.events().stream().anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("Profile renamed to 'factry-r2'")
                    && e.getFormattedMessage().contains("engine 'factry-r', which holds 7 pending and 2 quarantined")
                    && e.getFormattedMessage().contains("renamed back to 'factry-r'")), log.text());
        }
    }

    @Test
    void renameOfAnEmptyEngineIsNoWarning() throws Exception {
        pending = 0;
        quarantined = 0;
        provider = started("factry-r");

        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            provider.handleNameChange("factry-r2");

            assertTrue(log.text().contains("Store and forward engine 'factry-r' is empty"), log.text());
            assertTrue(log.events().stream().noneMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("renamed")), log.text());
        }
    }

    @Test
    void renameInLetterCaseOnlyKeepsTheEngine() throws Exception {
        provider = started("factry-r");

        try (LogCapture log = new LogCapture(FactryHistoryProvider.class.getName(), Level.INFO)) {
            provider.handleNameChange("Factry-R");

            assertTrue(log.text().contains("keeps store and forward engine 'factry-r'"), log.text());
            assertTrue(log.events().stream().noneMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("renamed")), log.text());
        }
    }

    @Test
    void theEngineStaysAnOrdinaryOne() throws Exception {
        provider = started("factry-r");

        assertEquals(1, engines.size());
        assertFalse(StoreAndForwardEngineSettings.getMeta().getCodec().decode(engines.get(0)).config().isThirdParty(),
                "a third-party engine stops with the profile and Ignition then drops tag data sent to it");
    }

    /**
     * Ignition (8.3.3 to 8.3.6) calls handleSettingsChange before handleNameChange on every save,
     * a rename-only save too. A false there restarts the profile under the new name at once, and
     * the backlog in the old engine is left behind: exactly what the rename warning is about.
     */
    @Test
    void aRenameOnlySaveWhileFactryIsDownDoesNotRestartTheProfile() throws Exception {
        provider = started("factry-r");

        assertTrue(provider.handleSettingsChange(unreachableFactry()), "same settings: nothing to apply");
        assertTrue(provider.handleNameChange("factry-r2"));
    }

    @Test
    void aSettingsSaveWhileFactryIsDownKeepsTheProfileRunning() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings changed = unreachableFactry();
        changed.setDelimiter("/");

        assertTrue(provider.handleSettingsChange(changed), "a cache refresh that fails is retried by the schedule");
        assertEquals("/", provider.getSettings().getDelimiter());
    }

    @Test
    void aNewFactryThatCannotBeReadStillRestartsTheProfile() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings elsewhere = unreachableFactry();
        elsewhere.setPortOverride(2);

        assertFalse(provider.handleSettingsChange(elsewhere),
                "the cache still holds the old Factry's measurements; a fresh start does not");
    }

    /**
     * Ignition 8.3.8 and newer (historian 1.3.8) call only handleNameChange on a save that renames,
     * so settings changed in the same save must be picked up there.
     */
    @Test
    void settingsChangedInTheSameSaveAsARenameAreApplied() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings saved = unreachableFactry();
        saved.setDelimiter("/");
        storedProfile("factry-r2", saved);

        assertTrue(provider.handleNameChange("factry-r2"));

        assertEquals("/", provider.getSettings().getDelimiter());
    }

    @Test
    void aRenameWithTheSameSettingsAppliesNothing() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings running = provider.getSettings();
        storedProfile("factry-r2", unreachableFactry());

        assertTrue(provider.handleNameChange("factry-r2"));

        assertSame(running, provider.getSettings());
    }

    @Test
    void aNewTokenForTheSameCollectorWhileFactryIsDownIsApplied() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings newToken = unreachableFactry();
        newToken.setCollectorToken(FakeSecrets.embedded(TOKEN_2));

        assertTrue(provider.handleSettingsChange(newToken));

        assertSame(newToken, provider.getSettings());
        assertEquals(TOKEN_2, provider.connection().token(), "the client uses the new token");
        assertEquals(ProfileStatus.State.Errored, provider.getStatus().getState(), "Factry is still down");
    }

    /** The same reference, but the secret provider now holds another token: not "unchanged". */
    @Test
    void aSaveAfterTheSecretChangedIsApplied() throws Exception {
        provider = started("factry-r");
        secrets.providers.get("vault").put("factry", TOKEN_2);

        assertTrue(provider.handleSettingsChange(unreachableFactry()));

        assertEquals(TOKEN_2, provider.connection().token());
    }

    @Test
    void aTlsChangeForTheSameCollectorWhileFactryIsDownIsApplied() throws Exception {
        provider = started("factry-r");
        FactryHistorianSettings tls = unreachableFactry();
        tls.setSkipTlsVerification(true);

        assertTrue(provider.handleSettingsChange(tls));

        assertSame(tls, provider.getSettings());
        assertTrue(provider.connection().skipTlsVerification());
        assertEquals(ProfileStatus.State.Errored, provider.getStatus().getState());
    }

    /** The 8.3.8 order, with the token read from its secret provider as at a start. */
    @Test
    void aRenameSaveReadsTheTokenFromItsSecretProvider() throws Exception {
        provider = started("factry-r");
        secrets.providers.get("vault").put("factry", TOKEN_2);
        FactryHistorianSettings saved = unreachableFactry();
        saved.setDelimiter("/");
        storedProfile("factry-r2", saved);

        assertTrue(provider.handleNameChange("factry-r2"));

        assertEquals("/", provider.getSettings().getDelimiter());
        assertEquals(TOKEN_2, provider.connection().token());
    }

    /** A profile started the way Ignition starts it; Factry is not reachable, which is fine here. */
    private FactryHistoryProvider started(String name) throws Exception {
        FactryHistoryProvider p = new FactryHistoryProvider(gatewayContext(), name, unreachableFactry());
        p.startup();
        return p;
    }

    /** The profile resource as Ignition saves it. */
    private void storedProfile(String name, FactryHistorianSettings settings) {
        JsonObject config = new JsonObject();
        JsonObject profile = new JsonObject();
        profile.addProperty("type", FactryHistorianExtensionPoint.TYPE_ID);
        profile.addProperty("enabled", true);
        config.add("profile", profile);
        config.add("settings", new FactryHistorianExtensionPoint().encode(settings));
        ResourceBuilder builder = Resource.newBuilder();
        builder.setResourceCollectionName("core");
        builder.setResourcePath(HistorianExtensionPoint.RESOURCE_TYPE.childPath(name));
        builder.setApplicationScope("G");
        builder.putData("config.json", config.toString());
        profiles.put(name, builder.build());
    }

    /** Collector tokens for one collector at a closed port: the second is a new one for it. */
    private static final String TOKEN = token("sig");
    private static final String TOKEN_2 = token("sig2");

    private static String token(String signature) {
        String payload = "{\"uuid\":\"collector-uuid\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"1\"}";
        return "e30." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + "." + signature;
    }

    private final FakeSecrets secrets = new FakeSecrets();
    private final com.inductiveautomation.ignition.gateway.secrets.SecretConfig vaultToken =
            secrets.referenced("vault", "factry", TOKEN);

    /** Settings as saved: the token read from a secret provider. */
    private FactryHistorianSettings unreachableFactry() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setCollectorToken(vaultToken);
        return settings;
    }

    private GatewayContext gatewayContext() {
        MetricRegistry registry = new MetricRegistry();
        GatewayContext secretsContext = secrets.gatewayContext();
        StoreAndForwardManager storeAndForward = storeAndForwardManager();
        ConfigurationManager configuration = configurationManager();
        return (GatewayContext) Proxy.newProxyInstance(GatewayContext.class.getClassLoader(),
                new Class<?>[]{GatewayContext.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getMetricRegistry" -> registry;
                    case "getStoreAndForwardManager" -> storeAndForward;
                    case "getConfigurationManager" -> configuration;
                    case "getSystemEncryptionService" -> secretsContext.getSystemEncryptionService();
                    case "getSecretProviderManager" -> secretsContext.getSecretProviderManager();
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Engines are found ignoring case, as Ignition's manager does. */
    private StoreAndForwardManager storeAndForwardManager() {
        return (StoreAndForwardManager) Proxy.newProxyInstance(StoreAndForwardManager.class.getClassLoader(),
                new Class<?>[]{StoreAndForwardManager.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getEngineInformation" -> engines.stream()
                            .filter(r -> r.getResourceName().equalsIgnoreCase((String) args[0]))
                            .findFirst()
                            .map(r -> EngineInformation.newBuilder().engineId(r.getResourceName())
                                    .engineStatus(EngineStatus.fullyOperationStatus())
                                    .storeMetric(new Meter()).forwardMetric(new Meter())
                                    .totalPending(pending).totalQuarantined(quarantined).build());
                    case "registerSink", "unregisterSink" -> null;
                    case "getQuarantineInterface" -> Optional.empty();
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    /** Saves what is pushed, like Ignition does before its listeners run. */
    private ConfigurationManager configurationManager() {
        return (ConfigurationManager) Proxy.newProxyInstance(ConfigurationManager.class.getClassLoader(),
                new Class<?>[]{ConfigurationManager.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getResources" -> new ArrayList<>(engines);
                    case "getNamedResource" -> Optional.ofNullable(profiles.get((String) args[1]));
                    case "push" -> {
                        for (Object o : (List<?>) args[0]) {
                            engines.add(ChangeOperation.getResourceFromChange((ChangeOperation) o));
                        }
                        yield CompletableFuture.completedFuture(null);
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
