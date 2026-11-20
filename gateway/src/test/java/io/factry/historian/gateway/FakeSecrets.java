package io.factry.historian.gateway;

import com.codahale.metrics.MetricRegistry;
import com.inductiveautomation.ignition.common.gson.JsonObject;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.secrets.ManagedSecretProvider;
import com.inductiveautomation.ignition.gateway.secrets.Plaintext;
import com.inductiveautomation.ignition.gateway.secrets.SecretConfig;
import com.inductiveautomation.ignition.gateway.secrets.SecretProviderManager;
import com.inductiveautomation.ignition.gateway.secrets.SystemEncryptionService;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The gateway's secrets, enough for Ignition's own {@code Secret.create(context, config)}: an
 * encryption service for Embedded secrets (not real encryption, the value is base64 under another
 * key) and secret providers for Referenced ones, whose values a test can change.
 */
final class FakeSecrets {

    /** Provider name, then secret name, then value. */
    final Map<String, Map<String, String>> providers = new ConcurrentHashMap<>();
    private final MetricRegistry registry = new MetricRegistry();

    static SecretConfig embedded(String plaintext) {
        JsonObject data = new JsonObject();
        data.addProperty("fake-ciphertext", Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8)));
        return SecretConfig.embedded(data);
    }

    SecretConfig referenced(String provider, String secret, String value) {
        providers.computeIfAbsent(provider, p -> new ConcurrentHashMap<>()).put(secret, value);
        return SecretConfig.referenced(provider, secret);
    }

    GatewayContext gatewayContext() {
        SystemEncryptionService encryption = new SystemEncryptionService() {
            @Override
            public com.inductiveautomation.ignition.common.gson.JsonElement encryptToJson(Plaintext plaintext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Plaintext decryptFromJson(com.inductiveautomation.ignition.common.gson.JsonElement json) {
                String value = json.getAsJsonObject().get("fake-ciphertext").getAsString();
                return Plaintext.fromBytes(Base64.getDecoder().decode(value));
            }
        };
        SecretProviderManager manager = (SecretProviderManager) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{SecretProviderManager.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getProvider")) {
                        Map<String, String> secrets = providers.get((String) args[0]);
                        return Optional.ofNullable(secrets == null ? null : provider(secrets));
                    }
                    return null;
                });
        return (GatewayContext) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{GatewayContext.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetricRegistry" -> registry;
                    case "getSystemEncryptionService" -> encryption;
                    case "getSecretProviderManager" -> manager;
                    default -> null;
                });
    }

    private ManagedSecretProvider provider(Map<String, String> secrets) {
        return (ManagedSecretProvider) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ManagedSecretProvider.class}, (proxy, method, args) -> {
                    if (method.getName().equals("read")) {
                        String value = secrets.get((String) args[0]);
                        if (value == null) {
                            throw new IllegalStateException("no secret " + args[0]);
                        }
                        return Plaintext.fromString(value);
                    }
                    return null;
                });
    }
}
