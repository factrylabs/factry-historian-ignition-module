package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import com.inductiveautomation.ignition.gateway.secrets.SecretConfig;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * "Unchanged settings" must mean unchanged in every field, also in fields added later, or a save
 * that changes only that field is ignored.
 */
class SettingsSameAsTest {

    private static FactryHistorianSettings settings() {
        String payload = "{\"uuid\":\"collector-uuid\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"1\"}";
        String token = "e30." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8)) + ".sig";
        FactryHistorianSettings s = new FactryHistorianSettings();
        s.setCollectorToken(FakeSecrets.embedded(token));
        s.applyTokenDefaults(token);
        return s;
    }

    @Test
    void equalSettingsAreTheSame() {
        assertTrue(settings().sameAs(settings()));
    }

    @Test
    void everyFieldCounts() throws Exception {
        for (Field field : FactryHistorianSettings.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
                continue;
            }
            field.setAccessible(true);
            FactryHistorianSettings changed = settings();
            field.set(changed, other(field.getType(), field.get(changed)));

            assertFalse(changed.sameAs(settings()), "a change of " + field.getName() + " is ignored");
            assertFalse(settings().sameAs(changed), "a change of " + field.getName() + " is ignored");
        }
    }

    private static Object other(Class<?> type, Object value) {
        if (type == String.class) {
            return value + "x";
        }
        if (type == int.class || type == Integer.class) {
            // A port override of 0 or none means "not set", so change it to a set one.
            return value == null || (Integer) value == 0 ? 1 : (Integer) value + 1;
        }
        if (type == SecretConfig.class) {
            return FakeSecrets.embedded("another token");
        }
        if (type == boolean.class) {
            return !(Boolean) value;
        }
        throw new AssertionError("no test value for a field of type " + type
                + ": extend this test and FactryHistorianSettings.sameAs");
    }
}
