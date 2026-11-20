package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.resourcecollection.ChangeOperation;
import com.inductiveautomation.ignition.common.resourcecollection.Resource;
import com.inductiveautomation.ignition.gateway.config.ConfigurationManager;
import com.inductiveautomation.ignition.gateway.storeforward.StoreAndForwardManager;
import io.factry.historian.gateway.FactryHistoryProvider.EngineCreation;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A new profile creates its store and forward engine on first start. Ignition writes the engine
 * resource inside push(), but the returned future also waits for the change listeners, which can
 * queue behind the profile's own startup (#16). The engine is there, so that is no error.
 */
class StoreAndForwardEngineCreationTest {

    private final List<Resource> resources = new ArrayList<>();

    /** push() saves the resource (unless told not to) and returns {@code answer}. */
    private ConfigurationManager configurationManager(boolean saves, CompletableFuture<Void> answer) {
        return (ConfigurationManager) Proxy.newProxyInstance(ConfigurationManager.class.getClassLoader(),
                new Class<?>[]{ConfigurationManager.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getResources":
                            return new ArrayList<>(resources);
                        case "push":
                            if (saves) {
                                for (Object op : (List<?>) args[0]) {
                                    resources.add(ChangeOperation.getResourceFromChange((ChangeOperation) op));
                                }
                            }
                            return answer;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    private static StoreAndForwardManager noEnginesRunning() {
        return (StoreAndForwardManager) Proxy.newProxyInstance(StoreAndForwardManager.class.getClassLoader(),
                new Class<?>[]{StoreAndForwardManager.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getEngineInformation")) {
                        return Optional.empty();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private record Run(EngineCreation outcome, String log) {}

    private Run ensure(ConfigurationManager configurationManager) {
        // slf4j-simple writes to whatever System.err is at the time of the call.
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            EngineCreation outcome = FactryHistoryProvider.ensureStoreAndForwardEngine(
                    configurationManager, noEnginesRunning(), "factry-b", 200);
            return new Run(outcome, captured.toString());
        } finally {
            System.setErr(original);
        }
    }

    @Test
    void engineSavedButStillBeingAppliedIsNoError() {
        Run run = ensure(configurationManager(true, new CompletableFuture<>()));

        assertEquals(EngineCreation.CREATED_PENDING, run.outcome());
        assertFalse(run.log().contains("ERROR"), run.log());
        assertTrue(run.log().contains("S&F engine 'factry-b' created; Ignition is still applying it"), run.log());
    }

    @Test
    void engineMissingAfterTheTimeoutIsStillAnError() {
        Run run = ensure(configurationManager(false, new CompletableFuture<>()));

        assertEquals(EngineCreation.FAILED, run.outcome());
        assertTrue(run.log().contains("ERROR"), run.log());
        assertTrue(run.log().contains("Failed to create S&F engine 'factry-b'"), run.log());
    }

    @Test
    void refusedPushIsAnError() {
        Run run = ensure(configurationManager(false,
                CompletableFuture.failedFuture(new IllegalStateException("refused"))));

        assertEquals(EngineCreation.FAILED, run.outcome());
        assertTrue(run.log().contains("Failed to create S&F engine 'factry-b'"), run.log());
    }

    @Test
    void appliedEngineIsCreated() {
        Run run = ensure(configurationManager(true, CompletableFuture.completedFuture(null)));

        assertEquals(EngineCreation.CREATED, run.outcome());
        assertEquals(1, resources.size());
        assertFalse(run.log().contains("ERROR"), run.log());
    }

    @Test
    void existingEngineResourceIsLeftAlone() {
        ensure(configurationManager(true, CompletableFuture.completedFuture(null)));

        Run second = ensure(configurationManager(true, CompletableFuture.completedFuture(null)));

        assertEquals(EngineCreation.EXISTS, second.outcome());
        assertEquals(1, resources.size());
    }
}
