package io.factry.historian.gateway;

import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import com.inductiveautomation.ignition.gateway.storeforward.StoreAndForwardManager;
import com.inductiveautomation.ignition.gateway.storeforward.quarantine.QuarantineInterface;
import com.inductiveautomation.ignition.gateway.storeforward.quarantine.QuarantinedDataInfo;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The gateway log during a long Factry problem: what it says once, and what it must not repeat.
 * The first two tests come from the second review of #67.
 */
class WriteHealthLoggingTest {
    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final WriteHealth health = new WriteHealth("factry", now::get);

    private static String logOf(Runnable r) {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            r.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString();
    }

    private static int count(String text, String word) {
        return text.split(word, -1).length - 1;
    }

    /** Two answers taking turns (some writes time out, some are refused): not a WARN per flush. */
    @Test
    void alternatingAnswersAreEachLoggedOnce() {
        health.connectionChecked(true);
        String log = logOf(() -> {
            for (int i = 0; i < 100; i++) {
                now.addAndGet(1000);
                health.writeFailed(i % 2 == 0
                        ? Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded after 2.9s")
                        : Status.UNKNOWN.withDescription("error writing points: timeout"), 5);
            }
        });
        assertEquals(ProfileStatus.State.Errored, health.status().getState());
        assertEquals(2, count(log, "did not store"), log);
    }

    /**
     * Errored for writes, then a connection check that fails and comes back, ten times. The
     * story is told once, and the stored write that ends it still says so.
     */
    @Test
    void aFlappingCheckDoesNotRepeatTheStory() {
        String log = logOf(() -> {
            for (int cycle = 0; cycle < 10; cycle++) {
                health.connectionChecked(true);
                for (int i = 0; i < 31; i++) {
                    now.addAndGet(1000);
                    health.writeFailed(Status.UNKNOWN.withDescription("error writing points"), 5);
                }
                health.connectionChecked(false);
            }
            health.connectionChecked(true);
            health.writeAccepted();
        });
        assertTrue(count(log, "WARN") + count(log, "INFO") <= 10, log);
        assertEquals(1, count(log, "did not store"), log);
        assertTrue(log.contains("Historian 'factry': Factry stores writes again, after 310 failed flushes / 1550"
                + " points"), log);
    }

    /** Ignition keeps the instance when a profile is renamed; the log lines follow the new name. */
    @Test
    void theLogLinesNameTheProfileAfterARename() {
        health.rename("factry-b");

        String log = logOf(() -> health.writeFailed(Status.INTERNAL.withDescription("refused"), 1));

        assertTrue(log.contains("Historian 'factry-b': Factry did not store 1 points"), log);
    }

    /** About 5,800 lines in a weekend outage: the quarantine retry is not news while writes fail. */
    @Test
    void theQuarantineRetryIsDebugWhileWritesFail() {
        StoreAndForwardManager storeAndForward = storeAndForwardWithOneQuarantinedEntry();
        health.connectionChecked(true);

        String healthy = logOf(() -> FactryHistoryProvider.retryQuarantinedData(storeAndForward, true, health, "factry"));
        health.writeFailed(Status.INTERNAL.withDescription("influx down"), 1);
        String failing = logOf(() -> FactryHistoryProvider.retryQuarantinedData(storeAndForward, true, health, "factry"));

        assertTrue(healthy.contains("INFO") && healthy.contains("Retrying 1 quarantined data entries"), healthy);
        assertEquals("", failing);
    }

    private static StoreAndForwardManager storeAndForwardWithOneQuarantinedEntry() {
        QuarantineInterface<?> quarantine = (QuarantineInterface<?>) Proxy.newProxyInstance(
                QuarantineInterface.class.getClassLoader(), new Class<?>[]{QuarantineInterface.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getQuarantinedCount" -> 1;
                    case "getQuarantinedDataInfo" -> List.of(new QuarantinedDataInfo(7, 10, "batch", "refused", null));
                    case "retryQuarantined" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (StoreAndForwardManager) Proxy.newProxyInstance(StoreAndForwardManager.class.getClassLoader(),
                new Class<?>[]{StoreAndForwardManager.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getQuarantineInterface")) {
                        return Optional.of(quarantine);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
