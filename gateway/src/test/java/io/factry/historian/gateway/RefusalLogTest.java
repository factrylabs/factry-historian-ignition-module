package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.factry.historian.gateway.MeasurementCache.WriteRefusal.NOT_ACTIVE;
import static org.junit.jupiter.api.Assertions.*;

class RefusalLogTest {

    @Test
    void aRefusalIsReportedOnceUntilTheTagIsWritten() {
        RefusalLog log = new RefusalLog();

        assertTrue(log.refused("a", NOT_ACTIVE));
        assertFalse(log.refused("a", NOT_ACTIVE));
        assertEquals(NOT_ACTIVE, log.written("a"));
        assertTrue(log.refused("a", NOT_ACTIVE));
    }

    @Test
    void clearingReportsEveryRefusalAgain() {
        RefusalLog log = new RefusalLog();
        log.refused("a", NOT_ACTIVE);

        log.clear();

        assertTrue(log.refused("a", NOT_ACTIVE));
    }

    @Test
    void namesShowsTheFirstFewAndCountsTheRest() {
        assertEquals("a, b", RefusalLog.names(List.of("a", "b"), 3));
        assertEquals("a, b and 2 more", RefusalLog.names(List.of("a", "b", "c", "d"), 2));
    }
}
