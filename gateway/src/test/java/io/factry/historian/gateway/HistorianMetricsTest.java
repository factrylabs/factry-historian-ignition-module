package io.factry.historian.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HistorianMetricsTest {

    private HistorianMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new HistorianMetrics();
    }

    // --- store metrics ---

    @Test
    void recordStore_incrementsCounters() {
        metrics.recordStore(5, 100);

        assertEquals(1, metrics.getStoreCount());
        assertEquals(5, metrics.getStorePointCount());
        assertEquals(100, metrics.getStoreTotalMs());
    }

    @Test
    void recordStore_multipleCallsAccumulate() {
        metrics.recordStore(3, 50);
        metrics.recordStore(7, 150);

        assertEquals(2, metrics.getStoreCount());
        assertEquals(10, metrics.getStorePointCount());
        assertEquals(200, metrics.getStoreTotalMs());
    }

    @Test
    void recordStoreError_incrementsErrorCount() {
        metrics.recordStoreError();
        metrics.recordStoreError();

        assertEquals(2, metrics.getStoreErrorCount());
    }

    // --- raw query metrics ---

    @Test
    void recordRawQuery_incrementsCounters() {
        metrics.recordRawQuery(50, 200);

        assertEquals(1, metrics.getRawQueryCount());
        assertEquals(50, metrics.getRawQueryRowCount());
        assertEquals(200, metrics.getRawQueryTotalMs());
    }

    @Test
    void recordRawQuery_multipleCallsAccumulate() {
        metrics.recordRawQuery(10, 100);
        metrics.recordRawQuery(20, 300);

        assertEquals(2, metrics.getRawQueryCount());
        assertEquals(30, metrics.getRawQueryRowCount());
        assertEquals(400, metrics.getRawQueryTotalMs());
    }

    // --- aggregated query metrics ---

    @Test
    void recordAggregatedQuery_incrementsCounters() {
        metrics.recordAggregatedQuery(25, 500);

        assertEquals(1, metrics.getAggQueryCount());
        assertEquals(25, metrics.getAggQueryRowCount());
        assertEquals(500, metrics.getAggQueryTotalMs());
    }

    // --- reset ---

    @Test
    void reset_clearsAllCounters() {
        metrics.recordStore(5, 100);
        metrics.recordStoreError();
        metrics.recordRawQuery(10, 200);
        metrics.recordAggregatedQuery(3, 50);

        metrics.reset();

        assertEquals(0, metrics.getStoreCount());
        assertEquals(0, metrics.getStorePointCount());
        assertEquals(0, metrics.getStoreTotalMs());
        assertEquals(0, metrics.getStoreErrorCount());
        assertEquals(0, metrics.getRawQueryCount());
        assertEquals(0, metrics.getRawQueryRowCount());
        assertEquals(0, metrics.getRawQueryTotalMs());
        assertEquals(0, metrics.getAggQueryCount());
        assertEquals(0, metrics.getAggQueryRowCount());
        assertEquals(0, metrics.getAggQueryTotalMs());
    }

    // --- initial state ---

    @Test
    void initialState_allZeros() {
        assertEquals(0, metrics.getStoreCount());
        assertEquals(0, metrics.getStorePointCount());
        assertEquals(0, metrics.getStoreTotalMs());
        assertEquals(0, metrics.getStoreErrorCount());
        assertEquals(0, metrics.getRawQueryCount());
        assertEquals(0, metrics.getRawQueryRowCount());
        assertEquals(0, metrics.getRawQueryTotalMs());
        assertEquals(0, metrics.getAggQueryCount());
        assertEquals(0, metrics.getAggQueryRowCount());
        assertEquals(0, metrics.getAggQueryTotalMs());
    }
}
