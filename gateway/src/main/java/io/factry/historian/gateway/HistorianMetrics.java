package io.factry.historian.gateway;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple metrics tracker for the Factry Historian module.
 * Tracks counters and cumulative timings for storage and query operations.
 */
public class HistorianMetrics {

    // Store metrics
    private final AtomicLong storeCount = new AtomicLong();
    private final AtomicLong storePointCount = new AtomicLong();
    private final AtomicLong storeTotalMs = new AtomicLong();
    private final AtomicLong storeErrorCount = new AtomicLong();

    // Raw query metrics
    private final AtomicLong rawQueryCount = new AtomicLong();
    private final AtomicLong rawQueryRowCount = new AtomicLong();
    private final AtomicLong rawQueryTotalMs = new AtomicLong();

    // Aggregated query metrics
    private final AtomicLong aggQueryCount = new AtomicLong();
    private final AtomicLong aggQueryRowCount = new AtomicLong();
    private final AtomicLong aggQueryTotalMs = new AtomicLong();

    public void recordStore(int points, long elapsedMs) {
        storeCount.incrementAndGet();
        storePointCount.addAndGet(points);
        storeTotalMs.addAndGet(elapsedMs);
    }

    public void recordStoreError() {
        storeErrorCount.incrementAndGet();
    }

    public void recordRawQuery(int rows, long elapsedMs) {
        rawQueryCount.incrementAndGet();
        rawQueryRowCount.addAndGet(rows);
        rawQueryTotalMs.addAndGet(elapsedMs);
    }

    public void recordAggregatedQuery(int rows, long elapsedMs) {
        aggQueryCount.incrementAndGet();
        aggQueryRowCount.addAndGet(rows);
        aggQueryTotalMs.addAndGet(elapsedMs);
    }

    public void reset() {
        storeCount.set(0);
        storePointCount.set(0);
        storeTotalMs.set(0);
        storeErrorCount.set(0);
        rawQueryCount.set(0);
        rawQueryRowCount.set(0);
        rawQueryTotalMs.set(0);
        aggQueryCount.set(0);
        aggQueryRowCount.set(0);
        aggQueryTotalMs.set(0);
    }

    public long getStoreCount() { return storeCount.get(); }
    public long getStorePointCount() { return storePointCount.get(); }
    public long getStoreTotalMs() { return storeTotalMs.get(); }
    public long getStoreErrorCount() { return storeErrorCount.get(); }
    public long getRawQueryCount() { return rawQueryCount.get(); }
    public long getRawQueryRowCount() { return rawQueryRowCount.get(); }
    public long getRawQueryTotalMs() { return rawQueryTotalMs.get(); }
    public long getAggQueryCount() { return aggQueryCount.get(); }
    public long getAggQueryRowCount() { return aggQueryRowCount.get(); }
    public long getAggQueryTotalMs() { return aggQueryTotalMs.get(); }
}
