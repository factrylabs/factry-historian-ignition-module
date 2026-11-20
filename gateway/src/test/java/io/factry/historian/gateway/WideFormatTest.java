package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.sqltags.history.AggregationMode;
import io.factry.historian.gateway.IgnitionHistoryQuery.Dataset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Wide-format reads through Ignition's own query path, so the test sees the rows a user sees. */
class WideFormatTest {

    private static final long START = 1_794_268_800_000L; // 2026-11-10T00:00:00Z

    private FakeFactryServer factry;
    private FactryGrpcClient client;
    private FactryQueryEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        factry = new FakeFactryServer();
        client = factry.client();
    }

    @AfterEach
    void tearDown() throws Exception {
        client.shutdown();
        factry.close();
    }

    private void startEngine() {
        FactryHistorianSettings settings = new FactryHistorianSettings();
        settings.setDelimiter("/");
        MeasurementCache cache = new MeasurementCache();
        cache.refresh(client);
        engine = new FactryQueryEngine(FakeFactryServer.gatewayContext(), "Factry", settings, client, cache,
                new HistorianMetrics());
    }

    @Test
    void rawWideRowsFollowTimeAcrossPens() throws Exception {
        // Pen a writes on even seconds, pen b on odd seconds.
        String a = factry.addMeasurement("default/a", "number");
        String b = factry.addMeasurement("default/b", "number");
        for (int s = 0; s < 10; s++) {
            factry.addPoint(s % 2 == 0 ? a : b, START + s * 1000L, s);
        }
        startEngine();

        Dataset ds = IgnitionHistoryQuery.wide(engine, START, START + 10_000L, -1, null, "a", "b");

        assertNull(ds.error);
        assertEquals(10, ds.rows.size(), ds::toString);
        for (int i = 0; i < ds.rows.size(); i++) {
            assertEquals(START + i * 1000L, ds.time(i), ds::toString);
            // Each pen shows its own latest value: a holds the last even second, b the last odd one.
            Object[] row = ds.rows.get(i);
            assertEquals((double) (i - i % 2), ((Number) row[1]).doubleValue(), ds::toString);
            if (i > 0) {
                assertEquals((double) (i % 2 == 1 ? i : i - 1), ((Number) row[2]).doubleValue(), ds::toString);
            }
        }
    }

    @Test
    void aggregatedWideRowsFollowTimeAcrossPens() throws Exception {
        // The aggregated path still hands over pen by pen. Ignition's interval writer waits until
        // every pen has data past a row before it writes the row, so that order is fine there.
        String a = factry.addMeasurement("default/a", "number");
        String b = factry.addMeasurement("default/b", "number");
        for (int min = 0; min < 10; min++) {
            factry.addPoint(min % 2 == 0 ? a : b, START + min * 60_000L + 30_000L, min);
        }
        startEngine();

        Dataset ds = IgnitionHistoryQuery.wide(engine, START, START + 600_000L, 10, AggregationMode.Average, "a", "b");

        assertNull(ds.error);
        assertEquals(10, ds.rows.size(), ds::toString);
        for (int min = 0; min < 10; min++) {
            assertEquals(START + min * 60_000L, ds.time(min), ds::toString);
            // The pen with data in this minute shows exactly that minute's average.
            Object own = ds.rows.get(min)[min % 2 == 0 ? 1 : 2];
            assertEquals((double) min, ((Number) own).doubleValue(), ds::toString);
        }
    }
}
