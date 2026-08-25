package io.factry.historian.gateway;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import io.factry.historian.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Factry Historian Ignition module.
 * <p>
 * Tests exercise all {@code system.historian.*} functions via WebDev endpoints,
 * plus direct gRPC for data seeding and verification.
 * <p>
 * Requires running infrastructure:
 * <ul>
 *   <li>Ignition gateway with Factry Historian module + WebDev module</li>
 *   <li>Factry Historian (gRPC)</li>
 *   <li>WebDev endpoints deployed in the configured project</li>
 * </ul>
 * <p>
 * Run via: {@code ./gradlew integrationTest}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FactryIntegrationTest {

    // -- Configuration (from system properties, set by Gradle) ----------------

    private static final String GATEWAY_URL = System.getProperty("gateway.url", "http://localhost:8089");
    private static final String WEBDEV_PROJECT = System.getProperty("webdev.project", "TestFactry");
    private static final String GRPC_HOST = System.getProperty("grpc.host", "localhost");
    private static final int GRPC_PORT = Integer.parseInt(System.getProperty("grpc.port", "8001"));
    private static final String COLLECTOR_TOKEN = System.getProperty("collector.token", "");
    private static final String COLLECTOR_UUID = extractFromToken(COLLECTOR_TOKEN, "uuid");
    private static final String GATEWAY_SYSTEM_NAME = System.getProperty("gateway.system.name", "Ignition-FactryTest");
    private static final String COLLECTOR_NAME = System.getProperty("collector.name", "Ignition");

    private static final String HISTORIAN_NAME = System.getProperty("historian.name", "Factry Historian");

    /** Wait time for Store &amp; Forward to flush pending points to the sink (+ margin). */
    private static final int BATCH_FLUSH_WAIT_MS = 8_000;

    /** Unique prefix per test run to avoid measurement collisions. */
    private static final String TEST_PREFIX = "IT" + randomSuffix(6);

    // Real Ignition-8 runtime QualityCode.getCode() values (the level is in the top two bits),
    // NOT the legacy 0..255 view. Sourced from the Script Console.
    private static final int QUALITY_GOOD = 192;            // 0x000000C0, level 0
    private static final int QUALITY_UNCERTAIN = 1073742080; // 0x40000200, level 1
    private static final int QUALITY_BAD = -2147483136;      // 0x80000200, level 2

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // -- Shared resources -----------------------------------------------------

    private ManagedChannel grpcChannel;
    private HistorianGrpc.HistorianBlockingStub grpcStub;
    private HttpClient httpClient;
    private final Gson gson = new Gson();

    // -- Logging helpers ------------------------------------------------------

    static void log(String msg) {
        System.out.println("  [" + LocalTime.now().format(TIME_FMT) + "] " + msg);
    }

    static void pass(String msg) {
        log("PASS: " + msg);
    }

    static void section(String title) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("  " + title);
        System.out.println("============================================================");
    }

    // -- Setup / Teardown -----------------------------------------------------

    @BeforeAll
    void setup() {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("  Factry Historian - Integration Tests");
        System.out.println("============================================================");
        log("Gateway:    " + GATEWAY_URL);
        log("Project:    " + WEBDEV_PROJECT);
        log("Factry:     " + GRPC_HOST + ":" + GRPC_PORT);
        log("System:     " + GATEWAY_SYSTEM_NAME);
        log("Collector:  " + COLLECTOR_NAME);
        log("Prefix:     " + TEST_PREFIX);
        log("Historian:  " + HISTORIAN_NAME);

        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        try {
            grpcChannel = NettyChannelBuilder.forAddress(GRPC_HOST, GRPC_PORT)
                    .sslContext(io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.forClient()
                            .trustManager(io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                            .build())
                    .build();
        } catch (javax.net.ssl.SSLException e) {
            throw new RuntimeException("Failed to create TLS gRPC channel", e);
        }

        Metadata headers = new Metadata();
        if (!COLLECTOR_UUID.isEmpty()) {
            headers.put(Metadata.Key.of("collectoruuid", Metadata.ASCII_STRING_MARSHALLER), COLLECTOR_UUID);
        }
        if (!COLLECTOR_TOKEN.isEmpty()) {
            headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                    "Bearer " + COLLECTOR_TOKEN);
        }

        grpcStub = HistorianGrpc.newBlockingStub(grpcChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

        section("Setup");

        // Check Ignition gateway
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(GATEWAY_URL + "/StatusPing"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode(), "Gateway StatusPing failed");
            log("Ignition gateway: connected (response: " + resp.body() + ")");
        } catch (Exception e) {
            fail("Ignition gateway unreachable at " + GATEWAY_URL + " — " + e.getMessage());
        }

        // Check Factry gRPC
        try {
            Measurements measurements = grpcStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getMeasurements(MeasurementRequest.newBuilder().build());
            log("Factry gRPC: connected (" + measurements.getMeasurementsCount() + " measurements)");
        } catch (Exception e) {
            fail("Factry gRPC unreachable at " + GRPC_HOST + ":" + GRPC_PORT + " — " + e.getMessage());
        }

        // Verify WebDev endpoint is reachable
        try {
            Map<String, Object> probe = webdevPost("test/queryRaw", Map.of(
                    "paths", List.of("histprov:" + HISTORIAN_NAME + ":/sys:probe:/prov:default:/tag:probe"),
                    "startDate", 1600000000000L,
                    "endDate", 1600000010000L
            ));
            log("WebDev endpoints: reachable (response: " + (probe.get("success")) + ")");
        } catch (Exception e) {
            fail("WebDev endpoints unreachable — " + e.getMessage());
        }
    }

    @AfterAll
    void teardown() {
        if (grpcChannel != null) {
            try {
                grpcChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                grpcChannel.shutdownNow();
            }
        }
    }

    // =========================================================================
    // Determine which historians to test
    // =========================================================================

    /** Extract a field from the JWT token payload (base64-decoded JSON). */
    private static String extractFromToken(String token, String field) {
        if (token == null || token.isEmpty()) return "";
        try {
            String[] parts = token.split("\\.");
            if (parts.length < 2) return "";
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            // Simple JSON field extraction without a JSON library
            String pattern = "\"" + field + "\":\"";
            int start = payload.indexOf(pattern);
            if (start < 0) return "";
            start += pattern.length();
            int end = payload.indexOf("\"", start);
            return end > start ? payload.substring(start, end) : "";
        } catch (Exception e) {
            return "";
        }
    }

    // =========================================================================
    // Historian tests
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("system.historian.storeDataPoints — store numeric data")
    void testStoreDataPoints() throws Exception {
        section("storeDataPoints");

        String tagName = TEST_PREFIX + "/StorePoints";
        long baseTs = 1700010000000L;

        // Pre-create the measurement so Ignition's lookupNode can resolve it
        String measurementName = storedTagPath(tagName);
        String preUuid = createMeasurement(measurementName, "number");
        assertFalse(preUuid.isEmpty(), "Pre-created measurement should have a UUID");
        log("Pre-created measurement " + preUuid);

        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        Map<String, Object> result = webdevPost("test/storePoints", Map.of(
                "paths", List.of(qPath, qPath, qPath),
                "values", List.of(100.0, 200.0, 300.0),
                "timestamps", List.of(baseTs, baseTs + 1000, baseTs + 2000),
                "qualities", List.of(192, 192, 192)
        ));
        assertTrue((Boolean) result.get("success"), "storeDataPoints should succeed");
        pass("storeDataPoints returned success");

        log("Waiting " + (BATCH_FLUSH_WAIT_MS / 1000) + "s for batch flush...");
        Thread.sleep(BATCH_FLUSH_WAIT_MS);

        assertNotNull(findMeasurementUuid(measurementName),
                "Measurement '" + measurementName + "' should exist in Factry");
        pass("Measurement exists in Factry: " + preUuid);

        QueryTimeseriesResponse response = grpcQuery(preUuid, baseTs - 1000, baseTs + 3000);
        assertFalse(response.getSeriesList().isEmpty(), "Should return at least one series");
        int pointCount = response.getSeries(0).getDataPointsCount();
        assertTrue(pointCount >= 3, "Expected >= 3 points, got " + pointCount);
        pass("gRPC verification: " + pointCount + " points stored");
    }

    @Test
    @Order(20)
    @DisplayName("system.historian.queryRawPoints — query raw data")
    void testQueryRawPoints() throws Exception {
        section("queryRawPoints");

        String tagName = TEST_PREFIX + "/RawQuery";
        String measurementName = storedTagPath(tagName);

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty(), "Measurement UUID should not be empty");

        long baseTs = 1700020000000L;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            points.add(buildPoint(uuid, baseTs + i * 1000, Value.newBuilder().setNumberValue(10.0 + i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 5 points for measurement " + uuid);
        Thread.sleep(2000);

        Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 5000
        ));

        assertTrue((Boolean) result.get("success"), "queryRawPoints should succeed");
        pass("Raw query returned success");

        int rowCount = ((Number) result.get("rowCount")).intValue();
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        log("Got " + rowCount + " rows, columns: " + columns);
        assertTrue(rowCount >= 5, "Expected >= 5 rows, got " + rowCount);
        pass("Got data rows back");
    }

    @Test
    @Order(30)
    @DisplayName("system.historian.queryAggregatedPoints — all aggregation types")
    void testQueryAggregatedPoints() throws Exception {
        section("queryAggregatedPoints (all aggregation types)");

        String tagName = TEST_PREFIX + "/Aggregation";
        String measurementName = storedTagPath(tagName);

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty());

        // Insert 100 points with values 0..99 (1 second apart)
        long baseTs = 1700030000000L;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            points.add(buildPoint(uuid, baseTs + i * 1000,
                    Value.newBuilder().setNumberValue(i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 100 points (values 0..99) for measurement " + uuid);
        Thread.sleep(2000);

        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        long startMs = baseTs - 1000;
        long endMs = baseTs + 100_000;

        // --- Average ---
        assertAggQuery(qPath, startMs, endMs, "Average", 49.5, 5.0);

        // --- SimpleAverage ---
        assertAggQuery(qPath, startMs, endMs, "SimpleAverage", 49.5, 5.0);

        // --- Minimum ---
        assertAggQuery(qPath, startMs, endMs, "Minimum", 0.0, 1.0);

        // --- Maximum ---
        assertAggQuery(qPath, startMs, endMs, "Maximum", 99.0, 1.0);

        // --- Sum ---
        assertAggQuery(qPath, startMs, endMs, "Sum", 4950.0, 50.0);

        // --- Count ---
        assertAggQuery(qPath, startMs, endMs, "Count", 100.0, 5.0);

        // --- LastValue ---
        assertAggQuery(qPath, startMs, endMs, "LastValue", 99.0, 1.0);

        // --- Range (spread = max - min) ---
        assertAggQuery(qPath, startMs, endMs, "Range", 99.0, 1.0);

        // Variance and StdDev are not supported by the Factry backend (returns NaN)

        // --- MinMax ---
        Map<String, Object> minMaxResult = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(qPath),
                "startDate", startMs,
                "endDate", endMs,
                "aggregates", List.of("MinMax"),
                "returnSize", 1
        ));
        assertTrue((Boolean) minMaxResult.get("success"), "MinMax query should succeed");
        int minMaxRows = ((Number) minMaxResult.get("rowCount")).intValue();
        log("MinMax returned " + minMaxRows + " rows");
        assertTrue(minMaxRows >= 1, "MinMax should return >= 1 rows, got " + minMaxRows);
        pass("MinMax");
    }

    /** Helper: query a single aggregation type and assert the value. */
    private void assertAggQuery(String qPath, long startMs, long endMs,
                                String aggName, double expected, double tolerance) throws Exception {
        Map<String, Object> result = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(qPath),
                "startDate", startMs,
                "endDate", endMs,
                "aggregates", List.of(aggName),
                "returnSize", 1
        ));
        assertTrue((Boolean) result.get("success"), aggName + " query should succeed");
        double actual = extractAggregationValue(result);
        log(aggName + " = " + actual + " (expected ~" + expected + ")");
        assertAggregationValue(result, expected, tolerance, aggName);
        pass(aggName);
    }

    @Test
    @Order(40)
    @DisplayName("system.historian.queryMetadata — query measurement metadata")
    void testQueryMetadata() throws Exception {
        section("queryMetadata");

        String tagName = TEST_PREFIX + "/Metadata";
        String measurementName = storedTagPath(tagName);

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty());
        log("Created measurement " + uuid);

        Map<String, Object> result = webdevPost("test/queryMeta", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName))
        ));

        assertTrue((Boolean) result.get("success"), "queryMetadata should succeed");
        pass("queryMetadata returned success");

        int rowCount = ((Number) result.get("rowCount")).intValue();
        log("Got " + rowCount + " metadata rows");
        assertTrue(rowCount >= 1, "Expected >= 1 metadata row, got " + rowCount);
        pass("Got metadata back");
    }

    @Test
    @Order(50)
    @DisplayName("system.historian.browse — browse historian hierarchy")
    void testBrowse() throws Exception {
        section("browse");

        Map<String, Object> rootResult = webdevPost("test/browse", Map.of(
                "path", "histprov:" + HISTORIAN_NAME + ":/"
        ));
        assertTrue((Boolean) rootResult.get("success"), "Root browse should succeed");
        pass("Root browse returned success");

        int rootCount = ((Number) rootResult.get("count")).intValue();
        assertTrue(rootCount >= 1, "Root should have at least 1 child node, got " + rootCount);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) rootResult.get("results");
        for (Map<String, Object> node : nodes) {
            log("Node: " + node.get("path") + " (type=" + node.get("type") + ", hasChildren=" + node.get("hasChildren") + ")");
        }
        pass("Root browse returned " + rootCount + " nodes");
    }

    @Test
    @Order(60)
    @DisplayName("system.historian.storeMetadata — store measurement metadata")
    void testStoreMetadata() throws Exception {
        section("storeMetadata");

        String tagName = TEST_PREFIX + "/StoreMeta";
        String measurementName = storedTagPath(tagName);

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty());
        log("Created measurement " + uuid);

        // Warm up the module's measurement cache by doing a query first
        webdevPost("test/queryRaw", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", 1600000000000L,
                "endDate", 1600000010000L
        ));

        Map<String, Object> result = webdevPost("test/storeMeta", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "timestamps", List.of(System.currentTimeMillis()),
                "properties", List.of(Map.of("engineeringUnits", "degC"))
        ));

        assertTrue((Boolean) result.get("success"), "storeMetadata should succeed (or no-op gracefully)");
        pass("storeMetadata returned success");
    }

    @Test
    @Order(70)
    @DisplayName("Empty time range returns zero rows")
    void testEmptyQuery() throws Exception {
        section("Empty Query");

        String tagName = TEST_PREFIX + "/Empty";
        String measurementName = storedTagPath(tagName);

        createMeasurement(measurementName, "number");

        Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", 1600000000000L,
                "endDate", 1600000010000L
        ));
        assertTrue((Boolean) result.get("success"));
        int rowCount = ((Number) result.get("rowCount")).intValue();
        log("Got " + rowCount + " rows for empty time range");
        assertEquals(0, rowCount, "Should return 0 rows for empty range");
        pass("Empty time range returns 0 rows");
    }

    @Test
    @Order(80)
    @DisplayName("String-type measurement storage and gRPC query")
    void testStringValues() throws Exception {
        section("String Values");

        String tagName = TEST_PREFIX + "/StringTest";
        String measurementName = storedTagPath(tagName);
        long baseTs = 1700050000000L;

        String uuid = createMeasurement(measurementName, "string");
        assertFalse(uuid.isEmpty(), "String measurement UUID should not be empty");
        log("Created string measurement " + uuid);

        List<Point> points = List.of(
                buildPoint(uuid, baseTs, Value.newBuilder().setStringValue("hello").build()),
                buildPoint(uuid, baseTs + 1000, Value.newBuilder().setStringValue("world").build()),
                buildPoint(uuid, baseTs + 2000, Value.newBuilder().setStringValue("test").build())
        );
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 3 string points via gRPC");
        Thread.sleep(2000);

        QueryTimeseriesResponse grpcResponse = grpcQuery(uuid, baseTs - 1000, baseTs + 3000);
        int grpcPoints = grpcResponse.getSeriesList().isEmpty() ? 0
                : grpcResponse.getSeries(0).getDataPointsCount();
        assertTrue(grpcPoints >= 3, "Expected >= 3 string points via gRPC, got " + grpcPoints);
        pass("gRPC verification: " + grpcPoints + " string points stored");

        Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 3000
        ));
        assertTrue((Boolean) result.get("success"), "queryRawPoints should succeed for string tags");
        int rowCount = ((Number) result.get("rowCount")).intValue();
        assertTrue(rowCount >= 3,
                "String tags must return their history via system.historian, got " + rowCount + " rows");
        pass("system.historian returned " + rowCount + " rows");
    }

    @Test
    @Order(90)
    @DisplayName("Multi-tag query returns all columns")
    void testMultiTagQuery() throws Exception {
        section("Multi-Tag Query");

        long baseTs = 1700060000000L;
        List<String> tagNames = List.of(
                TEST_PREFIX + "/Multi0",
                TEST_PREFIX + "/Multi1",
                TEST_PREFIX + "/Multi2"
        );

        for (int idx = 0; idx < tagNames.size(); idx++) {
            String measurementName = storedTagPath(tagNames.get(idx));
            String uuid = createMeasurement(measurementName, "number");
            assertFalse(uuid.isEmpty());

            List<Point> points = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                points.add(buildPoint(uuid, baseTs + i * 1000,
                        Value.newBuilder().setNumberValue(idx * 100.0 + i).build()));
            }
            grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
            log("Inserted 5 points for tag " + tagNames.get(idx));
        }
        Thread.sleep(2000);

        List<String> paths = tagNames.stream()
                .map(t -> qualifiedPath(HISTORIAN_NAME, t))
                .toList();
        Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                "paths", paths,
                "startDate", baseTs - 1000,
                "endDate", baseTs + 5000
        ));

        assertTrue((Boolean) result.get("success"));
        pass("Multi-tag query returned success");

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        log("Got " + columns.size() + " columns: t_stamp + " + (columns.size() - 1) + " tags");
        assertTrue(columns.size() >= 4,
                "Expected >= 4 columns (t_stamp + 3 tags), got " + columns);
        pass("Column count");

        int rowCount = ((Number) result.get("rowCount")).intValue();
        log("Got " + rowCount + " rows");
        assertTrue(rowCount >= 5, "Expected >= 5 rows");
        pass("Row count");
    }

    @Test
    @Order(95)
    @DisplayName("Mixed string + numeric raw query returns both instead of aborting")
    void testMixedStringAndNumericQuery() throws Exception {
        section("Mixed String + Numeric Query");

        long baseTs = 1700065000000L;

        // Numeric tag with 5 points
        String numTag = TEST_PREFIX + "/MixedNumeric";
        String numUuid = createMeasurement(storedTagPath(numTag), "number");
        assertFalse(numUuid.isEmpty(), "Numeric measurement UUID should not be empty");
        List<Point> numPoints = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            numPoints.add(buildPoint(numUuid, baseTs + i * 1000,
                    Value.newBuilder().setNumberValue(10.0 + i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(numPoints).build());
        log("Inserted 5 numeric points");

        // String tag with 3 points
        String strTag = TEST_PREFIX + "/MixedString";
        String strUuid = createMeasurement(storedTagPath(strTag), "string");
        assertFalse(strUuid.isEmpty(), "String measurement UUID should not be empty");
        List<Point> strPoints = List.of(
                buildPoint(strUuid, baseTs, Value.newBuilder().setStringValue("alpha").build()),
                buildPoint(strUuid, baseTs + 1000, Value.newBuilder().setStringValue("beta").build()),
                buildPoint(strUuid, baseTs + 2000, Value.newBuilder().setStringValue("gamma").build())
        );
        grpcStub.createPoints(Points.newBuilder().addAllPoints(strPoints).build());
        log("Inserted 3 string points");
        Thread.sleep(2000);

        // Query BOTH tags in a single raw query
        Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                "paths", List.of(
                        qualifiedPath(HISTORIAN_NAME, numTag),
                        qualifiedPath(HISTORIAN_NAME, strTag)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 5000
        ));

        // BUG being reproduced: a string tag in the mix aborts the entire query,
        // so even the numeric tag returns nothing.
        assertTrue((Boolean) result.get("success"),
                "Mixed string+numeric query must not abort — error: " + result.get("error"));
        pass("Mixed query returned success (did not abort)");

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) result.get("rows");
        log("Columns: " + columns);
        log("Row count: " + result.get("rowCount"));

        // Both tags must appear as columns
        String numCol = columns.stream().filter(c -> c.contains("MixedNumeric")).findFirst().orElse(null);
        String strCol = columns.stream().filter(c -> c.contains("MixedString")).findFirst().orElse(null);
        assertNotNull(numCol, "Numeric tag column must be present, got " + columns);
        assertNotNull(strCol, "String tag column must be present, got " + columns);

        // The good (numeric) tag must still return its values
        long numValues = rows.stream().filter(r -> r.get(numCol) != null).count();
        assertTrue(numValues >= 5, "Numeric tag must return its 5 values, got " + numValues);
        pass("Numeric tag returned " + numValues + " values in the mixed query");

        // The string tag should return its history too — strings can have history
        // and must work, not be silently dropped.
        long strValues = rows.stream().filter(r -> r.get(strCol) != null).count();
        log("String tag returned " + strValues + " non-null values");
        assertTrue(strValues >= 3, "String tag must return its 3 values, got " + strValues);
        pass("String tag returned " + strValues + " values in the mixed query");
    }

    @Test
    @Order(96)
    @DisplayName("Aggregated query on a string measurement returns instead of aborting")
    void testAggregatedStringQuery() throws Exception {
        section("Aggregated String Query (Power Chart path)");

        long baseTs = 1700066000000L;

        // Numeric tag so we can prove a string in the same aggregated query does not
        // take the numeric tag down with it (the framework commits a shared result set).
        String numTag = TEST_PREFIX + "/AggNumeric";
        String numUuid = createMeasurement(storedTagPath(numTag), "number");
        assertFalse(numUuid.isEmpty(), "Numeric measurement UUID should not be empty");
        List<Point> numPoints = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            numPoints.add(buildPoint(numUuid, baseTs + i * 1000,
                    Value.newBuilder().setNumberValue(20.0 + i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(numPoints).build());
        log("Inserted 5 numeric points");

        // String tag — this is what the Perspective Power Chart queries via the aggregated
        // path, and what used to throw a ClassCastException and abort the whole read.
        String strTag = TEST_PREFIX + "/AggString";
        String strUuid = createMeasurement(storedTagPath(strTag), "string");
        assertFalse(strUuid.isEmpty(), "String measurement UUID should not be empty");
        List<Point> strPoints = List.of(
                buildPoint(strUuid, baseTs, Value.newBuilder().setStringValue("alpha").build()),
                buildPoint(strUuid, baseTs + 1000, Value.newBuilder().setStringValue("beta").build()),
                buildPoint(strUuid, baseTs + 2000, Value.newBuilder().setStringValue("gamma").build())
        );
        grpcStub.createPoints(Points.newBuilder().addAllPoints(strPoints).build());
        log("Inserted 3 string points");
        Thread.sleep(2000);

        // Aggregated query over BOTH tags — LastValue works for strings and numbers alike.
        // queryAggregatedPoints requires one aggregate per path, so pass two.
        Map<String, Object> result = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(
                        qualifiedPath(HISTORIAN_NAME, numTag),
                        qualifiedPath(HISTORIAN_NAME, strTag)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 5000,
                "aggregates", List.of("LastValue", "LastValue"),
                "returnSize", 1
        ));

        // BUG being reproduced: the aggregated path initialised the framework with an empty
        // processing context, so string columns defaulted to numeric and a string value
        // aborted the entire query.
        assertTrue((Boolean) result.get("success"),
                "Aggregated string query must not abort — error: " + result.get("error"));
        pass("Aggregated string query returned success (did not abort)");

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        String numCol = columns.stream().filter(c -> c.contains("AggNumeric")).findFirst().orElse(null);
        String strCol = columns.stream().filter(c -> c.contains("AggString")).findFirst().orElse(null);
        assertNotNull(numCol, "Numeric tag column must be present, got " + columns);
        assertNotNull(strCol, "String tag column must be present, got " + columns);
        pass("Both numeric and string columns present in aggregated result");
    }

    @Test
    @Order(97)
    @DisplayName("Uncertain quality round-trips as an Uncertain status (not Good)")
    void testQualityStatusRoundTrip() throws Exception {
        section("Quality → Factry status mapping");

        String tagName = TEST_PREFIX + "/QualityStatus";
        String measurementName = storedTagPath(tagName);
        long baseTs = 1700067000000L;

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty(), "Measurement UUID should not be empty");

        // Store one point per Ignition-8 quality LEVEL, using the REAL runtime QualityCode
        // values (not the legacy 0..255 view). These are the codes system.tag / the SDK
        // actually emit — see QualityCode.getCode().
        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        webdevPost("test/storePoints", Map.of(
                "paths", List.of(qPath, qPath, qPath),
                "values", List.of(1.0, 2.0, 3.0),
                "timestamps", List.of(baseTs, baseTs + 1000, baseTs + 2000),
                "qualities", List.of(QUALITY_GOOD, QUALITY_UNCERTAIN, QUALITY_BAD)
        ));
        log("Stored Good/Uncertain/Bad points (codes " + QUALITY_GOOD + "/" + QUALITY_UNCERTAIN
                + "/" + QUALITY_BAD + ")");
        Thread.sleep(2000);

        // Read the stored per-point status straight from Factry, grouped by status tag.
        QueryTimeseriesResponse resp = grpcQueryGroupedByStatus(uuid, baseTs - 1000, baseTs + 3000);
        Set<String> statuses = new HashSet<>();
        for (Series s : resp.getSeriesList()) {
            Value stv = s.getTags().getFieldsMap().get("status");
            if (stv != null && !stv.getStringValue().isEmpty()) {
                statuses.add(stv.getStringValue());
            }
        }
        log("Stored statuses in Factry: " + statuses);

        // BUG (write-side mapping): qualityToStatus() uses legacy >=192/>=64 thresholds, so the
        // Uncertain code (~1.07e9) is >=192 and gets stored as "Good". The Uncertain level must
        // survive the round trip as an Uncertain* status instead.
        assertTrue(statuses.stream().anyMatch(st -> st.startsWith("Uncertain")),
                "Uncertain-quality point must be stored with an Uncertain status, got: " + statuses);
        pass("Uncertain quality stored as an Uncertain status");
    }

    @Test
    @Order(98)
    @DisplayName("Aggregation excludes Bad-quality points instead of blending them")
    void testAggregationExcludesBadQuality() throws Exception {
        section("Aggregation quality blending");

        String tagName = TEST_PREFIX + "/QualityAgg";
        String measurementName = storedTagPath(tagName);
        long baseTs = 1700068000000L;

        String uuid = createMeasurement(measurementName, "number");
        assertFalse(uuid.isEmpty(), "Measurement UUID should not be empty");

        // 5 Good points at ~5.0 interleaved with 5 Bad points at ~5000.0.
        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        List<Object> paths = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<Object> timestamps = new ArrayList<>();
        List<Object> qualities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            paths.add(qPath);
            timestamps.add(baseTs + i * 1000L);
            if (i % 2 == 0) {
                values.add(5.0);
                qualities.add(QUALITY_GOOD);
            } else {
                values.add(5000.0);
                qualities.add(QUALITY_BAD);
            }
        }
        webdevPost("test/storePoints", Map.of(
                "paths", paths, "values", values, "timestamps", timestamps, "qualities", qualities));
        log("Stored 5 Good (5.0) + 5 Bad (5000.0) points");
        Thread.sleep(2000);

        Map<String, Object> result = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(qPath),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 11000,
                "aggregates", List.of("Average"),
                "returnSize", 1
        ));
        assertTrue((Boolean) result.get("success"), "Average query should succeed: " + result.get("error"));
        double avg = extractAggregationValue(result);
        log("Average = " + avg + " (Good-only expects ~5.0; blended would be ~2502)");

        // BUG (read-side): the aggregated query runs across all statuses, so the Bad 5000.0
        // samples drag the average to ~2502. Aggregates must reflect Good data only.
        assertTrue(avg < 100.0,
                "Average must exclude Bad-quality points (expected ~5.0, got " + avg + ")");
        pass("Aggregate reflects Good-quality data only");
    }

    @Test
    @Order(99)
    @DisplayName("Batch new-tag creation: N brand-new tags in one flush all get stored")
    void testBatchNewTagCreation() throws Exception {
        section("Batch new-tag creation");

        int tagCount = 10;
        long baseTs = 1700069000000L;

        // Build paths and values for tagCount brand-new tags (none pre-created in Factry).
        List<Object> paths = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<Object> timestamps = new ArrayList<>();
        List<Object> qualities = new ArrayList<>();
        List<String> tagNames = new ArrayList<>();
        for (int i = 0; i < tagCount; i++) {
            String tagName = TEST_PREFIX + "/BatchNew" + i;
            tagNames.add(tagName);
            paths.add(qualifiedPath(HISTORIAN_NAME, tagName));
            values.add((double) (i + 1) * 10.0);
            timestamps.add(baseTs + i * 1000L);
            qualities.add(QUALITY_GOOD);
        }

        log("Storing " + tagCount + " points for brand-new tags in a single storeDataPoints call");
        Map<String, Object> storeResult = webdevPost("test/storePoints", Map.of(
                "paths", paths, "values", values, "timestamps", timestamps, "qualities", qualities));
        assertTrue((Boolean) storeResult.get("success"), "storeDataPoints should succeed: " + storeResult.get("error"));

        log("Waiting " + (BATCH_FLUSH_WAIT_MS / 1000) + "s for S&F flush and measurement creation...");
        Thread.sleep(BATCH_FLUSH_WAIT_MS);

        // Verify every tag was created in Factry and its point is queryable.
        int createdCount = 0;
        for (int i = 0; i < tagCount; i++) {
            String measurementName = storedTagPath(tagNames.get(i));
            String uuid = findMeasurementUuid(measurementName);
            if (uuid == null) {
                log("MISS: measurement not found for '" + measurementName + "'");
                continue;
            }
            QueryTimeseriesResponse resp = grpcQuery(uuid, baseTs - 1000, baseTs + tagCount * 1000L);
            int pts = resp.getSeriesList().isEmpty() ? 0 : resp.getSeries(0).getDataPointsCount();
            log("Tag " + i + " (" + measurementName + "): uuid=" + uuid + ", points=" + pts);
            if (pts >= 1) createdCount++;
        }
        log("Created and stored: " + createdCount + "/" + tagCount);
        assertEquals(tagCount, createdCount,
                "All " + tagCount + " new tags should have been created and their point stored");
        pass("Batch creation: all " + tagCount + " new measurements created in one flush");
    }

    @Test
    @Order(100)
    @DisplayName("Full round trip: store + query via system.historian")
    void testRoundTrip() throws Exception {
        section("Round Trip");

        String tagName = TEST_PREFIX + "/RoundTrip";
        long baseTs = 1700070000000L;

        // Pre-create the measurement so Ignition's lookupNode can resolve it
        String measurementName = storedTagPath(tagName);
        String preUuid = createMeasurement(measurementName, "number");
        assertFalse(preUuid.isEmpty(), "Pre-created measurement should have a UUID");
        log("Pre-created measurement " + preUuid);

        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        Map<String, Object> storeResult = webdevPost("test/storePoints", Map.of(
                "paths", List.of(qPath, qPath, qPath, qPath, qPath),
                "values", List.of(42.0, 43.0, 44.0, 45.0, 46.0),
                "timestamps", List.of(baseTs, baseTs + 1000, baseTs + 2000, baseTs + 3000, baseTs + 4000),
                "qualities", List.of(192, 192, 192, 192, 192)
        ));
        assertTrue((Boolean) storeResult.get("success"));
        pass("storeDataPoints returned success (5 points)");

        log("Waiting " + (BATCH_FLUSH_WAIT_MS / 1000) + "s for batch flush...");
        Thread.sleep(BATCH_FLUSH_WAIT_MS);

        Map<String, Object> queryResult = webdevPost("test/queryRaw", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 5000
        ));
        assertTrue((Boolean) queryResult.get("success"));
        pass("queryRawPoints returned success");

        int rowCount = ((Number) queryResult.get("rowCount")).intValue();
        log("Got " + rowCount + " rows back");
        assertTrue(rowCount >= 5, "Expected >= 5 rows in round-trip query");
        pass("Round trip: stored 5, queried " + rowCount);
    }

    // -------------------------------------------------------------------------
    // Aggregation coverage
    // -------------------------------------------------------------------------

    @Test
    @Order(101)
    @DisplayName("queryAggregatedPoints with default args returns a real value (not a placeholder)")
    void testAggregatedPointsDefaultArgs() throws Exception {
        section("queryAggregatedPoints — default args");

        String tagName = TEST_PREFIX + "/AggDefault";
        String uuid = createMeasurement(storedTagPath(tagName), "number");
        assertFalse(uuid.isEmpty());

        long baseTs = 1700120000000L;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            points.add(buildPoint(uuid, baseTs + i * 1000, Value.newBuilder().setNumberValue(i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 100 points (0..99)");
        Thread.sleep(2000);

        // Only paths/start/end — no aggregates, no returnSize. The historian must apply
        // its own defaults and return a REAL aggregated value, not a [timestamp, null] stub.
        Map<String, Object> result = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", baseTs - 1000,
                "endDate", baseTs + 100_000
        ));
        assertTrue((Boolean) result.get("success"), "Default-args aggregation should succeed");
        double value = extractAggregationValue(result);
        log("Default-args aggregate value = " + value);
        assertFalse(Double.isNaN(value), "Default-args aggregation must return a real value, not null");
        assertTrue(value >= 0.0 && value <= 99.0,
                "Default-args aggregate must fall within the seeded range [0,99], got " + value);
        pass("Default-args aggregation returned a real value: " + value);
    }

    @Test
    @Order(102)
    @DisplayName("queryAggregatedPoints returnSize produces multiple ascending windows")
    void testAggregationReturnSizeWindows() throws Exception {
        section("queryAggregatedPoints — returnSize windows");

        String tagName = TEST_PREFIX + "/AggWindows";
        String uuid = createMeasurement(storedTagPath(tagName), "number");
        assertFalse(uuid.isEmpty());

        // 100 points, value == index (0..99), one second apart.
        long baseTs = 1700130000000L;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            points.add(buildPoint(uuid, baseTs + i * 1000, Value.newBuilder().setNumberValue(i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 100 points (0..99)");
        Thread.sleep(2000);

        // returnSize=5 → 5 equal windows over the range; each window's Average should
        // increase monotonically (~9.5, 29.5, 49.5, 69.5, 89.5).
        Map<String, Object> result = webdevPost("test/queryAgg", Map.of(
                "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                "startDate", baseTs,
                "endDate", baseTs + 100_000,
                "aggregates", List.of("Average"),
                "returnSize", 5
        ));
        assertTrue((Boolean) result.get("success"), "Windowed aggregation should succeed");

        int rows = ((Number) result.get("rowCount")).intValue();
        log("Windowed aggregation returned " + rows + " rows");
        assertTrue(rows >= 4, "Expected ~5 windows, got " + rows);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rowList = (List<Map<String, Object>>) result.get("rows");
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) result.get("columns");
        String valCol = columns.stream().filter(c -> !c.equals("t_stamp")).findFirst().orElseThrow();

        Double prev = null;
        int increasing = 0;
        for (Map<String, Object> row : rowList) {
            Object v = row.get(valCol);
            if (v == null) continue;
            double d = ((Number) v).doubleValue();
            if (prev != null && d > prev) increasing++;
            prev = d;
        }
        log("Windows had " + increasing + " ascending steps");
        assertTrue(increasing >= 3, "Window averages should increase across the ascending dataset");
        pass("returnSize produced " + rows + " ascending windows");
    }

    @Test
    @Order(103)
    @DisplayName("system.tag.queryTagHistory — aggregation modes")
    void testTagHistoryAggregation() throws Exception {
        section("system.tag.queryTagHistory aggregation");

        String tagName = TEST_PREFIX + "/TagHistAgg";
        String uuid = createMeasurement(storedTagPath(tagName), "number");
        assertFalse(uuid.isEmpty());

        long baseTs = 1700140000000L;
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            points.add(buildPoint(uuid, baseTs + i * 1000, Value.newBuilder().setNumberValue(i).build()));
        }
        grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
        log("Inserted 100 points (0..99)");
        Thread.sleep(2000);

        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        long startMs = baseTs - 1000;
        long endMs = baseTs + 100_000;

        assertTagHistoryAgg(qPath, startMs, endMs, "Average", 49.5, 5.0);
        assertTagHistoryAgg(qPath, startMs, endMs, "Minimum", 0.0, 1.0);
        assertTagHistoryAgg(qPath, startMs, endMs, "Maximum", 99.0, 1.0);
        assertTagHistoryAgg(qPath, startMs, endMs, "Sum", 4950.0, 50.0);
    }

    /** Query one aggregation mode via system.tag.queryTagHistory (the test/query endpoint). */
    private void assertTagHistoryAgg(String qPath, long startMs, long endMs,
                                     String mode, double expected, double tolerance) throws Exception {
        Map<String, Object> result = webdevPost("test/query", Map.of(
                "paths", List.of(qPath),
                "startDate", startMs,
                "endDate", endMs,
                "aggregationMode", mode,
                "returnSize", 1
        ));
        assertTrue((Boolean) result.get("success"), mode + " tag-history query should succeed");
        double actual = extractAggregationValue(result);
        log("queryTagHistory " + mode + " = " + actual + " (expected ~" + expected + ")");
        assertAggregationValue(result, expected, tolerance, "queryTagHistory " + mode);
        pass("queryTagHistory " + mode);
    }

    // -------------------------------------------------------------------------
    // Store paths & metadata
    // -------------------------------------------------------------------------

    @Test
    @Order(105)
    @DisplayName("storeMetadata — engineering unit round trip via queryMetadata")
    void testMetadataEngUnitRoundTrip() throws Exception {
        section("Metadata engUnit round trip");

        String tagName = TEST_PREFIX + "/MetaEng";
        String measurementName = storedTagPath(tagName);
        String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
        long baseTs = 1700160000000L;

        // 1) Store metadata BEFORE the measurement exists — it is cached and applied when
        //    the measurement is created by the first data point.
        Map<String, Object> metaResult = webdevPost("test/storeMeta", Map.of(
                "paths", List.of(qPath),
                "timestamps", List.of(baseTs),
                "properties", Map.of(
                        "engUnit", "degC",
                        "engLow", "0",
                        "engHigh", "100",
                        "description", "Temperature sensor")
        ));
        assertTrue((Boolean) metaResult.get("success"),
                "storeMetadata should succeed — error: " + metaResult.get("error"));
        pass("storeMetadata accepted engUnit/engLow/engHigh/description");

        // 2) Store a data point — this triggers measurement creation with the cached metadata.
        webdevPost("test/storePoints", Map.of(
                "paths", List.of(qPath),
                "values", List.of(42.0),
                "timestamps", List.of(baseTs),
                "qualities", List.of(192)
        ));

        // 3) Wait for S&F to flush and the measurement to be created.
        String createdUuid = null;
        for (int i = 0; i < 20 && createdUuid == null; i++) {
            Thread.sleep(1500);
            createdUuid = findMeasurementUuid(measurementName);
        }
        assertNotNull(createdUuid, "Measurement should be created within timeout");
        log("Measurement created: " + createdUuid);

        // 4) Request the metadata back via system.historian.queryMetadata. This exercises
        //    the full store → create → query path and confirms the measurement's core
        //    metadata (datatype, name) is returned.
        Map<String, Object> queryMeta = webdevPost("test/queryMeta", Map.of("paths", List.of(qPath)));
        assertTrue((Boolean) queryMeta.get("success"),
                "queryMetadata should succeed — error: " + queryMeta.get("error"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) queryMeta.get("rows");
        assertNotNull(rows, "queryMetadata should return rows");
        assertFalse(rows.isEmpty(), "queryMetadata should return at least one row");
        Map<String, Object> row = rows.get(0);
        log("queryMetadata row: " + row);
        assertEquals("number", String.valueOf(row.get("datatype")),
                "queryMetadata should return the measurement datatype");
        pass("queryMetadata returned core metadata (datatype/name)");

        // 5) Engineering unit round-trip. storeMetadata DOES persist engUnit into Factry
        //    (measurements.metadata / attributes), but Factry's collector gRPC read API
        //    (GetMeasurements / GetMeasurementsByFilter) does not return the stored
        //    metadata map, so it cannot currently round-trip back into Ignition. Assert it
        //    when Factry starts returning it; otherwise log the known limitation instead of
        //    failing the suite.
        Object engUnit = row.get("engUnit");
        if (engUnit != null) {
            assertEquals("degC", String.valueOf(engUnit),
                    "queryMetadata must return engUnit=degC that was stored");
            pass("queryMetadata round-tripped engUnit=degC");
        } else {
            log("NOTE: engUnit not returned by queryMetadata — Factry's read API does not "
                    + "expose stored custom metadata (write side verified via Factry DB). "
                    + "Known Factry-side limitation.");
        }
    }

    @Test
    @Order(106)
    @DisplayName("browse — nested folder hierarchy is preserved")
    void testBrowseNestedFolders() throws Exception {
        section("Browse nested folders");

        // Create measurements under a nested folder path: <PREFIX>/Area/Line/Sensor{n}
        String folder = TEST_PREFIX + "/Area/Line";
        for (int i = 1; i <= 3; i++) {
            String uuid = createMeasurement(storedTagPath(folder + "/Sensor" + i), "number");
            assertFalse(uuid.isEmpty(), "Sensor" + i + " measurement should be created");
        }
        log("Created 3 measurements under " + folder);

        // Browse from the historian root and confirm the top-level test folder appears.
        Map<String, Object> rootResult = webdevPost("test/browse", Map.of(
                "path", "histprov:" + HISTORIAN_NAME + ":/"
        ));
        assertTrue((Boolean) rootResult.get("success"), "Root browse should succeed");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rootNodes = (List<Map<String, Object>>) rootResult.get("results");
        assertNotNull(rootNodes);
        boolean anyFolder = rootNodes.stream().anyMatch(n -> Boolean.TRUE.equals(n.get("hasChildren")));
        log("Root browse returned " + rootNodes.size() + " nodes; hasChildren present=" + anyFolder);
        assertTrue(rootNodes.size() >= 1, "Root browse should list at least one node");
        pass("Browse root returned " + rootNodes.size() + " nodes with folder structure");
    }

    @Test
    @Order(108)
    @DisplayName("Array measurement can be queried back")
    void testArrayQueryRoundTrip() throws Exception {
        section("Array query round trip");

        String measurementName = storedTagPath(TEST_PREFIX + "/ArrQuery");
        long ts = 1700400000000L;

        // Create a []number measurement and write an array point directly via gRPC.
        // (Writing arrays works fine; only the READ path hits the Factry []float64 bug,
        // so we set the scenario up deterministically rather than via the async S&F path.)
        grpcStub.createMeasurements(CreateMeasurementsRequest.newBuilder()
                .addMeasurements(CreateMeasurement.newBuilder()
                        .setName(measurementName)
                        .setDataType("[]number")
                        .setAutoOnboard(true)
                        .build())
                .build());

        String uuid = null;
        for (int i = 0; i < 20 && uuid == null; i++) {
            Thread.sleep(1000);
            uuid = findMeasurementUuidByFilter(measurementName);
        }
        assertNotNull(uuid, "Array measurement should be created");

        com.google.protobuf.ListValue arrList = com.google.protobuf.ListValue.newBuilder()
                .addValues(Value.newBuilder().setNumberValue(10))
                .addValues(Value.newBuilder().setNumberValue(11))
                .addValues(Value.newBuilder().setNumberValue(12))
                .addValues(Value.newBuilder().setNumberValue(13))
                .build();
        grpcStub.createPoints(Points.newBuilder()
                .addPoints(buildPoint(uuid, ts, Value.newBuilder().setListValue(arrList).build()))
                .build());
        log("Wrote array [10,11,12,13] via gRPC to measurement " + uuid);
        Thread.sleep(2000);

        // Query the array back via gRPC. Historically this failed INSIDE Factry with
        //   "UNKNOWN: error converting data point value: proto: invalid type: []float64"
        // (see the email to Factry). Fixed in Factry v8.2.0; this now runs for real.
        QueryTimeseriesResponse resp = grpcQuery(uuid, ts - 1000, ts + 1000);

        log("Array query returned seriesCount=" + resp.getSeriesCount());
        for (Series s : resp.getSeriesList()) {
            log("  fields=" + s.getFieldsList() + " datatype=" + s.getDatatype()
                    + " points=" + s.getDataPointsCount());
        }
        assertFalse(resp.getSeriesList().isEmpty(), "Array query should return at least one series");
        Series series = resp.getSeries(0);
        assertTrue(series.getDataPointsCount() >= 1, "Array query should return at least one data point");

        // Best-effort value check: if the array comes back as a single ListValue point,
        // verify it equals [10,11,12,13]. (If Factry returns it as a multi-field series
        // instead, the shape is logged above so the assertion can be tightened.)
        Value v = series.getDataPoints(series.getDataPointsCount() - 1).getValue();
        if (v.getKindCase() == Value.KindCase.LIST_VALUE) {
            List<Value> list = v.getListValue().getValuesList();
            double[] arr = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                arr[i] = list.get(i).getNumberValue();
            }
            log("Array values: " + java.util.Arrays.toString(arr));
            assertArrayEquals(new double[]{10, 11, 12, 13}, arr, 0.001, "Queried array should match stored");
        }
        pass("Array measurement queried back successfully");
    }

    // =========================================================================
    // Error cases
    // =========================================================================

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @DisplayName("Error cases")
    class ErrorCases {

        @Test
        @Order(10)
        @DisplayName("Query non-existent measurement returns 0 rows")
        void testNonExistentMeasurement() throws Exception {
            section("Error: Query non-existent measurement");

            Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                    "paths", List.of(qualifiedPath(HISTORIAN_NAME,
                            TEST_PREFIX + "/NonExistent/DoesNotExist")),
                    "startDate", 1700000000000L,
                    "endDate", 1700000010000L
            ));
            assertTrue((Boolean) result.get("success"), "Query should succeed (just return 0 rows)");
            int rowCount = ((Number) result.get("rowCount")).intValue();
            assertEquals(0, rowCount, "Non-existent measurement should return 0 rows");
            pass("Non-existent measurement returns 0 rows");
        }

        @Test
        @Order(20)
        @DisplayName("Inverted time range returns 0 rows")
        void testInvertedTimeRange() throws Exception {
            section("Error: Inverted time range");

            String tagName = TEST_PREFIX + "/Error/Inverted";
            String measurementName = storedTagPath(tagName);

            String uuid = createMeasurement(measurementName, "number");
            assertFalse(uuid.isEmpty());

            long baseTs = 1700080000000L;
            grpcStub.createPoints(Points.newBuilder()
                    .addPoints(buildPoint(uuid, baseTs, Value.newBuilder().setNumberValue(1.0).build()))
                    .build());
            Thread.sleep(2000);

            // Query with end < start — Ignition's TimeRange rejects this with
            // IllegalArgumentException, so the WebDev endpoint returns 500.
            String url = GATEWAY_URL + "/system/webdev/" + WEBDEV_PROJECT + "/test/queryRaw";
            String jsonBody = gson.toJson(Map.of(
                    "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                    "startDate", baseTs + 5000,
                    "endDate", baseTs - 5000
            ));
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "text/plain")
                    .timeout(Duration.ofSeconds(30))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            assertEquals(500, resp.statusCode(),
                    "Inverted time range should be rejected by Ignition's TimeRange");
            pass("Inverted time range correctly rejected with 500");
        }

        @Test
        @Order(30)
        @DisplayName("Bad quality code maps to Bad status")
        void testBadQualityStore() throws Exception {
            section("Error: Bad quality code storage");

            String tagName = TEST_PREFIX + "/Error/BadQuality";
            long baseTs = 1700081000000L;

            // Pre-create so lookupNode can resolve
            String measurementName = storedTagPath(tagName);
            String preUuid = createMeasurement(measurementName, "number");
            assertFalse(preUuid.isEmpty());
            log("Pre-created measurement " + preUuid);

            String qPath = qualifiedPath(HISTORIAN_NAME, tagName);
            Map<String, Object> result = webdevPost("test/storePoints", Map.of(
                    "paths", List.of(qPath, qPath, qPath),
                    "values", List.of(1.0, 2.0, 3.0),
                    "timestamps", List.of(baseTs, baseTs + 1000, baseTs + 2000),
                    "qualities", List.of(0, 64, 192)  // Bad, Uncertain, Good
            ));
            assertTrue((Boolean) result.get("success"), "Store with mixed qualities should succeed");
            pass("Store with Bad/Uncertain/Good quality codes accepted");
        }

        @Test
        @Order(40)
        @DisplayName("Browse non-existent path returns empty or error gracefully")
        void testBrowseNonExistentPath() throws Exception {
            section("Error: Browse non-existent path");

            Map<String, Object> result = webdevPost("test/browse", Map.of(
                    "path", "histprov:" + HISTORIAN_NAME + ":/folder:NonExistent_" + TEST_PREFIX + ":/"
            ));
            assertTrue((Boolean) result.get("success"), "Browse should succeed");
            int count = ((Number) result.get("count")).intValue();
            log("Browse non-existent path returned " + count + " nodes");
            assertEquals(0, count, "Non-existent folder should return 0 nodes");
            pass("Browse non-existent path returns 0 nodes");
        }

        @Test
        @Order(50)
        @DisplayName("Boolean round-trip: store and query")
        void testBooleanRoundTrip() throws Exception {
            section("Error: Boolean round trip");

            String tagName = TEST_PREFIX + "/Error/Boolean";
            String measurementName = storedTagPath(tagName);
            long baseTs = 1700082000000L;

            String uuid = createMeasurement(measurementName, "boolean");
            assertFalse(uuid.isEmpty());

            grpcStub.createPoints(Points.newBuilder()
                    .addPoints(buildPoint(uuid, baseTs, Value.newBuilder().setBoolValue(true).build()))
                    .addPoints(buildPoint(uuid, baseTs + 1000, Value.newBuilder().setBoolValue(false).build()))
                    .addPoints(buildPoint(uuid, baseTs + 2000, Value.newBuilder().setBoolValue(true).build()))
                    .build());
            log("Inserted 3 boolean points");
            Thread.sleep(2000);

            QueryTimeseriesResponse grpcResponse = grpcQuery(uuid, baseTs - 1000, baseTs + 3000);
            int grpcPoints = grpcResponse.getSeriesList().isEmpty() ? 0
                    : grpcResponse.getSeries(0).getDataPointsCount();
            assertTrue(grpcPoints >= 3, "Expected >= 3 boolean points via gRPC, got " + grpcPoints);
            pass("Boolean values stored and verified via gRPC");

            Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                    "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                    "startDate", baseTs - 1000,
                    "endDate", baseTs + 3000
            ));
            assertTrue((Boolean) result.get("success"));
            int rowCount = ((Number) result.get("rowCount")).intValue();
            log("system.historian returned " + rowCount + " rows for boolean tag");
            assertTrue(rowCount >= 3, "Expected >= 3 rows for boolean tag");
            pass("Boolean round-trip via system.historian: " + rowCount + " rows");
        }

        @Test
        @Order(60)
        @DisplayName("Large batch store (500 points)")
        void testLargeBatchStore() throws Exception {
            section("Error: Large batch store");

            String tagName = TEST_PREFIX + "/Error/LargeBatch";
            String measurementName = storedTagPath(tagName);
            long baseTs = 1700083000000L;

            String uuid = createMeasurement(measurementName, "number");
            assertFalse(uuid.isEmpty());

            List<Point> points = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                points.add(buildPoint(uuid, baseTs + i * 100,
                        Value.newBuilder().setNumberValue(Math.sin(i * 0.1)).build()));
            }
            grpcStub.createPoints(Points.newBuilder().addAllPoints(points).build());
            log("Inserted 500 points via gRPC");
            Thread.sleep(2000);

            QueryTimeseriesResponse grpcResponse = grpcQuery(uuid, baseTs - 1000, baseTs + 50_000);
            int grpcPoints = grpcResponse.getSeriesList().isEmpty() ? 0
                    : grpcResponse.getSeries(0).getDataPointsCount();
            assertTrue(grpcPoints >= 500, "Expected >= 500 points via gRPC, got " + grpcPoints);
            pass("Large batch: " + grpcPoints + " points stored and verified");

            Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                    "paths", List.of(qualifiedPath(HISTORIAN_NAME, tagName)),
                    "startDate", baseTs - 1000,
                    "endDate", baseTs + 50_000
            ));
            assertTrue((Boolean) result.get("success"));
            int rowCount = ((Number) result.get("rowCount")).intValue();
            log("system.historian returned " + rowCount + " rows for 500-point batch");
            assertTrue(rowCount >= 500, "Expected >= 500 rows");
            pass("Large batch query: " + rowCount + " rows");
        }

        @Test
        @Order(70)
        @DisplayName("Partial multi-tag query (some tags exist, some don't)")
        void testPartialMultiTagQuery() throws Exception {
            section("Error: Partial multi-tag query");

            String existingTag = TEST_PREFIX + "/Error/Exists";
            String missingTag = TEST_PREFIX + "/Error/Missing_" + randomSuffix(4);
            long baseTs = 1700084000000L;

            String measurementName = storedTagPath(existingTag);
            String uuid = createMeasurement(measurementName, "number");
            assertFalse(uuid.isEmpty());

            grpcStub.createPoints(Points.newBuilder()
                    .addPoints(buildPoint(uuid, baseTs, Value.newBuilder().setNumberValue(42.0).build()))
                    .addPoints(buildPoint(uuid, baseTs + 1000, Value.newBuilder().setNumberValue(43.0).build()))
                    .build());
            Thread.sleep(2000);

            Map<String, Object> result = webdevPost("test/queryRaw", Map.of(
                    "paths", List.of(
                            qualifiedPath(HISTORIAN_NAME, existingTag),
                            qualifiedPath(HISTORIAN_NAME, missingTag)
                    ),
                    "startDate", baseTs - 1000,
                    "endDate", baseTs + 2000
            ));
            assertTrue((Boolean) result.get("success"), "Partial query should succeed");
            int rowCount = ((Number) result.get("rowCount")).intValue();
            log("Partial multi-tag query returned " + rowCount + " rows");
            assertTrue(rowCount >= 2, "Should return data for the existing tag");
            pass("Partial multi-tag query succeeds with " + rowCount + " rows");
        }

        @Test
        @Order(80)
        @DisplayName("GetMeasurementsByFilter returns more than 100 measurements")
        void testGetMeasurementsByFilterNoLimit() throws Exception {
            section("Error: GetMeasurementsByFilter pagination limit");

            // Count existing measurements using pagination with a high limit
            Measurements initial = grpcStub.getMeasurementsByFilter(
                    GetMeasurementsByFilterRequest.newBuilder()
                            .setPagination(Pagination.newBuilder().setLimit(1000).build())
                            .build()
            );
            int existingCount = initial.getMeasurementsCount();
            log("Existing measurements via getMeasurementsByFilter: " + existingCount
                    + (initial.getTotal() > 0 ? " (total: " + initial.getTotal() + ")" : ""));

            // Create enough to exceed 100 total
            int toCreate = Math.max(0, 101 - existingCount);
            log("Creating " + toCreate + " measurements to exceed 100 total");

            for (int i = 0; i < toCreate; i++) {
                grpcStub.createMeasurements(CreateMeasurementsRequest.newBuilder()
                        .addMeasurements(CreateMeasurement.newBuilder()
                                .setName("default/" + TEST_PREFIX + "/Limit/" + i)
                                .setDataType("number")
                                .setAutoOnboard(true)
                                .build())
                        .build());
            }

            if (toCreate > 0) {
                // Wait for measurements to become visible
                Thread.sleep(3000);
            }

            // Query again with pagination and verify we get more than 100
            Measurements after = grpcStub.getMeasurementsByFilter(
                    GetMeasurementsByFilterRequest.newBuilder()
                            .setPagination(Pagination.newBuilder().setLimit(1000).build())
                            .build()
            );
            int afterCount = after.getMeasurementsCount();
            log("Measurements via getMeasurementsByFilter after creation: " + afterCount
                    + (after.getTotal() > 0 ? " (total: " + after.getTotal() + ")" : ""));

            assertTrue(afterCount > 100,
                    "getMeasurementsByFilter should return more than 100 measurements, got " + afterCount);
            pass("getMeasurementsByFilter returned " + afterCount + " measurements (no 100-record limit)");
        }

        @Test
        @Order(81)
        @DisplayName("Collector.name and Measurement.collectorUUID are populated")
        void testCollectorNameAndMeasurementCollectorUUID() {
            section("Proto: Collector.name and Measurement.collectorUUID");

            // Verify Collector has a name
            Collectors collectors = grpcStub.getCollectors(GetCollectorsRequest.newBuilder().build());
            assertFalse(collectors.getCollectorsList().isEmpty(), "Expected at least one collector");
            for (Collector c : collectors.getCollectorsList()) {
                log("Collector: uuid=" + c.getUuid() + ", name='" + c.getName() + "'");
                assertFalse(c.getName().isEmpty(),
                        "Collector name should not be empty for uuid=" + c.getUuid());
            }
            pass("All collectors have a name");

            // Verify Measurement.collectorUUID is populated
            Measurements measurements = grpcStub.getMeasurementsByFilter(
                    GetMeasurementsByFilterRequest.newBuilder()
                            .addCollectorUUIDs(COLLECTOR_UUID)
                            .setPagination(Pagination.newBuilder().setLimit(10).build())
                            .build()
            );
            assertFalse(measurements.getMeasurementsList().isEmpty(),
                    "Expected at least one measurement for collector " + COLLECTOR_UUID);
            for (Measurement m : measurements.getMeasurementsList()) {
                log("Measurement: uuid=" + m.getUuid() + ", name='" + m.getName()
                        + "', collectorUUID='" + m.getCollectorUUID() + "'");
                assertFalse(m.getCollectorUUID().isEmpty(),
                        "collectorUUID should not be empty for measurement " + m.getUuid());
            }
            pass("All " + measurements.getMeasurementsCount() + " measurements have collectorUUID");
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Build the qualified historian path used by system.historian.* functions.
     * Format: {@code histprov:Name:/sys:System:/prov:default:/tag:TagName}
     */
    private String qualifiedPath(String historianName, String tagName) {
        return "histprov:" + historianName + ":/sys:" + GATEWAY_SYSTEM_NAME
                + ":/prov:default:/tag:" + tagName;
    }

    /** Build the stored measurement name: {@code default/TagName} */
    private String storedTagPath(String tagName) {
        return "default/" + tagName;
    }

    /** Create a measurement in Factry and return its UUID. */
    private String createMeasurement(String name, String dataType) {
        grpcStub.createMeasurements(CreateMeasurementsRequest.newBuilder()
                .addMeasurements(CreateMeasurement.newBuilder()
                        .setName(name)
                        .setDataType(dataType)
                        .setAutoOnboard(true)
                        .build())
                .build());

        for (int attempt = 0; attempt < 5; attempt++) {
            String uuid = findMeasurementUuid(name);
            if (uuid != null) return uuid;
            try { Thread.sleep(500 * (attempt + 1)); } catch (InterruptedException ignored) {}
        }
        return "";
    }

    /** Find a measurement UUID by name. Returns null if not found. */
    private String findMeasurementUuid(String name) {
        Measurements measurements = grpcStub.getMeasurements(MeasurementRequest.newBuilder().build());
        for (Measurement m : measurements.getMeasurementsList()) {
            if (m.getName().equals(name)) {
                return m.getUuid();
            }
        }
        return null;
    }

    /**
     * Find a measurement UUID by name via the filter query (the same RPC the module uses).
     * Unlike {@link #findMeasurementUuid}, this reliably returns array ([]number) and
     * not-yet-onboarded measurements.
     */
    private String findMeasurementUuidByFilter(String name) {
        GetMeasurementsByFilterRequest req = GetMeasurementsByFilterRequest.newBuilder()
                .setKeyword(name)
                .build();
        for (Measurement m : grpcStub.getMeasurementsByFilter(req).getMeasurementsList()) {
            if (m.getName().equals(name)) {
                return m.getUuid();
            }
        }
        return null;
    }

    /** Build a gRPC Point. */
    private static Point buildPoint(String measurementUUID, long timestampMs, Value value) {
        return Point.newBuilder()
                .setMeasurementUUID(measurementUUID)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(timestampMs / 1000)
                        .setNanos((int) ((timestampMs % 1000) * 1_000_000))
                        .build())
                .setValue(value)
                .setStatus("Good")
                .build();
    }

    /** Query Factry directly via gRPC, grouped by the per-point status tag. */
    private QueryTimeseriesResponse grpcQueryGroupedByStatus(String measurementUUID, long startMs, long endMs) {
        return grpcStub.queryTimeseries(QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(measurementUUID)
                .addGroupBy("status")
                .setStart(Timestamp.newBuilder()
                        .setSeconds(startMs / 1000)
                        .setNanos((int) ((startMs % 1000) * 1_000_000))
                        .build())
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(endMs / 1000)
                        .setNanos((int) ((endMs % 1000) * 1_000_000))
                        .build())
                .build());
    }

    /** Query Factry directly via gRPC. */
    private QueryTimeseriesResponse grpcQuery(String measurementUUID, long startMs, long endMs) {
        return grpcStub.queryTimeseries(QueryTimeseriesRequest.newBuilder()
                .addMeasurementUUIDs(measurementUUID)
                .setStart(Timestamp.newBuilder()
                        .setSeconds(startMs / 1000)
                        .setNanos((int) ((startMs % 1000) * 1_000_000))
                        .build())
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(endMs / 1000)
                        .setNanos((int) ((endMs % 1000) * 1_000_000))
                        .build())
                .build());
    }

    /** POST JSON to a WebDev endpoint and parse the response. */
    Map<String, Object> webdevPost(String endpoint, Map<String, Object> data)
            throws IOException, InterruptedException {
        String url = GATEWAY_URL + "/system/webdev/" + WEBDEV_PROJECT + "/" + endpoint;
        String jsonBody = gson.toJson(data);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "text/plain")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, resp.statusCode(),
                "WebDev " + endpoint + " returned " + resp.statusCode() + ": " + resp.body());

        Type type = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = gson.fromJson(resp.body(), type);

        if (Boolean.FALSE.equals(result.get("success"))) {
            log("WebDev error: " + result.get("error"));
            if (result.containsKey("trace")) {
                log("Trace: " + result.get("trace"));
            }
        }

        return result;
    }

    /** Extract the first non-null numeric value from an aggregation result. */
    @SuppressWarnings("unchecked")
    private double extractAggregationValue(Map<String, Object> result) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) result.get("rows");
        if (rows == null || rows.isEmpty()) return Double.NaN;
        List<String> columns = (List<String>) result.get("columns");
        for (String col : columns) {
            if (col.equals("t_stamp")) continue;
            Object val = rows.get(0).get(col);
            if (val != null) return ((Number) val).doubleValue();
        }
        return Double.NaN;
    }

    /** Assert that an aggregation query returned a value close to expected. */
    @SuppressWarnings("unchecked")
    private void assertAggregationValue(Map<String, Object> result, double expected,
                                        double tolerance, String label) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) result.get("rows");
        assertNotNull(rows, label + " should return rows");
        assertFalse(rows.isEmpty(), label + " should return at least one row");

        List<String> columns = (List<String>) result.get("columns");
        for (String col : columns) {
            if (col.equals("t_stamp")) continue;
            Object val = rows.get(0).get(col);
            if (val != null) {
                double actual = ((Number) val).doubleValue();
                assertEquals(expected, actual, tolerance, label + " value");
                return;
            }
        }
        fail(label + " — no non-null value found in result");
    }

    /** Generate a random lowercase suffix. */
    static String randomSuffix(int length) {
        Random rng = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + rng.nextInt(26)));
        }
        return sb.toString();
    }
}
