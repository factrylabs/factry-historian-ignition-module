# Design

A guided tour through the Factry Historian architecture, class overview, detailed class infos. 

# Terminology:

  - JVM - Java Virtual Machine or Java engine, this is the runner
  - Java and Kotlin compiled to bytecode. This is a platform independent small instruction set code(~200 opcodes) to run in the JVM, it is in the file .class 
  - extensions:
       *.java - java code
       *.kts - kotlin code
       *.class - bytecode  
  - Gradle - Gradle Build Tool is a build automation tool (language idependent for example Java, C/C++, and JavaScript)
             Ignition official tool
  - JIT - compiler inside the JVM
  - JRE - JVM + standard libraries
  - JDK - JRE + dev tools for development
  - Jython -> it is a python syntax code, but it compiles to bytecode as well
  
---

## 1. Project Structure

### Gradle Setup

| File | Purpose |
|------|---------|
| `settings.gradle` | Declares the `:gateway` subproject, configures Inductive Automation Maven repositories |
| `build.gradle.kts` (root) | Module packaging: name, ID, version, scope mappings, hook registrations, module dependencies, signing. Also defines deploy tasks (`copy`, `restart`) |
| `gateway/build.gradle.kts` | Gateway dependencies (Ignition SDK, Historian API, gRPC/protobuf), protobuf compilation, unit test and integration test source sets |

### Key Gradle Commands

```bash
./gradlew clean build          # Build the .modl file
./gradlew test                 # Unit tests
./gradlew integrationTest      # Integration tests (requires running infrastructure)
./gradlew copy                 # Copy .modl to Ignition modules dir
./gradlew restart              # Copy + restart Ignition container
./gradlew printVersion         # Print current version string
```

### Folder Structure

```
factry-historian-module/
├── gateway/src/main/java/     # All historian logic lives here
├── gateway/src/main/proto/    # (generated from proto/)
├── gateway/src/test/java/     # Unit tests
├── gateway/src/integrationTest/java/  # Integration tests
├── proto/historian/            # Protobuf definitions (shared with Factry)
├── ignition/data/              # Docker-mounted Ignition data (WebDev scripts)
├── script/                     # Utility scripts (scan.sh)
└── docs/                       # Documentation
```







### Scope Architecture

Ignition modules have scopes that determine where code runs:

| Subproject | Scope | Runs in |
|------------|-------|---------|
| `:gateway` | G | Gateway only |

All historian logic (gRPC, storage, queries) is gateway-scoped.

### Class Overview

![Class Diagram](class_diagram.excalidraw.svg)


### Statistics


```bash
  ┌────────────────────────────────────┬───────┐
  │                File                │ Lines │
  ├────────────────────────────────────┼───────┤
  │ FactryQueryEngine.java             │ 705   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistoryProvider.java         │ 304   │
  ├────────────────────────────────────┼───────┤
  │ TagPathUtil.java                   │ 239   │
  ├────────────────────────────────────┼───────┤
  │ FactryGrpcClient.java              │ 235   │
  ├────────────────────────────────────┼───────┤
  │ FactryStorageEngine.java           │ 219   │
  ├────────────────────────────────────┼───────┤
  │ MeasurementCache.java              │ 205   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistorianSettings.java       │ 174   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistorianExtensionPoint.java │ 152   │
  ├────────────────────────────────────┼───────┤
  │ HistorianMetrics.java              │ 118   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistorianConfig.java         │  85   │
  ├────────────────────────────────────┼───────┤
  │ JwtTokenParser.java                │  83   │
  ├────────────────────────────────────┼───────┤
  │ FactryRecord.java                  │  64   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistorianGatewayHook.java    │  61   │
  ├────────────────────────────────────┼───────┤
  │ ModuleProperties.java              │  58   │
  ├────────────────────────────────────┼───────┤
  │ FactryHistorianModule.java         │  23   │
  ├────────────────────────────────────┼───────┤
  │ Total (15 files)                   │ 2,725 │
  └────────────────────────────────────┴───────┘
```

## 2. Gateway — Adapter

These classes handle communication with Factry Historian over gRPC.

### `FactryGrpcClient.java`

**Location:** `gateway/src/main/java/.../gateway/FactryGrpcClient.java`

The single point of contact with Factry. Wraps a gRPC `ManagedChannel` and exposes typed methods:

| Method | Proto RPC | Purpose |
|--------|-----------|---------|
| `createPoints(Points)` | `CreatePoints` | Write time-series data |
| `getMeasurements(MeasurementRequest)` | `GetMeasurements` | List all measurements |
| `createMeasurements(CreateMeasurementsRequest)` | `CreateMeasurements` | Auto-onboard new measurements |
| `queryTimeseries(QueryTimeseriesRequest)` | `QueryTimeseries` | Read raw or aggregated data |
| `getAssets()` | `GetAssets` | List all assets |
| `testConnection()` | `GetCollector` (5 s deadline) | Connection and token check; not `GetMeasurements`, which is too slow on large sites |

**Key design points:**
- **TLS modes**: plain text, TLS with bundled CA cert, TLS with insecure trust (skip verification), fallback to system trust store
- **Connection tracking**: `connected` flag set to `false` on `createPoints()` failure, checked by `StorageEngine.isEngineUnavailable()` so S&F can buffer without hitting the network
- **Write deadline**: 3-second timeout on writes (configurable via `ModuleProperties`)
- **Auth headers**: every call includes `collectoruuid` + `authorization: Bearer <token>` metadata
- **Read/write lock**: prevents race conditions during `reconfigure()` — old channel is shut down and replaced atomically
- **Reconfiguration**: `handleSettingsChange()` calls `reconfigure()` to swap the channel without restarting the module

### `MeasurementCache.java`

**Location:** `gateway/src/main/java/.../gateway/MeasurementCache.java`

In-memory cache of Factry measurements and assets. Both the storage engine and query engine depend on this.

- `tagPathToUUID` — maps stored tag path (e.g., `Ignition-abc:[default]Temperature`) to Factry UUID
- `uuidToMeasurement` — maps UUID to full `Measurement` proto object
- `refresh(grpcClient)` — fetches all measurements from Factry, replaces the maps. Only keeps measurements with `status=Active`
- `getOrCreateUUID(tagPath, grpcClient, value)` — cache lookup with auto-creation: on miss, creates the measurement in Factry, polls until visible, returns UUID. Includes concurrent creation prevention (`pendingCreations` set) and data type detection from the value
- `evictByUUID(uuid)` — removes a measurement so it gets re-created on next write (used when Factry rejects points for deleted measurements)

**Refreshed every 30 seconds** by `FactryHistoryProvider`'s scheduled executor.

### `proto/historian/historian.proto`

**Location:** `proto/historian/historian.proto`

The protobuf contract shared with Factry. Compiled by the `protobuf` Gradle plugin into Java stubs under `gateway/build/generated/source/proto/`. Key messages: `Point`, `Measurement`, `Asset`, `Series`, `QueryTimeseriesRequest/Response`.

---

## 3. Gateway — Configuration

### `FactryHistorianConfig.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistorianConfig.java`

A Java `record` that defines the **Gateway UI form** for creating/editing a historian profile. Uses Ignition's annotation-based form system:

| Field | Category | Type | Default |
|-------|----------|------|---------|
| `token` | Connection | textarea | *(empty)* |
| `useTls` | Connection | checkbox | `true` |
| `skipTlsVerification` | Connection | checkbox | `false` |
| `batchSize` | Advanced | number | `100` |
| `batchIntervalMs` | Advanced | number | `5000` |
| `debugLogging` | Advanced | checkbox | `false` |
| `storeAndForwardEngine` | Advanced | text | *(empty)* |

The `toSettings()` / `fromSettings()` methods convert between the UI record and the internal `FactryHistorianSettings` object.

**Note:** The token is the only required connection field. Host, port, and collector UUID are extracted from the JWT automatically.

### `FactryHistorianSettings.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistorianSettings.java`

Internal settings object implementing `HistorianSettings`. Holds all configuration plus derived fields:

- `applyTokenDefaults(token)` — parses the JWT to extract `grpcHost`, `grpcPort`, `collectorUUID`
- `validate()` — checks that collector ID, host, port (1-65535), batch size (>0), batch interval (>0) are valid

This is what the engines and gRPC client receive. The separation from `FactryHistorianConfig` allows internal fields (like parsed JWT claims) that aren't shown in the UI.

---

## 4. Gateway — Utilities

### `JwtTokenParser.java`

**Location:** `gateway/src/main/java/.../gateway/JwtTokenParser.java`

Parses the Factry collector JWT token **without signature verification** (the module trusts the token, Factry validates it server-side). Extracts:
- `uuid` — collector UUID
- `aud` — gRPC host
- `grpc-port` — gRPC port

Used by `FactryHistorianSettings.applyTokenDefaults()`.

### `TagPathUtil.java`

**Location:** `gateway/src/main/java/.../gateway/TagPathUtil.java`

Converts between two tag path formats:

| Format | Example | Used by |
|--------|---------|---------|
| Stored (measurement name) | `Ignition-abc:[default]Folder/Tag` | Factry, MeasurementCache |
| Display (UI/query) | `Ignition-abc/default/Folder/Tag` | Ignition tag browser, Power Chart |

Key methods:
- `qualifiedPathToStoredPath(String)` — converts Ignition's `QualifiedPath` string to stored format. Handles multiple input formats: direct queries (`/sys:X:/prov:Y:/tag:Z`), browse-originated paths (`/tag:X/Y/Z`), folder-based paths, and category prefixes (`Measurements/...`, `Assets/...`)
- `storedPathToDisplayPath(String)` — reverse: stored format back to display format for browse tree rendering
- `buildStoredPath(sys, prov, tag)` — the core formula: `sys + ":[" + prov + "]" + tag`

Has comprehensive unit tests in `TagPathUtilTest.java`.

### `FactryRecord.java`

**Location:** `gateway/src/main/java/.../gateway/FactryRecord.java`

Implements `HistoricalNode` — the Ignition SDK's representation of a browsable tag node. Created by the query engine's browse logic. Maps Factry data types to Ignition `DataType`:
- `"boolean"` → `DataType.Boolean`
- `"number"` → `DataType.Float8`
- `"string"` → `DataType.String`

### `ModuleProperties.java`

**Location:** `gateway/src/main/java/.../gateway/ModuleProperties.java`

Loads tuning values from `factry-historian.properties` (bundled in the .modl). These are not exposed in the UI — they're for operational tuning:

| Property | Default | Controls |
|----------|---------|----------|
| `grpc.write.deadline.seconds` | 3 | Timeout on `createPoints()` calls |
| `status.cache.ms` | 30000 | How often the connection check runs; it pauses S&F while Factry is unreachable and resumes it when it is back |
| `measurement.cache.refresh.seconds` | 30 | How often to reload measurements from Factry |

---

## 5. Gateway — Logic

### `FactryStorageEngine.java`

**Location:** `gateway/src/main/java/.../gateway/FactryStorageEngine.java`

Extends `AbstractStorageEngine`. Receives data points from Ignition's tag system and writes them to Factry.

**`doStoreAtomic(points)`** — the main method, called by Ignition (or S&F bridge):
1. Iterates over `AtomicPoint` list
2. Converts each point's `QualifiedPath` to stored tag path via `TagPathUtil`
3. Looks up (or auto-creates) the measurement UUID via `MeasurementCache`
4. Converts value to protobuf `Value` (Boolean, Number, String)
5. Maps Ignition `QualityCode` to status string ("Good", "Uncertain", "Bad")
6. Sends `CreatePoints` gRPC call
7. On failure: distinguishes connection errors (UNAVAILABLE, DEADLINE_EXCEEDED) from server rejections. On rejection, evicts the measurement UUID and retries once (handles deleted measurements)

**`isEngineUnavailable()`** — returns `true` when the gRPC connection is down. This is how S&F knows to buffer instead of attempting writes.

**`doStoreMetadata()`** / **`doStoreSourceChange()`** — no-ops. Metadata is managed on the Factry platform, not pushed from Ignition.

### `FactryQueryEngine.java`

**Location:** `gateway/src/main/java/.../gateway/FactryQueryEngine.java`

Extends `AbstractQueryEngine`. Handles all data retrieval from Factry.

**`doQueryRaw(options, processor)`**:
1. Maps each `RawQueryKey` to stored tag path → measurement UUID (via `lookupUUID`)
2. `lookupUUID` checks cache, then refreshes from Factry on miss
3. Builds `QueryTimeseriesRequest` with measurement UUIDs and time range
4. Iterates response `Series` → `SeriesPoint`, converts protobuf values to Java, creates `AtomicPoint` objects, feeds them to the `RawPointProcessor`

**`doQueryAggregated(options, processor)`**:
- Same pattern but adds aggregation function and interval to the request
- Supports 11 aggregation types mapped to Factry function names:

| Ignition | Factry |
|----------|--------|
| Average, SimpleAverage | `mean` |
| Minimum | `min` |
| Maximum | `max` |
| Sum | `sum` |
| Count | `count` |
| LastValue | `last` |
| Range | `spread` |
| Variance | `variance` |
| StdDev | `stddev` |
| MinMax | queries `min` and `max` separately, emits pairs |

- Time window → Go duration conversion (e.g., `TimeWindow(2h30m)` → `"2h30m0s"`)

**`doBrowse(browseContext, publisher)`**:
- Root level: publishes two category folders — "Measurements" and "Assets"
- Measurements: parses stored paths into hierarchical sys/prov/tag structure, publishes folders and leaf nodes
- Assets: splits asset names on "/" to build folder hierarchy
- Uses `BrowsePublisher` to emit folder and tag nodes

**`doQueryMetadata(options, processor)`**:
- Returns measurement metadata: data type, status, name, engineering specs (UOM, hi/lo limits)

**`lookupUUID(tagPath)`**:
- Cache hit → return
- Cache miss → `measurementCache.refresh()` → retry
- Checks both measurements and assets

---

## 6. Gateway — Integration

These classes plug the module into Ignition's extension point system.

### `FactryHistorianGatewayHook.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistorianGatewayHook.java`

The module's entry point. Extends `AbstractGatewayModuleHook`. Ignition instantiates this when the module loads.

- `setup()` — saves `GatewayContext`, registers resource bundles for localization
- `startup()` — no-op (historian instances are created on-demand by the extension point, not hardcoded)
- `shutdown()` — removes resource bundles
- `getExtensionPoints()` — returns the single `FactryHistorianExtensionPoint`
- `isFreeModule()` — returns `true` (no Ignition license check)

### `FactryHistorianExtensionPoint.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistorianExtensionPoint.java`

Extends `HistorianExtensionPoint<FactryHistorianSettings>`. Registers "Factry Historian" as a selectable historian type in **Config > Tags > History > Historians**.

- `settingsType()` — tells Ignition how to deserialize the config JSON → `FactryHistorianSettings.class`
- `defaultSettings()` — returns `Optional.empty()` intentionally. Returning defaults would cause the UI form to wipe the `config.profile.type` field (Ignition 8.3 bug workaround)
- `getWebUiComponent()` — builds the config form from `FactryHistorianConfig`'s annotated fields
- `createHistorianProvider()` — factory method called by Ignition on startup or when a user creates a new profile. Extracts settings, creates a `FactryHistoryProvider`

### `FactryHistoryProvider.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistoryProvider.java`

Extends `AbstractHistorian<FactryHistorianSettings>`. The orchestrator that ties everything together. Each historian profile in the gateway UI is one instance.

**Startup sequence (`onStartup()`)**:
1. Refresh measurement cache from Factry
2. If S&F engine configured:
   - Create `TagHistoryDataSinkBridge` (wraps StorageEngine for S&F)
   - Register sink with S&F manager
   - Force-initialize sink via reflection (workaround for Ignition 8.3.x issue where late-registered sinks stay stuck in "Storage Only")
   - Create `TagHistoryStorageEngineBridge` (routes writes through S&F instead of direct)
3. Start scheduled executor with 3 recurring tasks:
   - Metrics logging (every 30s)
   - Measurement cache refresh (every 30s)
   - Quarantine retry (every 30s, S&F only)
   - Connection check (every 30s, `GetCollector`): pauses the S&F sink while Factry is unreachable, resumes it when it is back

**`getStorageEngine()`** — returns S&F bridge if configured, otherwise the direct storage engine

**`getStatus()`** — returns the result of the last scheduled connection check

**`handleSettingsChange()`** — reconfigures gRPC client, refreshes cache, updates engines. No restart needed.

**`handleNameChange()`** — returns `true` (no-op). Measurement names in Factry don't include the historian profile name.

**`retryQuarantinedData()`** — moves quarantined S&F records back to pending. Ignition never auto-retries quarantined data, but our failures are almost always transient connection issues.

---

## 7. Gateway — Observability

### `HistorianMetrics.java`

**Location:** `gateway/src/main/java/.../gateway/HistorianMetrics.java`

Thread-safe counters using `AtomicLong`. Tracks:

| Metric | Description |
|--------|-------------|
| Store ops / points / errors / elapsed ms | Write performance |
| Raw query ops / rows / elapsed ms | Raw read performance |
| Agg query ops / rows / elapsed ms | Aggregated read performance |

`logSummary()` — writes a single-line summary every 30 seconds:
```
Metrics | store: 5 ops, 120 pts (4.0 pts/s), 450 ms total, 0 errors | raw query: ...
```

`recordStore()`, `recordRawQuery()`, `recordAggQuery()` — called by the engines after each operation.

---

## 8. Tests

### Unit Tests

| File | Tests |
|------|-------|
| `TagPathUtilTest.java` | Path conversion: stored ↔ display, QualifiedPath parsing, folder handling, edge cases |
| `FactryHistorianSettingsTest.java` | JWT parsing, token defaults, validation |
| `HistorianMetricsTest.java` | Counter increments, summary formatting |

Run with `./gradlew test`.

### Integration Tests

| File | Tests |
|------|-------|
| `FactryIntegrationTest.java` | End-to-end: gRPC insert + WebDev query, store via Ignition, aggregation, multi-tag, empty query, string values, round trip |

Run with `./gradlew integrationTest`. Requires running Docker infrastructure. See [integration_test.md](integration_test.md).

---

## 9. Proto → Java Data Flow (Example)

To illustrate how the pieces connect, here is the path a single tag value takes from Ignition to Factry:

```
Tag value changes in Ignition
  │
  ▼
FactryHistoryProvider.getStorageEngine()
  │ (returns StorageBridge if S&F, else StorageEngine directly)
  ▼
FactryStorageEngine.doStoreAtomic(points)
  │
  ├─ TagPathUtil.qualifiedPathToStoredPath(source)
  │   → "Ignition-abc:[default]Temperature"
  │
  ├─ MeasurementCache.getOrCreateUUID(tagPath, grpcClient, value)
  │   → "a16cac76-3272-..."
  │
  ├─ Point.newBuilder()
  │     .setMeasurementUUID(uuid)
  │     .setTimestamp(Timestamp)
  │     .setValue(Value.newBuilder().setNumberValue(23.5))
  │     .setStatus("Good")
  │     .build()
  │
  └─ FactryGrpcClient.createPoints(Points)
       │
       └─ gRPC → Factry Historian → InfluxDB
```

---

## 10. Remarks



### A few items were intentionally left out or deferred:

**Explicitly out of scope:**
- **Annotations** — not supported by the Factry platform
- **History renaming** — not supported by the Factry platform
- **Extended scripting** (e.g. `system.factry.queryEvents`) — deferred until customer demand exists

**Store metadata is a no-op:** `FactryStorageEngine.doStoreMetadata()` accepts the call and returns success, but does not forward metadata to Factry. Metadata (engineering units, limits, etc.) is managed on the Factry platform directly, not pushed from Ignition.

**Pagination:** The design document lists pagination as a provider requirement. In practice, there is no paginated use case — charts receive the full dataset and Jython scripts get a complete `DataSet` object. The gRPC proto does define `limit`/`offset` fields on `QueryTimeseriesRequest`, so pagination can be added if Factry ever imposes response size limits.

**Browse filters and optional query parameters:** The design document's appendix lists optional parameters for `browse`, `queryRawPoints`, and `queryAggregatedPoints` (e.g. `returnSize`, `includeBounds`, `fillModes`, `nameFilters`). These are part of the Ignition scripting API — the framework parses them and passes relevant options to the query engine. Our implementation handles the core parameters (time range, aggregation type, time window). The optional parameters are either handled by the Ignition framework itself or are not relevant for the Factry backend.

### Known Limitation: String Data in Raw Queries

The query engine correctly returns string data points to the Ignition framework (confirmed via `HistorianMetrics` counters), but Ignition 8.3's `system.historian.queryRawPoints` produces a `DataSet` with 0 rows for string-typed measurements. This appears to be a framework-level limitation in how `DataSet` handles non-numeric values. Numeric and boolean data types work correctly.
