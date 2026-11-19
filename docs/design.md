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
- **Deadlines**: every call has one, so a Factry that never answers cannot hang the module: 3 s for store lookups, 3 s plus 1 ms per point or measurement (at most 30 s) for writes and measurement creation, 30 s for history queries, each cache refresh page and the asset fetches, 10 s for the other small calls (configurable via `ModuleProperties`). The connection test has a fixed 5 s
- **Dead connections**: no keepalive pings, because Factry closes a connection that pings without a call running (`too_many_pings`). When `testConnection()` times out, the channel drops its connection with `enterIdle()` so the next call opens a new one
- **Auth headers**: every call includes `collectoruuid` + `authorization: Bearer <token>` metadata
- **Read/write lock**: prevents race conditions during `reconfigure()` — old channel is shut down and replaced atomically. `reconfigure()` and `shutdown()` wait at most 3 s for running calls, then cancel them
- **Reconfiguration**: `handleSettingsChange()` calls `reconfigure()` to swap the channel without restarting the module

### `MeasurementCache.java`

**Location:** `gateway/src/main/java/.../gateway/MeasurementCache.java`

In-memory cache of Factry measurements and assets. Both the storage engine and query engine depend on this.

- `tagPathToUUID` — maps stored tag path (e.g., `Ignition-abc:[default]Temperature`) to Factry UUID
- `uuidToMeasurement` — maps UUID to full `Measurement` proto object
- `refresh(grpcClient)` — fetches all measurements from Factry, replaces the maps. Only keeps measurements with `status=Active`
- `getOrCreateUUID(tagPath, grpcClient, value)` — cache lookup with auto-creation: on miss, creates the measurement in Factry, polls until visible, returns UUID. Includes concurrent creation prevention (`pendingCreations` set). The data type comes from the tag's Ignition `dataType` metadata when known (Store and Forward hands Boolean values over as Long 0/1), else from the value. Dates are stored as epoch milliseconds in a `number` measurement, and the tag's Ignition data type is written to the measurement settings (`ignitionDataType`, a Date value without metadata counts as `DateTime`) so queries can return Dates. NaN and Infinity are written as a Bad point without a value, and Longs above 2^53 as the nearest double, with a warning once per tag
- `evictByUUID(uuid)` — removes a measurement so it gets re-created on next write (used when Factry rejects points for deleted measurements)
- Before creating a measurement, the write path looks its exact name up in Factry and reuses an Active one. Measurements that are not Active (Discovered, ...) are kept in `inactiveByName`, for the write path only. Values for a tag whose measurement exists only in another status are discarded (`WriteRefusal.NOT_ACTIVE`): counted as handled, so store and forward does not retry them, with one warning per flush naming the newly refused tags and a count in `HistorianMetrics.getDiscardedPointCount()`. The measurement is not created again
- `writeRefusal(measurement, collectorUUID)` — the one check of whether the module may write to a measurement. Factry answers OK to points written to another collector's measurement and drops them, so the write paths only use measurements of the module's own collector (or without a collector UUID). Browse and query keep every collector's measurements; when several have the same name (one per time-series database), the cache keeps the module's own under that name, so query by name prefers it too. When the measurement with a tag's name is another collector's (`WriteRefusal.OTHER_COLLECTOR`), the module asks Factry once per tag to create its own, since the other may be in another database, and polls until one of its own appears. If none Active appears, the tag's values are discarded like a `NOT_ACTIVE` tag's. A measurement of its own with that name in another status (the Discovered duplicate such a try leaves) counts as tried, so a restart does not try again
- Paused measurements of the module's own collector (Factry's way to tell collectors to stop collecting) are refused as `WriteRefusal.PAUSED`: values for them are dropped and counted as handled (`HistorianMetrics.getPausedDroppedPointCount()`), with one warning per flush naming the newly paused tags and one info line when a tag is written again. The measurement is never created again. Array tags keep merging their element changes while paused, so the first value after the resume is the whole current array. From the first cache refresh after the measurement is Active again, new values are stored. Another collector's Paused measurement counts as `OTHER_COLLECTOR`

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
- a measurement whose `ignitionDataType` setting is `DateTime` → `DataType.DateTime`. Factry keeps the dates as epoch milliseconds; the query engine returns them as `java.util.Date`. Aggregates of such a tag: LastValue, Minimum, Maximum, MinMax, Average and SimpleAverage are Dates, Count is a number, and Sum, Range, Variance and StdDev fail the key with `Bad_Unsupported`. Measurements created before this setting existed read back as numbers. To have such a measurement read back as Dates and keep its history, add the setting in Factry instead of deleting the measurement: `GET /api/measurements/{uuid}`, then `PUT /api/measurements/{uuid}` with the same `Attributes`, `CollectorUUID`, `DatabaseUUID`, `Datatype`, `Description`, `Metadata`, `Name` and `Status`, and `Settings` plus `"ignitionDataType": "DateTime"`. The module reads Dates from its next cache refresh. The bulk `PATCH /api/measurements` refuses it ("Setting ignitionDataType is not common to all selected measurements"). Checked on Factry v8.2.3.
- Array tags get their Ignition type in the setting too (e.g. `DateTimeArray`), but the read side does not return array values yet. Only `DateTime` is checked, so when array read-back is added, the elements of a `DateTimeArray` measurement must be turned into Dates as well.

### `ModuleProperties.java`

**Location:** `gateway/src/main/java/.../gateway/ModuleProperties.java`

Loads tuning values from `factry-historian.properties` (bundled in the .modl). These are not exposed in the UI — they're for operational tuning:

| Property | Default | Controls |
|----------|---------|----------|
| `grpc.write.deadline.seconds` | 3 | Timeout on store lookups; base timeout on `createPoints()` and `createMeasurements()` |
| `grpc.write.deadline.ms.per.point` | 1 | Added to the `createPoints()` timeout per point, and to the `createMeasurements()` timeout per measurement |
| `grpc.write.deadline.max.seconds` | 30 | Ceiling on the `createPoints()` and `createMeasurements()` timeouts |
| `grpc.write.gzip` | true | Send `createPoints()` gzip-compressed. If Factry has no gzip decompressor, the client resends the batch uncompressed and stops using gzip until the historian settings are saved |
| `grpc.read.deadline.seconds` | 30 | Timeout on history queries, on each page of the measurement cache refresh, and on the asset and asset property fetches |
| `grpc.call.deadline.seconds` | 10 | Timeout on the small calls: collectors, health, collector state |
| `grpc.shutdown.grace.seconds` | 3 | How long a settings save or shutdown waits for running calls before it cancels them |
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

Steps 2 to 7 live in `PointWriter`, which the engine calls; it uses no Ignition gateway classes, so unit tests can run it against a fake Factry client.

**`isEngineUnavailable()`** — returns `true` when the gRPC connection is down. This is how S&F knows to buffer instead of attempting writes.

**`doStoreMetadata()`**: caches each tag's metadata in `MeasurementCache`; it sends nothing to Factry by itself. When the module creates a measurement for the tag, its data type, description, engineering units and limits (the `Config` attributes) and the `ignitionDataType` setting come from this cache. The cache keeps the metadata after the create, so a failed create or a measurement deleted in Factry is created again with it: Ignition may not send it again.

**`applySourceChanges()`**: drops the cached metadata of retired and renamed tags. Measurements in Factry are left alone.

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
| Average | `twa` with fill `previous`, over Good time only (see below) |
| SimpleAverage | `mean` |
| Minimum | `min` |
| Maximum | `max` |
| Sum | `sum` |
| Count | `count` |
| LastValue | `last` |
| Range | `spread` |
| Variance | `variance` |
| StdDev | `stddev` |
| MinMax | queries `min` and `max` separately, emits pairs |

- Count of a tag that is not numeric (String, DateTime) gets a number column: Ignition types a Count column after the tag, so it would otherwise return the text `"600"` or a Date in 1970
- Time window → Go duration conversion (e.g., `TimeWindow(2h30m)` → `"2h30m"`, 600 ms → `"600ms"`). The bucket length in milliseconds and the period string come from the same number.

**Average (time-weighted, `GoodTimeAverage`)**:
- Factry's `twa` is only right with fill `previous`: it then holds each value until the next sample, also from before the range. Its buckets start at the query start.
- `twa` refuses group by, so Bad and Uncertain time is masked in the module. A `last` aggregate grouped by status and the last sample before the range tell per bucket: Good throughout → Factry's `twa`; not Good throughout → a Bad point without a value (null); status changes inside → the bucket's raw samples are averaged over Good time.
- Raw reads have a budget per pen: 50,000 samples per request, 200,000 and 10 requests per pen, spent from the range end backwards so the most recent buckets are exact. Neighbouring buckets share a request. A `count` aggregate (only when some bucket needs raw samples) sizes the requests. When the status at a bucket start is unknown (the bucket before held several statuses and was not read), the last sample before it is read: always at the start of a raw request, and up to 10 times per pen for buckets outside one.
- A bucket beyond the budget gets `twa` over the samples with status exactly `Good`, returned as Good (the Power Chart drops values that are not Good). It leaves Bad values out but holds the last Good value through Bad time and misses Good sub-statuses.
- Requests per pen: 3 for a healthy tag (`twa`, `last`, sample before); at most 35 (plus `count`, up to 10 raw, 20 one-sample reads, 1 fallback). Raw samples: at most 200,000.
- The range stops at now: the current bucket is weighted up to now, and buckets after now are Bad.
- Every bucket gets a point. Ignition fills a bucket without a point with a neighbouring value marked Good (checked through its own query path, and live on 8.3.6), so an outage, the time before the first sample and the time after now would read as a held Good value. They are Bad points instead, which the Power Chart draws as gaps and `queryTagHistory` returns with Bad quality (a null reads as 0.0 in a Float column). Not NaN: the Power Chart streams history as strict JSON, which fails at a NaN.
- Booleans: Factry's `twa` refuses them, so they are averaged from raw samples as the fraction of Good time true (true = 1). A `mean` per status replaces `twa`: a Good mean of exactly 0 or 1 means the bucket holds one value, so it needs no raw read when that is the value held. Beyond the budget: that `mean`, no extra request, returned as Uncertain because a sample mean can be far off for a Boolean stored on change (0.5 instead of 0.017 for a bit on 1 minute of the hour). The Power Chart draws Uncertain values as gaps; with neighbouring buckets sharing a request and on-change storage this should be rare. At most 34 requests. Factry 8.2.3 accepts `last`, `count`, `mean`, `min` and `max` on a Boolean. Strings keep `mean`.
- Difference with Ignition's internal historian: the module leaves all Bad and Uncertain time out (the owner's choice) and holds the previous value until the next sample (steps). Ignition's own time-weighted average uses trapezoids between values and bridges Bad time inside a block, leaving out only Bad time at the block's start and end. So the numbers differ on ramps and across short Bad stretches.

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

**Metadata only seeds new measurements:** `FactryStorageEngine.doStoreMetadata()` caches the tag metadata, and it is applied only when the module creates a measurement (data type, description, engineering units and limits, `ignitionDataType` setting). Later changes to a tag's metadata in Ignition are not pushed to an existing measurement; edit those on the Factry platform.

**Pagination:** The design document lists pagination as a provider requirement. In practice, there is no paginated use case — charts receive the full dataset and Jython scripts get a complete `DataSet` object. The gRPC proto does define `limit`/`offset` fields on `QueryTimeseriesRequest`, so pagination can be added if Factry ever imposes response size limits.

**Browse filters and optional query parameters:** The design document's appendix lists optional parameters for `browse`, `queryRawPoints`, and `queryAggregatedPoints` (e.g. `returnSize`, `includeBounds`, `fillModes`, `nameFilters`). These are part of the Ignition scripting API — the framework parses them and passes relevant options to the query engine. Our implementation handles the core parameters (time range, aggregation type, time window). The optional parameters are either handled by the Ignition framework itself or are not relevant for the Factry backend.

### Known Limitation: String Data in Raw Queries

The query engine correctly returns string data points to the Ignition framework (confirmed via `HistorianMetrics` counters), but Ignition 8.3's `system.historian.queryRawPoints` produces a `DataSet` with 0 rows for string-typed measurements. This appears to be a framework-level limitation in how `DataSet` handles non-numeric values. Numeric and boolean data types work correctly.
