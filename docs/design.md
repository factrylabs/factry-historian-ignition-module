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
| `migrateMeasurementSettings(list)` | `GetCollector`, then `MigrateSettings` | Store a tag's data type and Deadband Style on its existing measurement. Replaces each measurement's whole settings Struct and the collector's own settings, so the collector's current settings are read just before and sent along |
| `getInfo()` | `GetInfo` | Factry's version, checked before `MigrateSettings` |
| `queryTimeseries(QueryTimeseriesRequest)` | `QueryTimeseries` | Read raw or aggregated data |
| `getAssets()` | `GetAssets` | List all assets |
| `testConnection()` | `GetMeasurements` (lightweight) | Connection health check |

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

It also carries the tag's Ignition data type and Deadband Style, when known, to pick how empty buckets are filled (`derivedFill`, see **Fill per Deadband Style** under `FactryQueryEngine.java`).

### `ModuleProperties.java`

**Location:** `gateway/src/main/java/.../gateway/ModuleProperties.java`

Loads tuning values from `factry-historian.properties` (bundled in the .modl). These are not exposed in the UI — they're for operational tuning:

| Property | Default | Controls |
|----------|---------|----------|
| `grpc.write.deadline.seconds` | 3 | Timeout on `createPoints()` calls |
| `status.cache.ms` | 30000 | How long to cache historian status (avoids gRPC poll on every UI refresh) |
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

**`doStoreMetadata()`**: caches each tag's metadata in `MeasurementCache` and marks the tag pending; it asks Factry nothing by itself. Metadata without a Deadband Style counts as Auto: with Include Metadata on, Ignition can leave a style at its default out. When the module creates a measurement for the tag, its description, engineering units and limits (the `Config` attributes) and the settings `ignitionDataType` (the tag's Ignition data type) and `ignitionDeadbandStyle` (its History Deadband Style) come from this cache. The cache keeps the metadata after the create.

**Keeping the settings up to date** (`MeasurementCache.syncTagSettings`): the settings the module derives from a tag's metadata (`MeasurementCache.derivedSettings`: `ignitionDataType` and `ignitionDeadbandStyle`) are kept up to date on the measurement this collector writes the tag to. The style is stored as the concrete mode, as the SQL historian stores it (`InterpolationMode.getConcreteMode` for the tag's type, the element type for an array): Auto becomes Analog_Compressed for a Float and Discrete otherwise, so another gateway that does not know the type still reads it right. Ignition sends the style at every start and on every change of a tag's history configuration, even with Include Metadata off. After each 30 s cache refresh the scheduler compares each pending tag's derived settings with its measurement's and sends `MigrateSettings` for those that differ: at most 500 measurements per call and 2 calls per run. Safeguards:
- Right before the calls, this collector's measurements are listed once with `GetMeasurementsByFilter` (collector filter, paged). Each entry is built from that fresh copy plus the derived settings, so a setting an operator changed in Factry is kept (the call replaces the whole Struct). If the listing fails, nothing is sent and the tags stay pending. A measurement the cache holds but the listing lacks stays pending; a tag whose measurement the cache no longer holds, or that is another collector's, is dropped.
- Only Active and Paused measurements are sent. While the cache has not loaded, nothing is compared and the tags stay pending.
- Only on a Factry version `MigrateSettings` was checked live on, v8.2.3 to v8.3.x (from `GetInfo`, asked before anything else). The version is read again after a failed connection check or a settings change.
- After PermissionDenied (a proxy in front of Factry may refuse the call on purpose) or Unimplemented, no more calls are made until the profile restarts, logged once at INFO. Other failures are logged once per gRPC status code and tried again at the next run. A failure changes nothing else.
- `MigrateSettings`, checked live on Factry v8.2.3 and v8.3.1: it replaces the collector's own settings with its field 1, and an unset field 1 wipes them, so the module reads them with `GetCollector` just before each call and sends them along. Each measurement's settings Struct is replaced, not merged. `updatedAt` does not move. The settings sent are kept over a cache refresh whose listing was read during the call.

**`doStoreSourceChange()`** — no-op.

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
| MinMax | queries `min` and `max` separately, a pair per bucket |

**Other aggregates (`BucketFill`)**: every aggregate except Average and Count gives every bucket a point, like Average does. Ignition fills a bucket without a point with a neighbouring value marked Good, so a Bad stretch, the time before the first sample and the empty buckets of a tag stored on change read a value that was never there (Good 7 at 10:00:05 and 8 at 10:00:45, LastValue in 10 s buckets, read `7, 8, 8, 8, 8, 8` instead of `7, 7, 7, 7, 8, 8`).
- Good samples in the bucket: Factry's aggregate over those samples, grouped by status with only the Good series kept. A bucket with Good and Bad samples gives the value of its Good samples (LastValue: the last Good value). Several Good statuses in one bucket (`Good`, `GoodLocalOverride`; only data from other collectors has them, the module writes plain `Good`) give one point: the smallest minimum, the largest maximum, the summed sum. LastValue reads the last sample of each status before the bucket end to find the latest Good one. SimpleAverage, Range, Variance and StdDev cannot be combined from per-status values, so such a bucket gives the value of one status, marked Uncertain (the Power Chart shows it as a gap).
- No samples, and the status held from before is Good: the value held, as Ignition's own aggregates take the value at the block start. Range is 0 there; Sum, Variance and StdDev have no value (Bad), as in Ignition's own functions. For an analog tag see **Fill per Deadband Style** below.
- Otherwise (only Bad or Uncertain samples, a status held that is not Good, before the first sample, and from now on): a Bad point without a value. MinMax gives each bucket a minimum and a maximum point, Bad ones too.
- Requests per pen: Factry's `last` per status (which is the LastValue itself) says which statuses each bucket holds and the value held after it, plus the aggregate per status (two for MinMax). The last sample before a bucket is read when the range starts with empty buckets (once), after a bucket holding several statuses when an empty bucket follows, and for LastValue at the end of a bucket with several Good statuses: up to 10 such reads per pen. Past that an empty bucket after a bucket holding several statuses is Bad (a gap is safer than a guess), and such a LastValue is Uncertain.
- A String tag has only LastValue and Count: Factry 8.2.3 refuses every other aggregate of a string measurement, so the engine answers them with `Bad_Unsupported` (Ignition shows it as `Bad_NotFound`) and they read null on every row.
- Time window → Go duration conversion (e.g., `TimeWindow(2h30m)` → `"2h30m"`, 600 ms → `"600ms"`). The bucket length in milliseconds and the period string come from the same number.

**Average (time-weighted, `GoodTimeAverage`)**:
- Factry's `twa` is only right with fill `previous`: it then holds each value until the next sample, also from before the range. Its buckets start at the query start.
- `twa` refuses group by, so Bad and Uncertain time is masked in the module. A `last` aggregate grouped by status and the last sample before the range tell per bucket: Good throughout → Factry's `twa`; not Good throughout → a Bad point without a value (null); status changes inside → the bucket's raw samples are averaged over Good time.
- Raw reads have a budget per pen: 50,000 samples per request, 200,000 and 10 requests per pen, spent from the range end backwards so the most recent buckets are exact. Neighbouring buckets share a request. A `count` aggregate (only when some bucket needs raw samples) sizes the requests. When the status at a bucket start is unknown (the bucket before held several statuses and was not read), the last sample before it is read: always at the start of a raw request, and up to 10 times per pen for buckets outside one.
- A bucket beyond the budget gets `twa` over the samples with status exactly `Good`, returned as Good (the Power Chart drops values that are not Good). It leaves Bad values out but holds the last Good value through Bad time and misses Good sub-statuses.
- Requests per pen: 3 for a healthy tag (`twa`, `last`, sample before); at most 35 (plus `count`, up to 10 raw, 20 one-sample reads, 1 fallback). Raw samples: at most 200,000.
- The range stops at now: the current bucket is weighted up to now, and buckets after now are Bad.
- Every bucket gets a point. Ignition fills a bucket without a point with a neighbouring value marked Good (checked through its own query path, and live on 8.3.6), so an outage, the time before the first sample and the time after now would read as a held Good value. They are Bad points instead, which the Power Chart draws as gaps and `queryTagHistory` returns with Bad quality (a null reads as 0.0 in a Float column). Not NaN: the Power Chart streams history as strict JSON, which fails at a NaN.
- Booleans: Factry's `twa` refuses them, so they are averaged from raw samples as the fraction of Good time true (true = 1). A `mean` per status replaces `twa`: a Good mean of exactly 0 or 1 means the bucket holds one value, so it needs no raw read when that is the value held. Beyond the budget: that `mean`, no extra request, returned as Uncertain because a sample mean can be far off for a Boolean stored on change (0.5 instead of 0.017 for a bit on 1 minute of the hour). The Power Chart draws Uncertain values as gaps; with neighbouring buckets sharing a request and on-change storage this should be rare. At most 34 requests. Factry 8.2.3 accepts `last`, `count`, `mean`, `min` and `max` on a Boolean. Average of a String tag is unsupported (see above).
- An analog tag's buckets without samples lie on the straight line between the samples around them (see **Fill per Deadband Style**).
- Difference with Ignition's internal historian: the module leaves all Bad and Uncertain time out (the owner's choice) and holds the previous value until the next sample (steps). Ignition's own time-weighted average uses trapezoids between values and bridges Bad time inside a block, leaving out only Bad time at the block's start and end. So the numbers differ on ramps and across short Bad stretches.

**Fill per Deadband Style**: the charts (Easy Chart, Power Chart) read 300 MinMax points by default, and every aggregate but Count is native, so Ignition draws the module's bucket points as they are. Ignition's own historians fill a bucket without samples per the tag's History Deadband Style (`QueryUtils.ColumnMapping.getFillMode`, the SQL historian's interpolator): an analog tag on the straight line between its samples, a discrete one held. Holding every tag drew analog pens as a staircase that lags the SQL historian's line. So each pen gets one fill mode (`FactryQueryEngine.fillMode`, `FactryRecord.derivedFill`):
- The query's own fill mode, unless it is `DERIVED` (which the charts and `queryTagHistory` send). Only `LINEAR` changes what the module returns; the others hold as before.
- String and Boolean tags hold, whatever their style: the SQL historian never interpolates a value that is not a number (`DatasourceQueryExecutor.createInterpolator`).
- Else the Deadband Style: Discrete holds; Analog, `Analog_Deadband` and `Analog_Compressed` are linear. Auto or unknown goes by the tag's Ignition type: a Float (Float4, Float8 or their arrays) is linear; integer, DateTime, Boolean and String tags hold. A line is drawn only for a tag known to be analog.
- Linear gives each bucket without samples one value, on the straight line from the last sample before its run of empty buckets to the first sample after it, each at its own time. Interpolating each function on its own drew a band that never happened (Good 0 at 0 s, 100 at 5 s and 100 at 60 s read 20/100, 40/100, ... as MinMax). Factry's aggregates are stamped at the bucket start, so they do not say when a bucket's last or first sample was: with up to 4 runs of empty buckets (`linearEdgeReadRuns`) the module reads, per run, the last sample of the bucket before it (descending, limit 1 per status) and the first sample of the bucket after it (ascending, limit 1 per status). With more, as for a tag stored once a minute in a chart of 300 points over an hour (60 runs), it reads the span from the bucket before the first run to the bucket after the last raw once, limit 10,000 per status (`linearRawLimit`); only when that read reaches the limit it falls back to the one-sample reads, for at most 10 runs per pen (`linearRunsPerPen`), and the runs past them hold. A run at the range start uses the sample before the range, already read for the held value; after the last run of a past range the first sample after the range end is read. Toward now the last value holds: nothing is extrapolated. The line needs the latest sample on each side to be Good: a run next to Bad time holds, and so do the runs past the cap and those whose read fails (logged once at DEBUG): a failed read never fails a pen or the query, it only costs the line. Sum, Range, Variance, StdDev and Count are never interpolated.
- The value per function follows Ignition's own aggregates for an empty analog block: LastValue, MinMax, Minimum, Maximum and SimpleAverage all take the line at the bucket end (or now, or the range end), stamped at the bucket start; the value at the bucket start would kink the line at every sample (a ramp sampled at 11 s and 71 s, in 12 s buckets, reads 11, 24, 36, 48, 60, 71). Average takes the line's average over the bucket, its value at the middle of the part averaged (up to the range end or now). Buckets with samples keep Factry's stepped `twa`.
- The style comes from the metadata Ignition sends this gateway (`historicalDeadbandStyle`, kept in `MeasurementCache` by `doStoreMetadata`), which is current on the gateway that historizes the tag, else from the measurement setting `ignitionDeadbandStyle`, for other gateways and before the metadata arrives. The tag's Ignition data type comes the same way (`dataType`, else `ignitionDataType`). The metadata only counts when the measurement is the one this collector writes the tag to: another collector's measurement with the same name may be another gateway's tag.
- `getProcessorProperties` also tells Ignition the style (`TagHistoryProps.HistoricalDeadbandStyle`, Discrete or Analog, when the style or the Ignition type is known), as Core does, so the aggregates Ignition computes itself from raw points hold a discrete tag too.
- When neither the style nor the Ignition type is known the tag holds, as before: another collector's measurement, an asset property, a measurement older than the settings read on another gateway, or a stored plain `Auto` without a type.

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

**`getStorageEngine()`** — returns S&F bridge if configured, otherwise the direct storage engine

**`getStatus()`** — cached gRPC health check (30s TTL)

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

**Metadata only seeds new measurements:** `FactryStorageEngine.doStoreMetadata()` caches the tag metadata, and it is applied when the module creates a measurement (description, engineering units and limits, `ignitionDataType` and `ignitionDeadbandStyle` settings). Later changes to a tag's metadata in Ignition are not pushed to an existing measurement, except the settings derived from it (`ignitionDataType`, `ignitionDeadbandStyle`, with `MigrateSettings`); edit the rest on the Factry platform.

**Pagination:** The design document lists pagination as a provider requirement. In practice, there is no paginated use case — charts receive the full dataset and Jython scripts get a complete `DataSet` object. The gRPC proto does define `limit`/`offset` fields on `QueryTimeseriesRequest`, so pagination can be added if Factry ever imposes response size limits.

**Browse filters and optional query parameters:** The design document's appendix lists optional parameters for `browse`, `queryRawPoints`, and `queryAggregatedPoints` (e.g. `returnSize`, `includeBounds`, `fillModes`, `nameFilters`). These are part of the Ignition scripting API — the framework parses them and passes relevant options to the query engine. Our implementation handles the core parameters (time range, aggregation type, time window). The optional parameters are either handled by the Ignition framework itself or are not relevant for the Factry backend.

### Known Limitation: String Data in Raw Queries

The query engine correctly returns string data points to the Ignition framework (confirmed via `HistorianMetrics` counters), but Ignition 8.3's `system.historian.queryRawPoints` produces a `DataSet` with 0 rows for string-typed measurements. This appears to be a framework-level limitation in how `DataSet` handles non-numeric values. Numeric and boolean data types work correctly.
