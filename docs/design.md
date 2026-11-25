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
| `getTimeseriesDatabases()` | `GetTimeseriesDatabases` | List the time-series databases, to tell Sites apart |
| `getDefaultDatabaseUUID()` | REST `GET /api/collectors/<uuid>` | The collector's default database (`FactryRestClient`; the only REST call, since gRPC does not give it). 3 s deadline; same trust as the gRPC channel |
| `testConnection()` | `GetCollector` (5 s deadline) | Connection and token check; not `GetMeasurements`, which is too slow on large sites |

**Key design points:**
- **TLS**: on unless **Plain Text (No TLS)** is on. A Historian serves plain gRPC only with `USE_SELF_SIGNED_CERTIFICATE=false` (Factry's option to turn gRPC TLS off, v8.1.0), usually behind a proxy that ends TLS. The checkbox is "Plain Text" rather than "Use TLS", off by default like every checkbox in the form: Ignition's create form shows no defaults (`defaultSettings()` is empty), so a box that defaults to on would look off while it saves on. When the token's `use-self-signed-certificate` claim is true, the module uses TLS even with Plain Text on, as Factry's own collectors do, so profiles saved with the old "Use TLS" off (its old default) keep working against a Factry with its built-in certificate. Skip TLS Verification and a Custom CA also mean TLS. Plain text logs a WARN that the token is sent unencrypted (gRPC and the REST call alike), and the label in the log says so. A plain text client against a TLS port fails with `Network closed for unknown reason`; only then, with the client in plain text, the hint says to turn Plain Text off. A TLS client against a plain port gets "does not answer with TLS", with "make a new collector token" when the token's claim forced TLS. The certificate check, in order: Skip TLS Verification (none); a Custom CA (system CAs plus the custom CA, name checked against the host); the token's `use-self-signed-certificate` claim (`FactryOrSystemTrustManager`: the certificate passes the normal check, system CAs against the host, or it chains to Factry's root CA, bundled as `factry-root-ca.crt`, and names `historian.factry.local`, whatever host is used; so a proxy with a Let's Encrypt or enterprise certificate in front of a Factry with the built-in one still passes; its two-argument `checkServerTrusted`, which knows no host, always refuses); otherwise the system CAs against the host. The module sends the real host as SNI and `:authority` on every path (no `overrideAuthority`), so a TLS-passthrough proxy routing by SNI sees the name it expects. Factry's built-in certificate and key are inside every Factry server binary, so that check proves "a Factry with the built-in certificate", not "your Factry"; it still stops a man in the middle with any other certificate, which skipping does not. For real verification give Factry (or a proxy in front of it) its own certificate and paste its CA. The old bundled `factry-historian-ca.crt` was a 2022 leaf certificate that matched no current Factry; it is gone
- **Connection hints**: a failed connection check that is a TLS problem adds what to change to the status details and the WARN (`FactryGrpcClient.tlsHint`): no TLS answer (check Host/Port Override; for a Factry with its gRPC TLS turned off, turn on Plain Text, turn off Skip TLS Verification and clear Custom CA Certificate; when the token's claim forced TLS, make a new collector token), plain text against a TLS port (turn Plain Text off; only when the client used plain text), a certificate that does not name the host (Host Override, or clear a Custom CA that holds Factry's own CA), an expired certificate or one not valid yet (renew it, check the clock), a certificate signed by Factry's CA that is not the built-in one (it names only `historian.factry.local`, so no host helps), a certificate from an unknown CA (Custom CA). The cause text in a status or log line is cut to 200 characters (`WriteHealth.shorten`), and Netty's NotSslRecordException (channel pipeline plus a hex dump of the reply) becomes just "not an SSL/TLS record"
- **Connection tracking**: `connected` flag set to `false` on `createPoints()` failure, checked by `StorageEngine.isEngineUnavailable()` so S&F can buffer without hitting the network
- **Deadlines**: every call has one, so a Factry that never answers cannot hang the module: 3 s for store lookups, 3 s plus 1 ms per point or measurement (at most 30 s) for writes and measurement creation, 30 s for history queries, each cache refresh page and the asset fetches, 10 s for the other small calls (configurable via `ModuleProperties`). The connection test has a fixed 5 s
- **Dead connections**: no keepalive pings, because Factry closes a connection that pings without a call running (`too_many_pings`). When `testConnection()` times out, the channel drops its connection with `enterIdle()` so the next call opens a new one
- **Auth headers**: every call includes `collectoruuid` + `authorization: Bearer <token>` metadata
- **Token stays out of the log**: Netty's frame logger `io.grpc.netty.shaded.io.grpc.netty.NettyClientHandler` writes every header at DEBUG, the token included. The client gives it its own INFO level before every call, so DEBUG on `io.grpc`, root or any level between does not reach it, also after a logback reload. That is why the Logs page shows INFO on this logger. Never set that exact logger to DEBUG on a production gateway.
- **Read/write lock**: prevents race conditions during `reconfigure()` — old channel is shut down and replaced atomically. `reconfigure()` and `shutdown()` wait at most 3 s for running calls, then cancel them
- **Reconfiguration**: `handleSettingsChange()` calls `reconfigure()` to swap the channel without restarting the module

### `MeasurementCache.java`

**Location:** `gateway/src/main/java/.../gateway/MeasurementCache.java`

In-memory cache of Factry measurements and assets. Both the storage engine and query engine depend on this.

- `tagPathToUUID` — maps stored tag path (e.g., `Ignition-abc:[default]Temperature`) to Factry UUID
- `uuidToMeasurement` — maps UUID to full `Measurement` proto object
- `refresh(grpcClient)` — fetches all measurements from Factry, database by database (`GetTimeseriesDatabases`, then `GetMeasurementsByFilter` with `databaseUUIDs`, plus one call for the Discovered ones, which are in no database yet, and a one-row call for the total, which loads the rest when some are in a database Factry did not list), and replaces the maps. Only keeps measurements with `status=Active` for browse and reads. The gRPC `Measurement` has no database field, so this is how the cache learns each one's Site
- **Sites**: several Sites can share one Factry organisation, each gateway with its own collector writing to its own time-series database, and the same tag then has the same measurement name at every Site (names are unique per database only). Every measurement whose database is known reads by the key `[<database name>]<name>`, for example `histprov:<historian>:/tag:[site_b]default/Line/Flow`, also from a script. A tag's own path (`sys:X:/prov:P:/tag:T`, name `P/T`) gives the bare name, which reads only:
  - this collector's measurement with that name (if it has one in two databases, the first listed one's; the other reads by `[database]name` and shows in browse with `[database]` after its leaf name), or
  - any collector's measurement in the own database: the collector's default database, which Factry's REST API gives (`GET /api/collectors/<uuid>`, `DefaultDatabaseUUID`, asked at each full reload with the same cleaned collector token as gRPC, on the token's `rest-port` at the gRPC host the profile uses (Host Override, else the token's host; Port Override is the gRPC port and does not apply), always over HTTPS with exactly the gRPC channel's certificate check (`FactryGrpcClient.trustManager`, one `X509TrustManager` for both; the JDK's HttpClient leaves the host name check to it, so Factry's built-in certificate passes by IP with the claim, without turning host name checks off), never plain HTTP; gRPC does not give it). The call has one 3 s deadline, body included, so an unreachable REST port costs each full reload at most 3 s. When it fails, the last answer for this collector stays; an answer without a default drops it. Without any answer, the module goes by where the collector's measurements are: if they are all in one database, that one; with only one database in the organisation, that one. A measurement whose database Factry does not list reads by tag path only in an organisation of one database.

  Otherwise a tag's own path reads nothing (`Bad_NotFound`), never another Site's data: a name only another Site has; without the REST answer, a collector with measurements in several databases (the module logs one WARN naming them), where other collectors' measurements read only by `[database]name`; and without the REST answer, a collector without measurements (a read-only gateway, or a new one before the full reload after its first measurement) in an organisation with several databases. One case still reads the wrong Site without the REST answer: a collector whose only measurements are in another Site's database (its first measurement made there by hand) takes that database as its own, so that Site's tags read by path. Factry answering the default database over gRPC would close it. Browse lists every Site under its collector's folder, each leaf with its key as node id, so every browsed tag reads the measurement it is listed as; a qualified leaf whose name is taken in its folder gets `[database]` after its name. A real name that looks like `[database]name` wins over that key, and a lookup tries such a name literally first. If Factry lists no databases, everything reads by its bare name, as before (said once in the log, and again only when that changes). Measurements in a database Factry does not list are kept: a one-row call compares the total, and the module loads all (logged at INFO) while it finds any.

  Limits: renaming a database breaks pens stored with `[old name]name` (`Bad_NotFound` from the next full reload); a new database reads by `[database]name` only from the next full reload; two databases with the same name collide; names with `]` or `:/` are not supported. Calculations belong to Factry's "Calculation collector" and are always in a database (Factry refuses an Active measurement without one), so they read by bare name only in the own database, like any other collector's.
- `sync(grpcClient)` — the scheduled update. Every 30 s it asks only for this collector's measurements changed since the newest `updatedAt` it has seen (`GetMeasurements` with `collectorUUID` and `since`; Factry compares strictly and also returns Paused and Deleted ones, checked live on 8.2.3), and applies them as a full refresh would: an Active one is cached, a Paused one refused, a Deleted one dropped so the next write creates it again. Every 10 minutes (`measurement.cache.full.refresh.seconds`), or while no full refresh has worked yet, it runs `refresh` instead. Measurements made by hand or by other collectors, and assets, show up in browse from that reload; a read by exact name finds them at once. The sync can miss a change Factry commits with an older `updatedAt` than one it already returned, and a measurement moved to another collector (it no longer shows up under this collector's `since`); the next full reload, at most 10 minutes later, puts both right
- `getOrCreateUUID(tagPath, grpcClient, value)` — cache lookup with auto-creation: on miss, creates the measurement in Factry, polls until visible, returns UUID. Includes concurrent creation prevention (`pendingCreations` set). The data type comes from the tag's Ignition `dataType` metadata when known (Store and Forward hands Boolean values over as Long 0/1), else from the value. Dates are stored as epoch milliseconds in a `number` measurement, and the tag's Ignition data type is written to the measurement settings (`ignitionDataType`, a Date value without metadata counts as `DateTime`) so queries can return Dates. NaN and Infinity are written as a Bad point without a value, and Longs above 2^53 as the nearest double, with a warning once per tag
- `evictByUUID(uuid)` — removes a measurement so it gets re-created on next write (used when Factry rejects points for deleted measurements)
- Before creating a measurement, the write path looks its exact name up in Factry and reuses an Active one. Measurements that are not Active (Discovered, ...) are kept in `inactiveByName`, for the write path only. Values for a tag whose measurement exists only in another status are discarded (`WriteRefusal.NOT_ACTIVE`): counted as handled, so store and forward does not retry them, with one warning per flush naming the newly refused tags and a count in `HistorianMetrics.getDiscardedPointCount()`. The measurement is not created again
- **Letter case**: Factry measurement names are case-sensitive (`Tank/Flow` and `Tank/flow` are two measurements), and its name search ignores case. A tag renamed in letter case only would otherwise start a second measurement. So when no measurement has a tag's exact name, the write path takes this collector's Active or Paused measurement whose name matches ignoring case, and a read takes the one measurement its key matches ignoring case, within the same Site rules (a tag's own path still never reads another Site's). The lookups keep such names. The exact name always wins: where both spellings already exist, each tag keeps its own. When several measurements match ignoring case, a read takes this collector's if it is the only one of its own (so a tag reads what it writes); otherwise none is used (as before: the write creates the exact name, the read finds nothing), with one WARN per name, again if the clash comes back after it was gone. A lowercase index, rebuilt at each full reload and added to by lookups, keeps this a constant-time step on a miss. Another collector's measurement, or one of this collector's that is only Discovered, is not taken this way. If the exact name exists in Factry but not yet in the cache, the other spelling is used until the next full reload (10 minutes)
- `writeRefusal(measurement, collectorUUID)` — the one check of whether the module may write to a measurement. Factry answers OK to points written to another collector's measurement and drops them, so the write paths only use measurements of the module's own collector (or without a collector UUID). Browse and reads use their own read keys (see Sites above). When the measurement with a tag's name is another collector's (`WriteRefusal.OTHER_COLLECTOR`), the module asks Factry once per tag to create its own, since the other may be in another database, and polls until one of its own appears. If none Active appears, the tag's values are discarded like a `NOT_ACTIVE` tag's. A measurement of its own with that name in another status (the Discovered duplicate such a try leaves) counts as tried, so a restart does not try again
- Paused measurements of the module's own collector (Factry's way to tell collectors to stop collecting) are refused as `WriteRefusal.PAUSED`: values for them are dropped and counted as handled (`HistorianMetrics.getPausedDroppedPointCount()`), with one warning per flush naming the newly paused tags and one info line when a tag is written again. The measurement is never created again. Array tags keep merging their element changes while paused, so the first value after the resume is the whole current array. From the first cache refresh after the measurement is Active again, new values are stored. Another collector's Paused measurement counts as `OTHER_COLLECTOR`

**Synced every 30 seconds** by `FactryHistoryProvider`'s scheduled executor, with a full reload every 10 minutes. Measured against a live Factry with 84 measurements, 10 minutes of the timer went from 60 calls and 262 KB (a full reload every 30 s) to 22 calls and 13 KB. At 20,000 measurements (about 145 bytes each) the full reload is about 2.9 MB, so about 0.4 GB a day instead of 8 GB.

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
| `collectorToken` | Connection | secret (Embedded or Referenced) | *(none)* |
| `hostOverride` | Connection | text | *(empty: the token's host)* |
| `portOverride` | Connection | number | `0` (the token's grpc-port) |
| `plainText` | Connection | checkbox | `false` |
| `skipTlsVerification` | Connection | checkbox | `false` |
| `batchSize` | Advanced | number | `100` |
| `batchIntervalMs` | Advanced | number | `5000` |
| `debugLogging` | Advanced | checkbox | `false` |
| `storeAndForwardEngine` | Advanced | text | *(empty)* |

The `toSettings()` / `fromSettings()` methods convert between the UI record and the internal `FactryHistorianSettings` object.

**Note:** The token is the only required connection field. Host, port, and collector UUID are extracted from the JWT automatically.

**Collector token as a secret:** `collectorToken` is a `SecretConfig`: Embedded (the gateway encrypts it with its system key; config.json holds only ciphertext) or Referenced (read from a secret provider). `FactryHistoryProvider.resolveToken` reads it at start and on every settings change. Profiles saved before this field hold the token in plain text under `token`; it is still used when `collectorToken` is not set, with one WARN per start. `FactryHistorianExtensionPoint.encode` leaves `token` out once `collectorToken` is set, so the first save with the secret removes the plain copy. The extension point's `validate` refuses an Embedded value that is not a collector token. It checks nothing else: Ignition runs it on every decode, a refused decode does not load the profile at all, and a Referenced token would need a provider call; so an older profile with a blank or bad plain token still loads and shows Errored, and the form marks the field required. The legacy field is `@NonSecret`, so Ignition does not warn at every start that a field named like a secret is a plain string. Ignition fires no change when a profile is saved unchanged, so a token replaced in a secret provider is read again when Factry refuses the one in use (the scheduled connection check), when the profile restarts, or on a save that changes something. A value that is not a collector token is not taken (one WARN; the status keeps saying Factry refuses the token), and a new token starts from the stored host and port, so one naming another Factry is followed. A Referenced token that cannot be read when the profile starts (a provider of another module, or a remote one whose Gateway Network link is not up yet) does not fail the start: the profile shows Errored "Collector token cannot be read" with the provider and secret name, and every connection check reads it again until it can, then connects and registers the collector with Factry (Active, Collecting). A save whose token cannot be read points the client nowhere (an `.invalid` host), so nothing reads the previous Factry with the previous token. A value that is not a collector token is warned about once, not on every check. Shutdown waits up to 5 s for the scheduler, so no check builds a channel after the client is closed. The REST call for the default database gets the same cleaned token as the gRPC channel (both are rebuilt by `reconfigure`), so it follows a reload too. `handleSettingsChange` and these reconnects are synchronized, so a save and a reload never mix settings and token. Downgrading to a module without this field after a save loses the token.

### `FactryHistorianSettings.java`

**Location:** `gateway/src/main/java/.../gateway/FactryHistorianSettings.java`

Internal settings object implementing `HistorianSettings`. Holds all configuration plus derived fields:

- `applyTokenDefaults(token)`: cleans the token with `JwtTokenParser.normalize()`, parses the JWT for the collector, host, port and certificate claim, and returns the cleaned token, which the provider gives to the gRPC client. The host and port used (`getHost()`, `getPort()`) are the overrides when set, else the token's; they are transient, so a resolved value never turns into a stored override, and a new token moves them. It logs once where it connects, whether each value came from the token or an override, and the certificate check
- Host and port of profiles saved before the overrides (`grpcHost`, `grpcPort`) are moved into `hostOverride` and `portOverride` when the profile is decoded, so they show in the form and the next save stores the new keys. `localhost` and `9876` were the defaults written with every save, so they count as not set. A stored `useTls` ("Use TLS" of profiles from before #42) becomes `plainText`: `false` turns Plain Text on, which still means TLS against a Factry whose token claims its built-in certificate
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

`normalize(token)` removes all whitespace (a pasted token often ends with a newline or space, or is wrapped over two lines, and Factry refuses it then) and checks that what remains is three base64url parts separated by dots. Its error message never contains the token.

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
- a measurement whose `ignitionDataType` setting is `DateTime` → `DataType.DateTime`. Factry keeps the dates as epoch milliseconds; the query engine returns them as `java.util.Date`. Aggregates of such a tag: LastValue, Minimum, Maximum, MinMax, Average and SimpleAverage are Dates, Count is a number, and Sum, Range, Variance and StdDev read null with `Bad_Unsupported` on every row (set in the processing context, like a refused pen's `Bad_Failure`). Measurements created before this setting existed read back as numbers. To have such a measurement read back as Dates and keep its history, add the setting in Factry instead of deleting the measurement: `GET /api/measurements/{uuid}`, then `PUT /api/measurements/{uuid}` with the same `Attributes`, `CollectorUUID`, `DatabaseUUID`, `Datatype`, `Description`, `Metadata`, `Name` and `Status`, and `Settings` plus `"ignitionDataType": "DateTime"`. The module reads Dates from its next cache sync (within 30 s for its own measurements, at most 10 minutes for another collector's). The bulk `PATCH /api/measurements` refuses it ("Setting ignitionDataType is not common to all selected measurements"). Checked on Factry v8.2.3.
- Array tags get their Ignition type in the setting too (e.g. `DateTimeArray`); the elements of such a measurement read back as Dates.
- An array element path `Arr[i]` gets a node of its own: the measurement `Arr` (data type `[]number`, `[]string`, `[]boolean`), element `i`, the element's type, and a node id that includes the index. A measurement whose own name is `Arr[i]` still wins. The base path `Arr` of an array measurement resolves too, but reads as unsupported (see the query engine).

### `ModuleProperties.java`

**Location:** `gateway/src/main/java/.../gateway/ModuleProperties.java`

Loads tuning values from `factry-historian.properties` (bundled in the .modl). These are not exposed in the UI — they're for operational tuning:

| Property | Default | Controls |
|----------|---------|----------|
| `grpc.write.deadline.seconds` | 3 | Timeout on store lookups; base timeout on `createPoints()` and `createMeasurements()` |
| `grpc.write.deadline.ms.per.point` | 1 | Added to the `createPoints()` timeout per point (plus time for its size, see `grpc.write.min.bytes.per.second`), and to the `createMeasurements()` timeout per measurement |
| `grpc.write.deadline.max.seconds` | 30 | Ceiling on the `createPoints()` and `createMeasurements()` timeouts |
| `grpc.write.max.message.bytes` | 4194304 | Largest `createPoints()` message (at least 64 KiB); a bigger flush goes in several calls |
| `grpc.write.min.bytes.per.second` | 262144 | Link speed the `createPoints()` timeout allows for: the message's size divided by this is added to it, within the ceiling |
| `grpc.write.gzip` | true | Send `createPoints()` gzip-compressed. If Factry has no gzip decompressor, the client resends the batch uncompressed and stops using gzip until the historian settings are saved |
| `grpc.read.deadline.seconds` | 30 | Timeout on history queries, on each page of the measurement cache refresh, and on the asset and asset property fetches |
| `grpc.call.deadline.seconds` | 10 | Timeout on the small calls: collectors, health, collector state |
| `grpc.shutdown.grace.seconds` | 3 | How long a settings save or shutdown waits for running calls before it cancels them |
| `status.cache.ms` | 30000 | How often the connection check runs; it pauses S&F while Factry is unreachable and resumes it when it is back |
| `measurement.cache.refresh.seconds` | 30 | How often to ask Factry for this collector's changed measurements (new, Paused, Deleted) |
| `measurement.cache.full.refresh.seconds` | 600 | How often to reload every measurement, collector and asset |
| `measurement.lookup.miss.seconds` | 60 | How long a read remembers that Factry has no measurement with a name, before it looks the name up again |
| `query.raw.max.points` | 500000 | Most raw points one query reads over all its pens; a range that holds more fails the read with an error. An aggregated read fails the same way, before it asks Factry, when pens x intervals (twice for MinMax) is more. About 200 bytes of heap per point while the query runs, so about 100 MB per query |

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
6. Sends `CreatePoints` gRPC call, in parts of at most `grpc.write.max.message.bytes`. Factry refuses a message over 64 MiB (`RESOURCE_EXHAUSTED`, checked after decompression) every time, so a part it still finds too large is sent in halves; so is a part a proxy refuses with HTTP 413 (grpc-java: `UNKNOWN` "HTTP status code 413"). Half the refused size is then the most per call until the settings are saved, so the next flush is not refused again. A single point too large on its own can never be stored; it is sent as a Bad point without a value, with a WARN naming the tag and time. Factry v8.2.3 also answers OK to string values over about 1 MiB but does not store them.
7. On failure: distinguishes connection errors (UNAVAILABLE, DEADLINE_EXCEEDED) from server rejections. On rejection, evicts the measurement UUID and retries once (handles deleted measurements)

Steps 2 to 7 live in `PointWriter`, which the engine calls; it uses no Ignition gateway classes, so unit tests can run it against a fake Factry client.

**`isEngineUnavailable()`** — returns `true` when the gRPC connection is down. This is how S&F knows to buffer instead of attempting writes.

**`doStoreMetadata()`**: caches each tag's metadata in `MeasurementCache`; it sends nothing to Factry by itself. When the module creates a measurement for the tag, its data type, description, engineering units and limits (the `Config` attributes) and the `ignitionDataType` setting come from this cache. The cache keeps the metadata after the create, so a failed create or a measurement deleted in Factry is created again with it: Ignition may not send it again.

**`applySourceChanges()`**: drops the cached metadata of retired and renamed tags. Measurements in Factry are left alone.

### `FactryQueryEngine.java`

**Location:** `gateway/src/main/java/.../gateway/FactryQueryEngine.java`

Extends `AbstractQueryEngine`. Handles all data retrieval from Factry.

**`doQueryRaw(options, processor)`**:
1. Maps each `RawQueryKey` to the measurement UUID of the node `mapKeysToNodes` resolved (the cache; on a miss, one exact-name lookup in Factry, see "Resolving a tag" below)
2. Several keys can read one measurement: the elements `Arr[0]`, `Arr[1]`, ... of an array tag, which Ignition historizes as paths of their own and `PointWriter` stores as one list per row in measurement `Arr`. Each element key gets its element of every stored row; an element a row does not hold is a Bad point without a value. Every element counts as a point for `returnSize` and `query.raw.max.points`, as separate tags would; a pen Factry refuses fails all its element columns. The base path `Arr` reads null with `Bad_Unsupported` on every row: Ignition historizes the elements, not the array, and a whole list has no place in one column (read as a value it was 0.0, Good). Ignition's wide writer starts a row for every point, also for two pens at the same time, so a stored array row gives one row per element read
3. Builds `QueryTimeseriesRequest` with measurement UUIDs and time range, grouped by status
4. Reads in pages of `limit`/`offset` (Factry applies both per series), halving the page when an answer is too large, and keeps only the first `returnSize` points across all pens when Ignition sets one. With `returnSize`, each page drops the points past the first `returnSize` and stops paging pens whose next points come after them, so a read holds about `returnSize` points plus one page per pen, and the cap counts only the points kept. When Factry refuses a pen (see item 7), the read starts again from the first page without that pen: its points may have decided where the first `returnSize` end and which pens stopped paging. Fails the read when the range holds more than `query.raw.max.points` points over all pens, or when a page starts before the previous one ended, as a Factry that ignores `offset` would answer. Part of a range is never returned: Ignition repeats a pen's last value with Good quality into every row after its data stops, so a pen cut short looks like a flat line. Ignition's own `HistoryWriter` fails the same way at its row limit
5. Adds per pen the last sample before the start and the first sample at or after the end, at their own times and qualities, as Ignition's Core Historian adds its seed values. A tag stored on change then has a value at the start of a trend, also when it did not change inside the range. Two requests for all pens (`limit 1` grouped by status, `desc` for the one before; Factry applies the limit per measurement and status), none for pens Factry refused, and none after the end when the range ends after now. Left out when the query options carry `NO_BOUND_VALUES` (`queryTagHistory(..., includeBoundingValues=False)`), or `NO_SEED_VALUES` without `YES_BOUND_VALUES`. Ignition 8.3.6 (historian 1.3.6, `HistorianQueryInterface.createQuery`) sets `NO_SEED_VALUES` on every raw read through its legacy path, which `system.tag.queryTagHistory` uses: there the bounds come only with `includeBoundingValues=True` (`YES_BOUND_VALUES`, which wins, as in Ignition's legacy loaders). Ignition 8.3.0 to 8.3.5 (historian 1.3.3) set it only for raw columns in a read that also has aggregated ones, so there plain raw reads (`queryTagHistory` without `includeBoundingValues`, `queryRawPoints`, raw bindings) get the bounds by default, as from the Core Historian on those versions. The Core Historian ignores `YES_BOUND_VALUES` and returns no bounds on that path at all. `system.historian.queryRawPoints` takes the same path (`HistorianManagerImpl.queryDataPoints` turns its options into legacy query parameters), so in 8.3.6 it gets no bounds either, unless a `RawQueryOptions` passed as its first argument carries `YES_BOUND_VALUES`. Only a caller of the query engine itself that sets no option gets them by default. With `returnSize` the value before the start counts as the first point and the one after the end only fits if the cap is not reached. When Factry refuses these reads, the range is returned without them; when Factry itself cannot be read (unavailable, timed out, token refused) the read fails, as for the range. A sample exactly at the end is the value after the end (Factry's end is exclusive, checked live), and comes back once. The read before the start scans from epoch 0, so on a large database it can be the slowest request of the read (the Average path sends the same one). Ignition's wide writer keeps only the first row past the end
6. Iterates response `Series` → `SeriesPoint`, converts protobuf values to Java, creates `AtomicPoint` objects, and feeds them to the `RawPointProcessor` in one time order across all pens. Ignition builds wide-format rows from that single stream; pen by pen gives rows out of order with values carried between pens
7. A pen Factry refuses on its own (for example a measurement deleted after it was cached) reads null with `Bad_Failure` on every row, and the other pens still show their data. The engine reads Factry before it initializes Ignition's processor, and gives that pen a `Bad_Failure` quality in the `ProcessingContext`, the way `mapKeysToNodes` marks an unknown tag `Bad_NotFound`. Ignition then builds an errored column for it. This is safe because `HistorianPointQueryExecutor.startReading` runs the whole query before it waits for the columns. The other ways fall short: a point with no value reads 0.0 in a number column, and `onKeyFailure` after initialization shows as `Bad_NotFound`. Factry fails a raw request for all pens when one of them fails, so after such a failure each pen is asked again on its own to find the one to blame. When Factry itself cannot be read (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `UNAUTHENTICATED`, `CANCELLED`), or a raw request for all pens fails while each pen alone reads fine, or Factry refuses every pen of a raw read, the read fails as a whole: it calls `processor.onError` and then throws, in `doQueryAggregated` too. Ignition's history reader ignores `onError` and would show an empty, Good result; the exception ends the dataset with an error instead. The Power Chart answers that with an error, and the RPC to Vision and Designer clients carries it. Gateway-scope `system.tag.queryTagHistory` and Perspective history bindings drop that error in Ignition itself (8.3.6) and still return an empty dataset. An aggregated read whose pens all fail on their own (for example `UNKNOWN` on every call while Factry's database is down) still shows `Bad_Failure` columns on every interval row, not an error. Ignition splits one chart into several queries and runs them in one loop (`AggregateHistoryQueryExecutor.startReading`) that the first error ends: one query per historian (`HistorianManagerImpl`), and with `FillMode.DERIVED` one per aggregated pen. An error from one query would take the other queries' pens down with it. A raw read with every pen refused fails instead: raw rows come only from points, so it would otherwise be an empty, Good dataset that looks like no data. The trade-off: when Factry itself is down or refuses every raw pen, the read fails, so a chart that mixes historians loses the other historians' pens too. A tag Factry does not have stays a `Bad_NotFound` column, not an error, also when every tag in the read is unknown; the engine builds those columns itself then, because `mapKeysToNodes` builds none and Ignition would wait 5 s for them

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
| Variance | `stddev` and `count`: s² (n - 1) / n, 0 for one sample (see below) |
| StdDev | `stddev` and `count`: s √((n - 1) / n), 0 for one sample (see below) |
| MinMax | queries `min` and `max` separately, a pair per bucket |

**Other aggregates (`BucketFill`)**: every aggregate except Average and Count gives every bucket a point, like Average does. Ignition fills a bucket without a point with a neighbouring value marked Good, so a Bad stretch, the time before the first sample and the empty buckets of a tag stored on change read a value that was never there (Good 7 at 10:00:05 and 8 at 10:00:45, LastValue in 10 s buckets, read `7, 8, 8, 8, 8, 8` instead of `7, 7, 7, 7, 8, 8`).
- Good samples in the bucket: Factry's aggregate over those samples, grouped by status with only the Good series kept. A bucket with Good and Bad samples gives the value of its Good samples (LastValue: the last Good value). Several Good statuses in one bucket (`Good`, `GoodLocalOverride`; only data from other collectors has them, the module writes plain `Good`) give one point: the smallest minimum, the largest maximum, the summed sum. LastValue reads the last sample of each status before the bucket end to find the latest Good one. SimpleAverage, Range, Variance and StdDev cannot be combined from per-status values, so such a bucket gives the value of one status, marked Uncertain (the Power Chart shows it as a gap).
- No samples, and the status held from before is Good: the value held, as Ignition's own aggregates take the value at the block start. Range is 0 there; Sum, Variance and StdDev have no value (Bad), as in Ignition's own functions.
- Otherwise (only Bad or Uncertain samples, a status held that is not Good, before the first sample, and from now on): a Bad point without a value. MinMax gives each bucket a minimum and a maximum point, Bad ones too.
- Requests per pen: Factry's `last` per status (which is the LastValue itself) says which statuses each bucket holds and the value held after it, plus the aggregate per status (two for MinMax). The last sample before a bucket is read when the range starts with empty buckets (once), after a bucket holding several statuses when an empty bucket follows, and for LastValue at the end of a bucket with several Good statuses: up to 10 such reads per pen. Past that an empty bucket after a bucket holding several statuses is Bad (a gap is safer than a guess), and such a LastValue is Uncertain.
- A String tag has only LastValue and Count: Factry 8.2.3 refuses every other aggregate of a string measurement, so those read null with `Bad_Unsupported` on every row, like Sum of a DateTime tag.
- An array element (`Arr[i]`) has only LastValue and Count too: Factry 8.2.3 aggregates an array measurement only with `first`, `last` and `count` and refuses the rest, `twa` included (checked live). The others read `Bad_Unsupported` without asking Factry. LastValue takes element `i` of the last list; Count counts stored rows, and a row is written whenever any element of the array changes. The base path `Arr` reads `Bad_Unsupported` for every aggregate.
- Count counts the Good samples in each bucket, summed over Good statuses. A bucket without Good samples counts 0 (Good), as Ignition's own Count does; Ignition would otherwise copy a neighbour's count into it. Buckets from now on are Bad. One request per pen.
- Variance and StdDev: Ignition's are the population values (they divide by the number of samples, `VarianceAggregate`); Factry's `stddev` is the sample standard deviation (divides by one less) and has no value for a bucket with one sample, and Factry 8.2.3 has no `variance` at all (it refuses it, checked live). So a `count` of the Good samples per bucket is read too and the `stddev` scaled; one sample gives 0, as in Ignition. Samples 1, 2, 3 give Variance 0.667 and StdDev 0.816 (Factry's `stddev` alone says 1.0).
- Count of a tag that is not numeric (String, DateTime) gets a number column: Ignition types a Count column after the tag, so it would otherwise return the text `"600"` or a Date in 1970
- Time window → Go duration conversion (e.g., `TimeWindow(2h30m)` → `"2h30m"`, 600 ms → `"600ms"`). The bucket length in milliseconds and the period string come from the same number.

**Average (time-weighted, `GoodTimeAverage`)**:
- Factry's `twa` is only right with fill `previous`: it then holds each value until the next sample, also from before the range. Its buckets start at the query start.
- `twa` refuses group by, so Bad and Uncertain time is masked in the module. A `last` aggregate grouped by status and the last sample before the range tell per bucket: Good throughout → Factry's `twa`; not Good throughout → a Bad point without a value (null); status changes inside → the bucket's raw samples are averaged over Good time.
- Raw reads have a budget per pen: 50,000 samples per request, 200,000 and 10 requests per pen, spent from the range end backwards so the most recent buckets are exact. Neighbouring buckets share a request. A `count` aggregate (only when some bucket needs raw samples) sizes the requests. When the status at a bucket start is unknown (the bucket before held several statuses and was not read), the last sample before it is read: always at the start of a raw request, and up to 10 times per pen for buckets outside one.
- A bucket beyond the budget gets `twa` over the samples with status exactly `Good`, returned as Good (the Power Chart drops values that are not Good). It leaves Bad values out but holds the last Good value through Bad time and misses Good sub-statuses.
- Requests per pen: 3 for a healthy tag (`twa`, `last`, sample before); at most 35 (plus `count`, up to 10 raw, 20 one-sample reads, 1 fallback). Raw samples: at most 200,000.
- The range stops at now: the current bucket is weighted up to now, and buckets after now are Bad.
- Every bucket gets a point. Ignition fills a bucket without a point with a neighbouring value marked Good (checked through its own query path, and live on 8.3.6), so an outage, the time before the first sample and the time after now would read as a held Good value. They are Bad points instead, which the Power Chart draws as gaps and `queryTagHistory` returns with Bad quality (a null reads as 0.0 in a Float column, and as 1970-01-01 00:00 in a DateTime column, since Ignition turns it into `Date(0)`). Not NaN: the Power Chart streams history as strict JSON, which fails at a NaN.
- Booleans: Factry's `twa` refuses them, so they are averaged from raw samples as the fraction of Good time true (true = 1). A `mean` per status replaces `twa`: a Good mean of exactly 0 or 1 means the bucket holds one value, so it needs no raw read when that is the value held. Beyond the budget: that `mean`, no extra request, returned as Uncertain because a sample mean can be far off for a Boolean stored on change (0.5 instead of 0.017 for a bit on 1 minute of the hour). The Power Chart draws Uncertain values as gaps; with neighbouring buckets sharing a request and on-change storage this should be rare. At most 34 requests. Factry 8.2.3 accepts `last`, `count`, `mean`, `min`, `max`, `sum`, `spread` and `stddev` on a Boolean (checked live), so every aggregate but Average goes to Factry as for a number. Average of a String tag is `Bad_Unsupported` (see below).
- Difference with Ignition's internal historian: the module leaves all Bad and Uncertain time out (the owner's choice) and holds the previous value until the next sample (steps). Ignition's own time-weighted average uses trapezoids between values and bridges Bad time inside a block, leaving out only Bad time at the block's start and end. So the numbers differ on ramps and across short Bad stretches.

**`doBrowse(browseContext, publisher)`**:
- Root level: publishes two category folders — "Measurements" and "Assets"
- Measurements: groups measurements under their collector's name, splits the name into folders and publishes the leaf with the measurement's read key as its node id (`[database]name` for another Site's). A measurement whose Site the cache does not know is left out
- Assets: splits asset names on "/" to build folder hierarchy
- Uses `BrowsePublisher` to emit folder and tag nodes
- Reads the cache only; it never asks Factry. A measurement made by hand or by another collector shows up from the next full cache reload, at most 10 minutes later

**`doQueryMetadata(options, processor)`**:
- Returns measurement metadata: data type, status, name, engineering specs (UOM, hi/lo limits)

**Resolving a tag** (`findNode` and `doQueryMetadata`, through `MeasurementCache.findForRead`):
- Cache hit, or an asset with that name → return. An array element `Arr[i]` first checks the cache for `Arr[i]` and for its array `Arr`, and only then asks Factry for either
- Cache miss → one exact-name lookup in Factry (`GetMeasurementsByFilter` with the name as keyword, within the own database, or the database a `[database]name` key names; such a key is first looked up as a literal name), so a measurement made since the last refresh reads at once. A name Factry does not have is remembered for `measurement.lookup.miss.seconds` (60 s), so a trend of a misspelt or deleted tag does not ask on every poll. Before, every miss downloaded every measurement of the organisation, twice per read

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
3. Start scheduled executor with recurring tasks:
   - Health update to Factry (every 30s): `Collecting`, or `Unstable` while the status says Factry does not store writes
   - Measurement cache sync (every 30s: this collector's changes; every 10 minutes: everything)
   - Quarantine retry (every 30s, S&F only)
   - Connection check (every 30s, `GetCollector`): pauses the S&F sink while Factry is unreachable, resumes it when it is back

**`getStorageEngine()`** — returns S&F bridge if configured, otherwise the direct storage engine

**`getStatus()`** — from `WriteHealth`, fed by the scheduled connection check and by what becomes of every write: Errored when the last check failed, with the reason (`UNAVAILABLE`/`DEADLINE_EXCEEDED`: "Factry Historian unreachable"; `UNAUTHENTICATED`/`PERMISSION_DENIED`: "Factry refused the collector token"; `NOT_FOUND` "collector is deleted": "Collector deleted in Factry") and Factry's own text in the details; one WARN per reason and one INFO when the check passes again, not repeated by a check that comes and goes (a reason is news again after 5 minutes of passing checks). Also Errored ("Factry does not store writes") when every write failed over at least 30 s while the check passes, refused (for example `INTERNAL` while Factry's InfluxDB is down) or timed out (InfluxDB hanging), until a write is stored again. A failed check restarts that count, so writes are judged afresh after an outage. Data handling does not change: store and forward keeps the data and the quarantine retry sends it again. The gateway's Historians page shows an Errored profile as a red "Error" badge with the status details as its tooltip. Logging, each line naming the profile (also after a rename): one WARN per answer from Factry until a write is stored (repeats at DEBUG), one WARN when the status turns Errored, a summary of failed flushes and points about every 5 minutes while it lasts, and one INFO with the totals when a write is stored again. A connection check that fails in between does not end this story, so a flapping check does not repeat it. While writes fail, the 30 s quarantine retry logs at DEBUG instead of INFO.

With Factry as a History Splitter's primary, an Errored status also sends the splitter's queries to the secondary: Ignition's splitter uses a historian only while its status is Good.

**`handleSettingsChange()`** — reconfigures gRPC client, refreshes cache, updates engines. No restart needed. Ignition 8.3.3 to 8.3.6 call it on every save, also a rename-only save (before `handleNameChange()`), so a save that changes nothing returns `true` and changes nothing: the form (compared as `FactryHistorianConfig`, so new form fields count) and the values taken from the token are the same, and so is what the gRPC client was set up with (host, port, collector, the token as resolved, TLS settings; a token secret whose value changed behind the same reference is a change). The check and the apply happen in `connect()`, which is synchronized with the token reload. Ignition 8.3.8 and newer (historian 1.3.8) call only `handleNameChange()` on a save that renames; it reads the profile's saved settings and applies them through `handleSettingsChange()` when they differ. A cache refresh that fails (Factry down) is only a WARN when the Factry host, port and collector stay the same: the scheduled sync retries it, and a restart would leave a just-renamed profile's buffered data behind. For another Factry or collector it returns `false`, so Ignition restarts the profile with an empty cache instead of keeping the old Factry's measurements.

**`handleNameChange()`** — applies settings changed in the same save (see `handleSettingsChange()`), and otherwise returns `true`: the running instance keeps its name inside the module and keeps forwarding from the store and forward engine named after the old name. Measurement names in Factry don't include the profile name, so Factry needs nothing. From the profile's next start (gateway restart, module update, a settings save that restarts it) it buffers in a new engine with the new name, and data still in the old engine is not forwarded. So a rename logs one WARN with the old engine's pending and quarantined counts; an empty engine gets an INFO, and a rename in letter case only keeps the engine (engine names ignore case). Before restarting after a rename, let the old engine reach 0 on the Store and Forward page. Data already left behind is forwarded again by renaming the profile back to the old name (it then uses the old engine again; the new engine keeps its own data for when the profile gets that name again). An empty old engine can be deleted on the Store and Forward page. Version limit: in historian 1.3.3 to 1.3.6 (Ignition 8.3.0 to 8.3.6) Ignition's historian collection updates its name map only when a profile is added or removed (1.3.8 added `updateMapsForNameChange`), so after a rename that keeps the profile running, `histprov:<new name>` may not resolve until the profile restarts, and a later save may start a second instance.

Ignition 8.3.6's own historians move the data along on a rename (`SinkLifecycleEvent.sinkRenamed`), but only for a third-party engine, and a third-party engine is not started at gateway boot and is unloaded when its profile stops; store and forward then drops tag data sent to it (`StoreAndForwardManagerImpl.storeData`, a WARN once a minute). That would add data loss to common operations (gateway boot, module update, profile disabled or failing) to cover a rare one, and Ignition's rename of a third-party engine also deletes the buffered data on a rename in letter case only (engine folders are lowercased). So the module keeps the ordinary engine.

**`retryQuarantinedData()`** — moves quarantined S&F records back to pending. Ignition never auto-retries quarantined data, but our failures are almost always transient connection issues.

---

## 7. Gateway — Observability

### `HistorianMetrics.java`

**Location:** `gateway/src/main/java/.../gateway/HistorianMetrics.java`

Thread-safe counters using `AtomicLong`. Tracks:

| Metric | Description |
|--------|-------------|
| Store ops / points / errors / elapsed ms | Write performance |
| Discarded / paused-dropped points | Points not written because their measurement refuses them |
| Raw query ops / rows / elapsed ms | Raw read performance |
| Agg query ops / rows / elapsed ms | Aggregated read performance |

`recordStore()`, `recordRawQuery()`, `recordAggregatedQuery()` and the others are called by the engines after each operation. No code reads the counters outside the tests: nothing logs them and they are not in the gateway's metric registry. Ignition already shows per profile what most of them count (storage and query throughput, success and failure, on the Historians page; the backlog and quarantine on the Store and Forward page).

---

## 8. Tests

### Unit Tests

| File | Tests |
|------|-------|
| `TagPathUtilTest.java` | Path conversion: stored ↔ display, QualifiedPath parsing, folder handling, edge cases |
| `FactryHistorianSettingsTest.java` | JWT parsing, token defaults, validation |
| `HistorianMetricsTest.java` | Counter increments |

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
- **History renaming** — not supported by the Factry platform. Renaming or moving a tag starts a new measurement: Ignition tells the module only that the old path was retired (store and forward drops the new path, `TagHistoryStorageEngineBridge.processSourceChanges`), and Factry v8.2.3 cannot rename a collector's measurement (no gRPC call; the REST `PUT` answers 200 but does not store the new name). The old history stays in the old measurement, which stays Active and reads and browses under the old path, so it can still be charted next to the new one. Ignition's SQL Historian splits history the same way. A rename in letter case only does not split (see `MeasurementCache`)
- **Extended scripting** (e.g. `system.factry.queryEvents`) — deferred until customer demand exists

**Metadata only seeds new measurements:** `FactryStorageEngine.doStoreMetadata()` caches the tag metadata, and it is applied only when the module creates a measurement (data type, description, engineering units and limits, `ignitionDataType` setting). Later changes to a tag's metadata in Ignition are not pushed to an existing measurement; edit those on the Factry platform.

**Pagination:** Raw reads page through `QueryTimeseries` with `limit`/`offset`. One answer for a day of 1-second data on a few pens is over grpc-java's default 4 MiB message limit, so the client also accepts answers up to 64 MiB, which is Factry's own send limit (`GRPC_MAX_MESSAGE_SIZE`). Charts and scripts still receive the full dataset, up to `query.raw.max.points` points per query; a query over it fails with an error that names the setting.

**Browse filters and optional query parameters:** The design document's appendix lists optional parameters for `browse`, `queryRawPoints`, and `queryAggregatedPoints` (e.g. `returnSize`, `includeBounds`, `fillModes`, `nameFilters`). The framework parses them and passes them to the query engine in the query options; the engine has to apply them itself. It applies the time range, aggregation type, time window and `returnSize`, and the bounding-value options of a raw read (`NO_SEED_VALUES`, `NO_BOUND_VALUES`, see `doQueryRaw`). `queryRawPoints(includeBounds=True)` does not ask for bounding values: it makes the time range inclusive (`TimeRange.includeBounds`), and Ignition 8.3.6 drops that before the engine sees it. `doBrowse` does not apply browse filters such as `nameFilters`.

### Known Limitation: String Data in Raw Queries

The query engine correctly returns string data points to the Ignition framework (confirmed via `HistorianMetrics` counters), but Ignition 8.3's `system.historian.queryRawPoints` produces a `DataSet` with 0 rows for string-typed measurements. This appears to be a framework-level limitation in how `DataSet` handles non-numeric values. Numeric and boolean data types work correctly.
