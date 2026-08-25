# Store & Forward (S&F) in the Factry Historian Module

## Overview

Store & Forward buffers tag history data locally when the Factry server is unreachable,
then forwards it automatically once the connection is restored. This prevents data loss
during network outages or server maintenance.

## How It Works

### Data Flow

```
Tag value changes
    │
    ▼
StorageBridge (routes into S&F)
    │
    ▼
S&F Engine (pending queue)
    │
    ▼
DataSinkBridge → StorageEngine.doStoreAtomic() → gRPC createPoints()
```

### S&F States

Data in the S&F engine can be in one of three states:

| State          | Description |
|----------------|-------------|
| **Pending**    | Buffered and waiting to be forwarded. S&F retries these automatically at the configured forward rate. |
| **Quarantined**| Failed too many times and set aside. **Never retried automatically by Ignition.** Requires manual intervention or programmatic retry. |
| **Forwarded**  | Successfully delivered to the storage engine and sent to Factry. |

### Why Quarantine Exists

Quarantine prevents one bad record from blocking the entire pipeline. Ignition's S&F
cannot distinguish between "server is temporarily down" and "this specific record is
invalid". After repeated failures, it moves the record to quarantine so the rest of the
queue can flow.

The quarantine trigger is **not configurable** — there is no "max retry count" setting
in the Ignition gateway UI. Ignition internally decides based on failure patterns:
- Bulk insert fails → retries records individually → individual failures → quarantine
- Connection unavailable → data stays in pending (not quarantined)

**Important:** Ignition never automatically retries quarantined data. The Factry Historian
module includes a scheduled task that moves quarantined records back to pending every
30 seconds (see `FactryHistoryProvider.retryQuarantinedData()`), since in our case
failures are almost always transient connection issues, not bad data.

## Connection Tracking

The module tracks the gRPC connection state to minimize data loss during outages:

1. `FactryGrpcClient` maintains a `connected` flag
2. When `createPoints()` fails, `connected` is set to `false`
3. `FactryStorageEngine.isEngineUnavailable()` returns `true` when disconnected
4. S&F sees the engine is unavailable and **buffers points in pending without attempting the call**
5. A periodic check (every 30s) tests connectivity and sets `connected = true` when the server is back
6. S&F resumes forwarding from the pending queue

This means only the **first** failed call experiences the gRPC timeout. All subsequent
points go straight to the pending buffer with zero delay.

### gRPC Deadline

The `createPoints()` call has a 3-second deadline. This ensures that when the server goes
down, the failure is detected quickly rather than hanging indefinitely on a TCP timeout.
The deadline can be adjusted via the `WRITE_DEADLINE_SECONDS` constant in `FactryGrpcClient`.

## Ignition S&F Engine Configuration

The module automatically creates a Store & Forward engine when the historian profile starts. The engine name matches the historian profile name. You can view and tune its settings in the Ignition gateway at **Config → Store & Forward → Engines**.

### Engine Settings

| Parameter         | Default | Description |
|-------------------|---------|-------------|
| **Forward Rate**  | 1000 ms | How often S&F attempts to drain the pending queue to the data sink. Lower values mean faster forwarding but more frequent gRPC calls. |
| **Schedule Pattern** | (empty) | Optional time window for forwarding (e.g., `9:00-15:00`). Leave empty for continuous forwarding. |

### Store Settings

| Parameter          | Default | Description |
|--------------------|---------|-------------|
| **Time Threshold** | 30000 ms | Max time data accumulates in memory before flushing to disk cache. Lower values reduce risk of data loss on gateway crash. |
| **Data Threshold** | 10000   | Max number of records in memory before flushing to disk cache. |
| **Batch Size**     | 10000   | Max records per forwarding attempt. |

### Maintenance Settings

| Parameter                          | Default            | Description |
|------------------------------------|--------------------|-------------|
| **Primary Store Maintenance Value**   | 0 (unlimited)   | Max records in the primary store (memory). |
| **Primary Store Maintenance Action**  | PREVENT_NEW_DATA | What happens when the limit is reached. `PREVENT_NEW_DATA` stops accepting new data — **this can cause data loss if the memory buffer fills up.** |
| **Secondary Store Maintenance Value** | 0 (unlimited)   | Max records in the secondary store (disk cache). |
| **Secondary Store Maintenance Action**| PREVENT_NEW_DATA | Same as above but for disk. |

### Advanced Settings

| Parameter            | Default | Description |
|----------------------|---------|-------------|
| **Forwarding Policy** | ALL    | Which data to forward. `ALL` forwards everything. |
| **Engine Scan Rate**  | 100 ms | How often S&F scans data pipelines for forwarding decisions. |

## Recommended Settings for Factry

For typical deployments:

- **Forward Rate**: `1000` (1 second) — good balance between latency and load
- **Time Threshold**: `5000` (5 seconds) — flush to disk quickly to minimize data loss on crash
- **Data Threshold**: `1000` — flush to disk before memory grows too large
- **Maintenance Values**: `0` (unlimited) — avoid `PREVENT_NEW_DATA` dropping points during long outages
- **S&F engine name**: Created automatically by the module (matches the historian profile name)

## Monitoring

Check S&F status in the Ignition gateway at **Status → Store & Forward**:

- **Pending count**: Records waiting to be forwarded. During an outage, this should grow steadily.
- **Quarantined count**: Records that failed forwarding. Should stay at 0 during normal outages thanks to the module's automatic quarantine retry.

If the pending count stays at 0 during an outage while data is being written, points are
being lost before reaching S&F — check the gateway logs for errors in the storage bridge.

## Troubleshooting

### Data lost during server outage (2-10 second gap)
The gRPC `createPoints()` call has a 3-second deadline. The first call after the server
goes down will block for up to 3 seconds before failing and marking the connection as down.
Points arriving during this initial window may be delayed but should not be lost if S&F is
properly configured.

### Quarantined count keeps growing
Check the Ignition gateway logs for the quarantine reason. Common causes:
- Data type mismatch between Ignition tag and Factry measurement
- Measurement UUID no longer valid (deleted in Factry)
- Authentication token expired

### Pending count never drains after server recovery
The module checks connectivity every 30 seconds. After the server comes back, it may take
up to 30 seconds for the module to detect this and re-enable forwarding. Check logs for
"Factry server is reachable again" message.

## References

- [Ignition S&F Documentation](https://www.docs.inductiveautomation.com/docs/8.1/platform/database-connections/store-and-forward)
- [Controlling Quarantine Data](https://www.docs.inductiveautomation.com/docs/8.1/platform/database-connections/store-and-forward/controlling-quarantine-data)
- [S&F Status Page](https://www.docs.inductiveautomation.com/docs/8.1/platform/gateway/status/connections/connections-store-and-forward)
- [Forum: Data Quarantining (Carl Gould explanation)](https://forum.inductiveautomation.com/t/data-quarantining/1942)
