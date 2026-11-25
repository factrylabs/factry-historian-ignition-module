# Integration Tests

Integration tests verify the Factry Historian module end-to-end against a running Ignition gateway and Factry Historian instance. Tests exercise two paths:

1. **WebDev endpoints** (HTTP) -> Ignition -> Factry module -> gRPC -> Factry (end-to-end)
2. **Direct gRPC** -> Factry (bypass Ignition, for data seeding and verification)

## Prerequisites

### 1. Docker environment running

```bash
docker-compose up -d
```

Verify all containers are healthy:

```bash
docker ps
```

You should see `factry-historian-ignition`, `factry-historian`, `factry-postgres`, `factry-influx` all running.

### 2. WebDev module installed in Ignition

The WebDev module must be installed in the Ignition gateway. Check at:

**Config > System > Modules**

Look for `Web Developer Module` in the list. If it's not installed, download it from [Inductive Automation](https://inductiveautomation.com/downloads/modules) and install it via the Modules page.

### 3. WebDev test scripts deployed

The integration test scripts live in:

```
ignition/data/projects/TestFactry/com.inductiveautomation.webdev/resources/test/
    store/doPost.py    -- calls system.tag.storeTagHistory()
    query/doPost.py    -- calls system.tag.queryTagHistory()
```

These files are mounted into Ignition via the Docker volume (`./ignition/data`). After any changes to these scripts, trigger a project scan so Ignition picks them up:

```bash
./script/scan.sh
```

### 4. Gateway API Key for scan.sh

The `scan.sh` script uses Ignition's REST API to trigger project scans. This requires an API key with proper permissions.

#### Create security levels

Go to **Config > Security > Levels** and create the following hierarchy under `Authenticated`:

```
Public
  Authenticated
    Roles
      Administrator
    ApiToken          <-- create this
      Access          <-- create this (child of ApiToken)
      Read            <-- create this (child of ApiToken)
      Write           <-- create this (child of ApiToken)
    SecurityZones
```

To create child levels: select the parent level, then use the add/create button to add a child underneath it.

#### Create API key

Go to **Config > Security > API Keys** and click **Create API Key +**:

- **Name**: `cicd` (no spaces)
- **Require secure connections**: **uncheck** this (we use HTTP for local dev)
- **Security Level**: expand the tree and check:
  - `Authenticated > ApiToken > Access`
  - `Authenticated > ApiToken > Read`
  - `Authenticated > ApiToken > Write`

Save the key and copy the full token (format: `name:secret`).

#### Configure Roles & Permissions

Go to **Config > Security > General Settings** and scroll to the **ROLES & PERMISSIONS** section. Make sure the `ApiToken` levels (Access, Read, Write) are allowed. The exact configuration should match the security levels you just created.

#### Update scan.sh

Update the `API_KEY` default in `script/scan.sh`:

```bash
API_KEY="${API_KEY:-cicd:YOUR_TOKEN_HERE}"
```

Or pass it as an environment variable:

```bash
API_KEY="cicd:your-secret" ./script/scan.sh
```

#### Verify scan works

```bash
./script/scan.sh
```

Expected output:

```
Triggering Ignition resource scans...

Scanning factry-historian (http://localhost:8089)...
  Config scan triggered
  Projects scan triggered

Done!
```

### 5. Factry Historian collector token

The integration tests need a Factry collector token to authenticate gRPC calls. The token is configured as a default in `gateway/build.gradle.kts`. If your environment uses a different token, override via environment variable:

```bash
COLLECTOR_TOKEN="your-jwt-token" ./gradlew integrationTest
```

The token is a JWT containing the collector UUID, gRPC host/port. You can decode it to inspect:

```bash
echo "TOKEN_PAYLOAD_PART" | base64 -d | python3 -m json.tool
```

### 6. Gateway system name

The test needs to know the Ignition gateway's system name (visible in **Config > System > Gateway Settings**). Default is `Ignition-296a8ca4b6cd`. Override if different:

```bash
GATEWAY_SYSTEM_NAME="your-system-name" ./gradlew integrationTest
```

## Running the tests

```bash
# Run integration tests
./gradlew integrationTest

# Run unit tests (does not require running infrastructure)
# (remark: cleanTest needed for redo it, otherwise it is cashed)
./gradlew cleanTest test
```

## Environment variables

All configuration has defaults for the local Docker setup. Override as needed:

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_URL` | `http://localhost:8089` | Ignition gateway URL |
| `WEBDEV_PROJECT` | `TestFactry` | Ignition project with WebDev endpoints |
| `HISTORIAN_NAME` | `Factry Historian 0.8` | Historian provider name in Ignition |
| `GRPC_HOST` | `localhost` | Factry Historian gRPC host |
| `GRPC_PORT` | `8001` | Factry Historian gRPC port |
| `COLLECTOR_UUID` | *(from JWT)* | Factry collector UUID |
| `COLLECTOR_TOKEN` | *(dev token)* | Factry collector JWT token |
| `GATEWAY_SYSTEM_NAME` | `Ignition-296a8ca4b6cd` | Ignition system name |

## Test cases

| Test | Description |
|------|-------------|
| Raw Query | Insert data via direct gRPC, query via Ignition WebDev |
| Store via Ignition | Store data via WebDev, verify in Factry via direct gRPC |
| Aggregation | Average, Min, Max aggregation queries via WebDev |
| Multi-Tag Query | Query 3 tags simultaneously via WebDev |
| Empty Query | Query a time range with no data |
| String Values | String-type measurement round-trip via gRPC |
| Round Trip | Store + query both via Ignition (full end-to-end) |

## Architecture

```
                         +------------------+
                         |  Integration     |
                         |  Test (JUnit 5)  |
                         +--------+---------+
                                  |
                    +-------------+-------------+
                    |                           |
              HTTP POST                    gRPC (TLS)
              (WebDev)                     (direct)
                    |                           |
            +-------v--------+          +-------v--------+
            |   Ignition     |  gRPC    |    Factry      |
            |   Gateway      +--------->+    Historian    |
            |  (port 8089)   |  (TLS)   |   (port 8001)  |
            +----------------+          +-------+--------+
                                                |
                                        +-------v--------+
                                        |   InfluxDB     |
                                        |  (time-series) |
                                        +----------------+
```

## Troubleshooting

### scan.sh returns HTTP 401
- API key format is wrong. Must be `name:secret` (e.g., `cicd:ABC123...`).
- "Require secure connections" is checked on the API key -- uncheck it for HTTP.

### scan.sh returns HTTP 403
- API key lacks permissions. Edit the key in **Config > Security > API Keys** and ensure `Authenticated > ApiToken > Access/Read/Write` are checked.
- Check **Config > Security > General Settings > ROLES & PERMISSIONS** allows ApiToken levels.

### WebDev returns HTTP 405 "Method POST is not supported"
- Ignition hasn't loaded the WebDev scripts. Run `./script/scan.sh` to trigger a project scan.
- Verify the WebDev module is installed.
- Check that files exist at `ignition/data/projects/TestFactry/com.inductiveautomation.webdev/resources/test/`.

### gRPC "UNAVAILABLE: Network closed for unknown reason"
- Factry serves TLS unless its gRPC TLS is turned off (`USE_SELF_SIGNED_CERTIFICATE=false`). The test uses TLS with insecure trust (matching module config). Against a Historian without TLS, this fails; a plain text client against a TLS port fails the same way.
- Verify Factry is running: `docker ps | grep factry-historian`.
- Check port is reachable: `nc -z localhost 8001`.

### gRPC authentication errors
- `COLLECTOR_TOKEN` or `COLLECTOR_UUID` is wrong or empty.
- Token may have expired. Check the `exp` claim in the JWT payload.

### Tests pass but "0 rows returned"
- The module batches writes with a 5-second interval. Tests wait 8 seconds by default. If the batch interval is configured higher, increase `BATCH_FLUSH_WAIT_MS` in the test.
- Check Ignition logs for storage errors: `docker-compose logs ignition | grep -i error`.
