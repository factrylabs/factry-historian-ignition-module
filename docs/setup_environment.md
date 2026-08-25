# Development Environment Setup

Guide for setting up the local development environment, building the module from source, and running tests.

## Prerequisites

- **Java 17** — Required for building and running the module
- **Docker** and **Docker Compose** — For running Ignition, Factry Historian, and supporting services

### Verify Java Version

```bash
java -version  # Should show Java 17
```

If you have multiple Java versions installed, ensure Java 17 is active before building.

## 1. Start the Docker Services

First, create the required files in the ignition folder. We mount the ignition folder to Docker, and it needs many files there. Committing all of them to git would be overwhelming, so we use a script:

```bash
script/setup-ignition.sh
```

Then start the services:

```bash
docker compose up -d
```

This starts:

| Service | URL | Credentials |
|---------|-----|-------------|
| Ignition Gateway | http://localhost:8089 | admin / password |
| Factry Historian | http://localhost:8000 | (see setup wizard) |
| Grafana | http://localhost:3050 | admin / admin |
| PostgreSQL | localhost:5432 | factry / password |
| InfluxDB | localhost:8086 | factry / password |

## 2. Set Up Factry Historian

### First-time login

After a fresh start with `AUTO_MIGRATE: "true"`, Factry creates a default `factry` user with password `password`. Log in at http://localhost:8000 and complete the setup wizard.

### Setup Wizard

> **IMPORTANT:** Pay close attention to the values below. Incorrect settings (especially the URL and InfluxDB password) are the most common source of problems.

**Step 1 — Welcome**: Create your admin user (e.g. `factry` / `stereo`)

**Step 2 — License**: Skip (trial mode)

**Step 3 — Organization**: Enter an organization name (e.g. `Test`)

**Step 4 — Internal Time Series Database (InfluxDB)**:

| Field | Value | Notes |
|-------|-------|-------|
| Database type | Influx | |
| Admin user | `factry` | |
| Admin password | `password` | **Must match docker-compose, NOT your Factry login password** |
| Host | `http://influx:8086` | Docker service name, NOT `localhost` or `127.0.0.1` |
| Database | `_internal_factry` | Pre-filled, leave as-is |
| Read only user | `grafana` | |
| Read only password | `password` | |
| Create database | checked | |

> **Common mistake:** Using your Factry login password (e.g. `stereo`) instead of the InfluxDB password (`password` from docker-compose). These are different credentials.

**Step 5 — Historian Configuration**:

| Field | Value | Notes |
|-------|-------|-------|
| GRPC port | `8001` | |
| REST port | `8000` | |
| URL | `http://historian` | **CRITICAL: Must be the Docker service name, not `http://localhost`** |
| Session inactive duration | `7d` | Default is fine |

> **Why `http://historian`?** This URL is embedded in collector tokens (as the `aud` JWT claim). The Ignition module extracts the hostname from the token to connect via gRPC. Since Ignition and Factry run in separate Docker containers, `localhost` inside the Ignition container does NOT reach Factry. Using the Docker service name `historian` ensures containers can communicate.

**Step 6 — Finished**: Click finish

### Create a Time Series Database for data

The setup wizard only creates `_internal_factry`. You need a second database for actual historian data:

1. Go to **Configuration > Time Series Databases**
2. Click **Create Database**

| Field | Value |
|-------|-------|
| Database type | Influx |
| Name | `Influx` |
| Admin user | `factry` |
| Admin password | `password` |
| Host | `http://influx:8086` |
| Database | `historian` |
| Read only user | `grafana` |
| Read only password | `password` |
| Create database | checked |
| Status | Active |

### Create a Collector

1. Go to **Configuration > Collectors**
2. Click **Create Collector**
3. Name: `IgnitionCollector`, Database: `Influx`, Status: Active
4. After creation, click on the collector and **generate a token**
5. Copy the token — you'll need it for the Ignition historian profiles

> **Note:** Without a Factry license, the setup wizard must be completed again after every container restart. The token also becomes invalid after a restart without a license.

## 3. Build the Module

```bash
./gradlew clean build
```

Output:
- `build/Factry-Historian.modl` (signed)
- `build/Factry-Historian.unsigned.modl` (unsigned)

### Module Signing (Production)

Place certificates in a `certificates/` directory (git-ignored):

```
certificates/
  keystore.jks
  cert.p7b
```

Configure in `gradle.properties` (also git-ignored):

```properties
ignition.signing.keystoreFile=certificates/keystore.jks
ignition.signing.keystorePassword=<password>
ignition.signing.certAlias=factry-modules
ignition.signing.certFile=certificates/cert.p7b
ignition.signing.certPassword=<password>
```

## 4. Install the Module

```bash
cp build/Factry-Historian.unsigned.modl ignition/data/modules/Factry-Historian.modl
docker compose restart ignition
```

On first install, go to **Config > System > Modules** and accept the **Factry** certificate.

## 5. Configure Historian Profiles in Ignition

The historian profiles are configured via JSON files in:
```
ignition/data/config/resources/core/com.inductiveautomation.historian/historian-provider/
```

A profile is created by `setup-historians.sh`:
- **Factry Historian** — Writes to Factry with automatic Store & Forward buffering

The module automatically creates an S&F engine matching the historian profile name on startup.

Key settings in `config.json`:

| Field | Value | Notes |
|-------|-------|-------|
| `grpcHost` | `historian` | Docker service name |
| `grpcPort` | `8001` | gRPC port |
| `token` | (from Factry) | The collector token |
| `useTls` | `true` | |
| `skipTlsVerification` | `true` | For dev environment |

> **Note:** The module reads `grpcHost` and `grpcPort` from the JWT token's `aud` claim by default. If the token contains `http://historian`, it will use `historian:8001`. The config file values serve as fallback.

After updating the config, restart Ignition:
```bash
docker compose restart ignition
```

## 6. Run Tests

### Unit Tests

```bash
./gradlew test
```

### Integration Tests

Integration tests require the full Docker environment running with configured historians.

```bash
./gradlew integrationTest
```

The token is automatically read from the historian config files in `ignition/data/`. No need to configure it separately.

Key system properties (configurable via env vars):

| Property | Env Variable | Default |
|----------|-------------|---------|
| `gateway.url` | `GATEWAY_URL` | `http://localhost:8089` |
| `grpc.host` | `GRPC_HOST` | `localhost` |
| `grpc.port` | `GRPC_PORT` | `8001` |
| `collector.name` | `COLLECTOR_NAME` | `Ignition` |
| `historian.name` | `HISTORIAN_NAME` | `Factry Historian` |

The integration tests use WebDev endpoints deployed in a `TestFactry` project on the Ignition gateway.

## Troubleshooting

### "Connection refused: localhost:8001" in Ignition logs
- The token's `aud` claim contains `http://localhost` instead of `http://historian`
- **Fix:** In Factry, go to **Configuration > Settings > API**, change URL to `http://historian`, then regenerate the collector token and update the historian configs
- This happens when the setup wizard URL field was left as `http://localhost`

### "could not use historian database" errors
- The InfluxDB credentials in the Factry TSDB config don't match docker-compose
- **Fix:** The InfluxDB admin password must be `password` (from docker-compose), not your Factry login password

### Factry login — "incorrect user or password" after fresh start
- With `AUTO_MIGRATE: "true"`, the default credentials are `factry` / `password`
- After completing the setup wizard, use the password you set during setup

### Historian status shows ERROR
- Check the token is correct and not expired
- Check gRPC connectivity: `docker compose logs ignition | grep -i grpc`
- Check Factry is running: `docker compose logs historian`
- Verify the collector exists in Factry and is Active

### Tag history not appearing in Factry
- Check that the historian status shows **Running** in the Historians list
- Check that the tag's History Provider matches the historian name
- Check Ignition gateway logs: `docker compose logs ignition | grep -i factry`

### Build fails with Java errors
- Ensure Java 17 is active: `java -version`
- The build will fail with other Java versions

### Factry setup wizard doesn't appear
- `AUTO_MIGRATE: "true"` in docker-compose auto-creates the schema and default user
- The wizard appears on first login after migration
- If you need a completely fresh start: `docker compose down`, remove volumes (`docker volume rm factry-historian-module_postgres_data factry-historian-module_historian_data factry-historian-module_influx_data`), then `docker compose up -d`
