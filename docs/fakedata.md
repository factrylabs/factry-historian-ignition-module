# Generating Test Data with realfakedata.com

This guide sets up an MQTT Sparkplug B collector that streams simulated industrial data from [realfakedata.com](https://realfakedata.com) into Factry Historian. Useful for testing that the Ignition module can browse and query measurements from other collectors.

Reference: [Factry docs — Installing for testing purposes](https://docs.factry.io/installing-factry-historian-using-docker-for-testing-purposes)

## Prerequisites

- Factry Historian running (see `docs/setup_environment.md`)
- Setup wizard completed with a time series database created

## 1. Create a Collector in Factry

1. Go to **Configuration > Collectors**
2. Click **Create Collector**

| Field | Value |
|-------|-------|
| Name | `realfakedata` |
| Description | Collects data from realfakedata.com |
| Default database | Select your data TSDB (e.g. `Influx`) |
| Status | Active |

3. After creation, click on the collector and **Generate token**
4. Copy the token

## 2. Run the Collector Container

```bash
docker run -d --restart unless-stopped --name factry-collector-realfakedata \
  --network=factry-historian-module \
  -e PRODUCT=mqtt-sparkplugb \
  -e API_TOKEN=<PASTE_YOUR_TOKEN_HERE> \
  ghcr.io/factrylabs/collector:latest
```

> **Note:** The `--network` must match the Docker network used by the historian. Our docker-compose uses `factry-historian-module` (see `docker-compose.yml`).

## 3. Configure MQTT Settings

Once the collector appears in Factry with status "Initializing":

1. Click on the collector, then **Edit**
2. Configure the Sparkplug B settings:

| Setting | Value |
|---------|-------|
| MqttURL | `tcp://mqtt.realfakedata.com:1883` |
| Topic | `spBv1.0/simulator/#` |
| QOS | `1` |
| PersistentSession | on |
| DiscoverMeasurements | true |
| AutoOnboard | true |
| Failover | off |
| ClientCertificate | false |
| Username | *(leave empty)* |
| Password | *(leave empty)* |
| TimestampLayout | *(leave empty)* |

3. Click **Save**
4. Select the collector and click **Start**

## 4. Verify

After a few seconds, the collector should show:
- **Health**: Collecting (green)
- **Heartbeat**: Last seen just now

New measurements will appear automatically under **Measurements** in Factry. These should also be visible in the Ignition power chart tag browser under the historian profile.

## Cleanup

To stop and remove the collector container:

```bash
docker stop factry-collector-realfakedata
docker rm factry-collector-realfakedata
```
