# Factry Historian Ignition Module

An Ignition 8.3+ module that connects [Inductive Automation Ignition](https://inductiveautomation.com/) to [Factry Historian](https://factry.io) for structured, contextualised industrial data integration.

The module implements a custom historian that writes tag data to Factry via gRPC and reads it back for Power Chart, tag history bindings, and scripting.

## Prerequisites

1. **Ignition 8.3+** — Download and install from https://inductiveautomation.com/downloads/
2. **Factry Historian** — A running Factry Historian instance with a time series database configured. See [factry.io](https://factry.io) for setup instructions.

## Installing the Module

1. Download the latest `Factry-Historian.modl` from the [GitHub releases page](https://github.com/factrylabs/factry-historian-ignition-module/releases)
2. In the Ignition Gateway, go to **Config > System > Modules**
3. Click **Install or Upgrade a Module**
4. Upload the downloaded `.modl` file
5. Accept the **Factry** certificate when prompted
6. The module status should show **Running**

> The module is open source and free to use.

## Quick Start

### 1. Create a Collector in Factry

1. Open the Factry Historian web UI
2. Go to **Collectors** in the sidebar
3. Click **Create Collector**
4. Select your time series database as the collector's default database. Without a default database, Factry keeps new measurements in Discovered and stores nothing
5. Give it a name (e.g., `Ignition`)
6. Click **Generate Token** and **copy the token**

For a dev setup, `script/setup-factry.sh` does the same through Factry's REST API (checked against Factry v8.2.3). Set `FACTRY_URL` if Factry is not on `http://localhost:8000`.

### 2. Create a Historian in Ignition

1. Go to **Config > Tags > History > Historians**
2. Click **Create New Historian Profile**
3. Select **Factry Historian**, click **Next**
4. Paste the **Token** from step 1 — the connection settings (host, port, collector ID) are extracted automatically
5. Click **Create Historian**
6. Verify the status shows **Running**

### 3. Create a Test Tag

1. Open the Ignition Designer (download the launcher from your gateway's home page)
2. In the **Tag Browser**, create a new **Memory Tag** (e.g., `TestTag`, type: Float8)
3. Edit the tag and go to the **History** section:
   - **History Enabled** = true
   - **History Provider** = the historian you created in step 2
   - **Sample Mode** = On Change
4. Save the tag

### 4. Write Values and Verify

1. In the Designer's Tag Browser, right-click the tag and select **Write Value**
2. Change the value a few times (e.g., 10.0, 20.0, 30.0)
3. Open the Factry Historian web UI and go to **Measurements**
4. You should see a measurement matching your tag (e.g., `Ignition/default/TestTag`)
5. Click on it to view the stored data points

## Documentation

See [docs/content.md](docs/content.md) for the full documentation index.

## License

Open source — see [LICENSE](LICENSE) for details.
