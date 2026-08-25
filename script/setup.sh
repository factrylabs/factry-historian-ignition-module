#!/bin/bash

# Main setup wizard for Factry Historian Module development environment.
# Orchestrates all sub-scripts with manual pauses where needed.

set -e
cd "$(dirname "$0")/.."

SCRIPT_DIR="./script"

echo ""
echo "=========================================="
echo "  Factry Historian Module - Setup Wizard"
echo "=========================================="
echo ""

# ──────────────────────────────────────────────
# Step 1: Initialize Ignition data
# ──────────────────────────────────────────────
echo "──────────────────────────────────────────"
echo "  Step 1: Initialize Ignition data"
echo "──────────────────────────────────────────"
echo ""
bash "$SCRIPT_DIR/setup-ignition.sh"

# ──────────────────────────────────────────────
# Step 2: Start Docker services
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 2: Starting Docker services"
echo "──────────────────────────────────────────"
echo ""
docker compose up -d
echo ""
echo "Waiting for services to be ready..."

# Wait for Ignition
echo -n "  Ignition: "
for i in $(seq 1 90); do
    if curl -sf http://localhost:8089/StatusPing > /dev/null 2>&1; then
        echo "ready"
        break
    fi
    if [ "$i" -eq 90 ]; then
        echo "FAILED (timeout)"
        echo "Check logs: docker compose logs ignition"
        exit 1
    fi
    sleep 2
done

# Wait for Factry
echo -n "  Factry:   "
for i in $(seq 1 90); do
    if curl -sf http://localhost:8000/ > /dev/null 2>&1; then
        echo "ready"
        break
    fi
    if [ "$i" -eq 90 ]; then
        echo "FAILED (timeout)"
        echo "Check logs: docker compose logs historian"
        exit 1
    fi
    sleep 2
done

# ──────────────────────────────────────────────
# Step 3: Manual — Activate trials
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 3: Manual setup required"
echo "──────────────────────────────────────────"
echo ""
echo "  A) Ignition Gateway: http://localhost:8089"
echo "     - Start the trial (or enter license)"
echo "     - Credentials: admin / password"
echo ""
echo "  B) Factry Historian: http://localhost:8000"
echo "     - Start the trial (or enter license)"
echo "     - Complete the Setup Wizard:"
echo "       1. Organization: any name"
echo "       2. Internal TSDB: Influx, admin=factry, password=password,"
echo "          host=http://influx:8086, database=_internal_factry"
echo "       3. Settings: gRPC=8001, REST=8000,"
echo "          URL=http://historian, Base URL=http://localhost:8000"
echo "     - Create a Time Series Database:"
echo "       Configuration > Time Series Databases > Create Database"
echo "       Type=Influx, admin=factry, password=password,"
echo "       host=http://influx:8086, database=historian, create=enabled"
echo ""
read -p "  Press ENTER when both are set up... "

# Verify Ignition is still reachable after trial setup
if ! curl -sf http://localhost:8089/StatusPing > /dev/null 2>&1; then
    echo "  Ignition is not reachable. Please check and try again."
    exit 1
fi
echo "  Ignition: OK"

# Verify Factry is responding (authenticated endpoints should return 401, not connection error)
if ! curl -sf http://localhost:8000/ > /dev/null 2>&1; then
    echo "  Factry is not reachable. Please check and try again."
    exit 1
fi
echo "  Factry:   OK"

# ──────────────────────────────────────────────
# Step 4: Build and install the module
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 4: Build and install the module"
echo "──────────────────────────────────────────"
echo ""
bash "$SCRIPT_DIR/setup-module.sh"

# ──────────────────────────────────────────────
# Step 5: Create Factry collector and get token
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 5: Create Factry collector"
echo "──────────────────────────────────────────"
echo ""
bash "$SCRIPT_DIR/setup-factry.sh"

# Read the token saved by setup-factry.sh
TOKEN_FILE="./script/.collector-token"
if [ ! -f "$TOKEN_FILE" ]; then
    echo "ERROR: Collector token not found at $TOKEN_FILE"
    exit 1
fi
COLLECTOR_TOKEN=$(cat "$TOKEN_FILE")

# ──────────────────────────────────────────────
# Step 6: Create historian profiles + S&F database
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 6: Create historian profiles"
echo "──────────────────────────────────────────"
echo ""
bash "$SCRIPT_DIR/setup-historians.sh" "$COLLECTOR_TOKEN"

# ──────────────────────────────────────────────
# Step 7: Restart Ignition to pick up changes
# ──────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────────"
echo "  Step 7: Restart Ignition"
echo "──────────────────────────────────────────"
echo ""
docker compose restart ignition
echo "Waiting for Ignition to be ready..."
for i in $(seq 1 90); do
    if curl -sf http://localhost:8089/StatusPing > /dev/null 2>&1; then
        echo "Ignition is ready."
        break
    fi
    if [ "$i" -eq 90 ]; then
        echo "FAILED (timeout)"
        exit 1
    fi
    sleep 2
done

# ──────────────────────────────────────────────
# Done
# ──────────────────────────────────────────────
echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "  Ignition Gateway:  http://localhost:8089"
echo "  Factry Historian:  http://localhost:8000"
echo "  Grafana:           http://localhost:3050"
echo ""
echo "  Run integration tests:"
echo "    ./gradlew integrationTest"
echo ""
echo "  Run unit tests:"
echo "    ./gradlew test"
echo ""
