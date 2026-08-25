#!/bin/bash

# Creates the ignition/data directory from a fresh Ignition container.
# This gives us the base database and config files that Ignition needs.

set -e
cd "$(dirname "$0")/.."

IGNITION_DATA_DIR="${1:-./ignition/data}"
SYSTEM_NAME="${2:-Ignition-FactryTest}"
TEMP_PORT="${3:-8088}"
IGNITION_IMAGE="inductiveautomation/ignition:8.3.3"

# Check if data directory already exists
if [ -d "$IGNITION_DATA_DIR" ] && [ "$(ls -A $IGNITION_DATA_DIR 2>/dev/null)" ]; then
    echo "Ignition data directory already exists: $IGNITION_DATA_DIR"
    read -p "Do you want to DELETE it and start fresh? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing existing data directory..."
        rm -rf "$IGNITION_DATA_DIR"
    else
        echo "Keeping existing data. Skipping initialization."
        exit 0
    fi
fi

echo ""
echo "Starting temporary Ignition container to initialize data..."
echo ""

# Create temporary docker-compose file without volumes
cat > docker-compose.temp.yml << EOF
services:
  ignition:
    image: ${IGNITION_IMAGE}
    container_name: ignition-init-temp
    hostname: FactryTest
    ports:
      - "${TEMP_PORT}:8088"
    environment:
      ACCEPT_IGNITION_EULA: "Y"
      GATEWAY_ADMIN_USERNAME: "admin"
      GATEWAY_ADMIN_PASSWORD: "password"
    restart: "no"
EOF

docker compose -f docker-compose.temp.yml up -d

echo "Waiting for Ignition to initialize (this takes ~30-60 seconds)..."

MAX_WAIT=120
COUNTER=0
while [ $COUNTER -lt $MAX_WAIT ]; do
    if docker exec ignition-init-temp curl -sf http://localhost:8088/StatusPing > /dev/null 2>&1; then
        echo "Ignition is ready."
        break
    fi
    if [ $((COUNTER % 10)) -eq 0 ]; then
        echo "  ... waiting ($COUNTER seconds elapsed)"
    fi
    sleep 1
    COUNTER=$((COUNTER + 1))
done

if [ $COUNTER -eq $MAX_WAIT ]; then
    echo "ERROR: Ignition failed to start within $MAX_WAIT seconds"
    docker compose -f docker-compose.temp.yml down
    rm docker-compose.temp.yml
    exit 1
fi

echo ""
echo "Copying Ignition data from container..."
mkdir -p "$IGNITION_DATA_DIR"
docker cp ignition-init-temp:/usr/local/bin/ignition/data/. "$IGNITION_DATA_DIR/"

if [ ! -d "$IGNITION_DATA_DIR/db" ]; then
    echo "ERROR: Data copy failed"
    docker compose -f docker-compose.temp.yml down
    rm docker-compose.temp.yml
    exit 1
fi

echo "Data copied ($(du -sh $IGNITION_DATA_DIR | cut -f1))"

echo "Stopping temporary container..."
docker compose -f docker-compose.temp.yml down
rm docker-compose.temp.yml

# Set a fixed system name so it's consistent across machines
echo "Setting system name to $SYSTEM_NAME..."
SYSPROPS="$IGNITION_DATA_DIR/config/resources/core/ignition/system-properties/config.json"
if [ -f "$SYSPROPS" ]; then
    # Use python for reliable JSON editing, fallback to sed
    python3 -c "
import json, sys
with open('$SYSPROPS', 'r') as f:
    d = json.load(f)
d['systemName'] = '$SYSTEM_NAME'
with open('$SYSPROPS', 'w') as f:
    json.dump(d, f, indent=2)
" 2>/dev/null || sed -i.bak "s/\"systemName\": \".*\"/\"systemName\": \"$SYSTEM_NAME\"/" "$SYSPROPS"
fi

# Restore git-tracked project files (TestFactry WebDev scripts, project.json)
echo "Restoring git-tracked project files..."
git checkout -- ignition/data/projects/ 2>/dev/null || true

echo ""
echo "Ignition data initialized in $IGNITION_DATA_DIR"
