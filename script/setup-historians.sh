#!/bin/bash

# Creates two Factry Historian profiles in Ignition:
#   1. "Factry Historian NoSF" — without Store & Forward
#   2. "Factry Historian SF"   — with Store & Forward
#
# Also creates the S&F database connection and engine if needed.
#
# Usage: setup-historians.sh <collector-token>

set -e
cd "$(dirname "$0")/.."

TOKEN="$1"
if [ -z "$TOKEN" ]; then
    echo "Usage: setup-historians.sh <collector-token>"
    exit 1
fi

IGNITION_DATA_DIR="./ignition/data"
HISTORIAN_BASE="$IGNITION_DATA_DIR/config/resources/core/com.inductiveautomation.historian/historian-provider"
DB_CONN_BASE="$IGNITION_DATA_DIR/config/resources/core/ignition/database-connection"
SF_ENGINE_BASE="$IGNITION_DATA_DIR/config/resources/core/ignition/store-and-forward-engine"

SF_DB_NAME="FactryS&F"
SF_ENGINE_NAME="FactryS&F"

# ── Helper: create a historian profile ────────────────────────────
create_historian() {
    local name="$1"
    local sf_engine="$2"  # empty for no S&F
    local dir="$HISTORIAN_BASE/$name"

    mkdir -p "$dir"

    # config.json — historian settings
    cat > "$dir/config.json" << EOF
{
  "profile": {
    "type": "factry-historian"
  },
  "settings": {
    "batchIntervalMs": 5000,
    "batchSize": 10,
    "collectorName": "",
    "collectorUUID": "",
    "debugLogging": false,
    "grpcHost": "historian",
    "grpcPort": 8001,
    "skipTlsVerification": true,
    "storeAndForwardEngine": "${sf_engine}",
    "token": "${TOKEN}",
    "useTls": true
  }
}
EOF

    # resource.json — Ignition resource metadata
    local uuid
    uuid=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "00000000-0000-0000-0000-000000000001")
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    cat > "$dir/resource.json" << EOF
{
  "scope": "A",
  "description": "",
  "version": 1,
  "restricted": false,
  "overridable": true,
  "files": [
    "config.json"
  ],
  "attributes": {
    "lastModification": {
      "actor": "setup-script",
      "timestamp": "${timestamp}"
    },
    "uuid": "${uuid}",
    "enabled": true
  }
}
EOF

    echo "  Created historian: $name"
}

# ── Helper: create S&F database connection ────────────────────────
create_sf_database() {
    local dir="$DB_CONN_BASE/$SF_DB_NAME"
    mkdir -p "$dir"

    cat > "$dir/config.json" << 'EOF'
{
  "connectURL": "jdbc:sqlite:storeforward.db",
  "connectionProps": "",
  "connectionResetParams": "",
  "defaultTransactionLevel": "DEFAULT",
  "driver": "SQLite",
  "evictionRate": -1,
  "evictionTests": 3,
  "evictionTime": 1800000,
  "failoverMode": "STANDARD",
  "failoverProfile": "",
  "includeSchemaInTableName": false,
  "poolInitSize": 0,
  "poolMaxActive": 8,
  "poolMaxIdle": 8,
  "poolMaxWait": 5000,
  "poolMinIdle": 0,
  "slowQueryLogThreshold": 60000,
  "testOnBorrow": true,
  "testOnReturn": false,
  "testWhileIdle": false,
  "translator": "SQLITE",
  "username": "",
  "validationQuery": "SELECT 1",
  "validationSleepTime": 10000
}
EOF

    local uuid
    uuid=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || echo "00000000-0000-0000-0000-000000000002")
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    cat > "$dir/resource.json" << EOF
{
  "scope": "A",
  "description": "SQLite database for Factry Historian Store & Forward",
  "version": 1,
  "restricted": false,
  "overridable": true,
  "files": [
    "config.json"
  ],
  "attributes": {
    "lastModification": {
      "actor": "setup-script",
      "timestamp": "${timestamp}"
    },
    "uuid": "${uuid}",
    "enabled": true
  }
}
EOF

    echo "  Created database connection: $SF_DB_NAME"
}

# ── Helper: create S&F engine ─────────────────────────────────────
create_sf_engine() {
    local dir="$SF_ENGINE_BASE/$SF_ENGINE_NAME"
    mkdir -p "$dir"

    cat > "$dir/config.json" << 'EOF'
{
  "batchSize": 10000,
  "dataThreshold": 10000,
  "forwardRateMs": 1000,
  "forwardingPolicy": "ALL",
  "isThirdParty": false,
  "primaryMaintenancePolicy": {
    "action": "PREVENT_NEW_DATA",
    "limit": {
      "limitType": "COUNT",
      "value": 0
    }
  },
  "scanRateMs": 100,
  "secondaryMaintenancePolicy": {
    "action": "PREVENT_NEW_DATA",
    "limit": {
      "limitType": "COUNT",
      "value": 0
    }
  },
  "timeThresholdMs": 30000
}
EOF

    local uuid
    uuid=$(python3 -c "import uuid; print(uuid.uuid4())" 2>/dev/null || echo "00000000-0000-0000-0000-000000000003")
    local timestamp
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    cat > "$dir/resource.json" << EOF
{
  "scope": "A",
  "version": 1,
  "restricted": false,
  "overridable": true,
  "files": [
    "config.json"
  ],
  "attributes": {
    "lastModification": {
      "actor": "setup-script",
      "timestamp": "${timestamp}"
    },
    "uuid": "${uuid}"
  }
}
EOF

    echo "  Created S&F engine: $SF_ENGINE_NAME"
}

# ── Main ──────────────────────────────────────────────────────────

echo "Creating historian profiles..."

# Historian without Store & Forward
create_historian "Factry Historian NoSF" ""

# S&F infrastructure (database connection + engine)
create_sf_database
create_sf_engine

# Historian with Store & Forward
create_historian "Factry Historian SF" "$SF_ENGINE_NAME"

echo ""
echo "Historian profiles created. Restart Ignition to apply."
