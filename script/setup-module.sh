#!/bin/bash

# Builds the Factry Historian module and installs it into the Ignition data folder.
# Also registers it in modules.json so Ignition loads it on startup without
# requiring manual certificate acceptance.

set -e
cd "$(dirname "$0")/.."

IGNITION_DATA_DIR="./ignition/data"
MODULES_JSON="$IGNITION_DATA_DIR/modules.json"
MODULE_ID="io.factry.historian.FactryHistorian"
MODULE_DIR="$IGNITION_DATA_DIR/var/ignition/modl"
CERT_FINGERPRINT="df049c75927bae8b1000cc3c9894ab4ecf48244f"

echo "Building module..."
./gradlew clean build -q

# Copy the unsigned module (signed requires certificates)
MODL_FILE="build/Factry-Historian.unsigned.modl"
if [ ! -f "$MODL_FILE" ]; then
    # Try the signed version
    MODL_FILE="build/Factry-Historian.modl"
fi

if [ ! -f "$MODL_FILE" ]; then
    echo "ERROR: Module file not found. Build may have failed."
    exit 1
fi

mkdir -p "$MODULE_DIR"
cp "$MODL_FILE" "$MODULE_DIR/Factry-Historian.modl"
echo "Module copied to $MODULE_DIR/"

# Register module in modules.json (skip if already present)
if [ -f "$MODULES_JSON" ]; then
    if python3 -c "
import json, sys
with open('$MODULES_JSON', 'r') as f:
    d = json.load(f)
if '$MODULE_ID' in d:
    print('already registered')
    sys.exit(0)
d['$MODULE_ID'] = {
    'filename': '/usr/local/bin/ignition/data/var/ignition/modl/Factry-Historian.modl',
    'onStartup': 'enabled',
    'certFingerprint': '$CERT_FINGERPRINT'
}
with open('$MODULES_JSON', 'w') as f:
    json.dump(d, f, indent=2)
print('registered')
" 2>/dev/null; then
        echo "Module registered in modules.json"
    else
        echo "WARNING: Could not update modules.json automatically."
        echo "You may need to accept the certificate manually in Ignition."
    fi
else
    echo "WARNING: modules.json not found. Module may require manual certificate acceptance."
fi
