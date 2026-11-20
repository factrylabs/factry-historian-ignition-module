#!/bin/bash

# Creates a collector in Factry Historian via REST API and saves the token.
#
# Requires: Factry setup wizard completed, valid user credentials.
# The token is saved to script/.collector-token for use by setup-historians.sh.
# Checked against Factry Historian v8.2.3.
#
# Environment overrides: FACTRY_URL, COLLECTOR_NAME, FACTRY_ORG (organization name or UUID;
# default: the first one that is not the root organization).

set -e
# The cookie jar holds a session token and the saved collector token is a credential.
umask 077
cd "$(dirname "$0")/.."

TOKEN_FILE="./script/.collector-token"
COLLECTOR_NAME="${COLLECTOR_NAME:-Ignition}"
FACTRY_URL="${FACTRY_URL:-http://localhost:8000}"

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
COOKIE_FILE="$WORK_DIR/cookies.txt"
RESP_FILE="$WORK_DIR/response"

# ── Helpers ───────────────────────────────────────────────────────
# Prints an error with the HTTP status and Factry's error text. Never the raw body: responses
# can hold session or collector tokens.
fail() {
    echo "ERROR: $1 (HTTP $HTTP_CODE)"
    python3 -c "
import json, re, sys
raw = open(sys.argv[1], errors='replace').read()
try:
    d = json.loads(raw)
except ValueError:
    d = raw
if isinstance(d, dict):
    msg = ' '.join(str(d[k]) for k in ('title', 'error', 'detail', 'info') if d.get(k))
elif isinstance(d, str):
    msg = d
else:
    msg = ''
msg = re.sub(r'eyJ[\w-]*\.[\w-]*\.[\w-]*', '<token removed>', msg).strip()
if msg:
    print('  Factry says: ' + msg[:300])
" "$RESP_FILE" 2>/dev/null || true
    exit 1
}

# Authenticated API call. Sets HTTP_CODE and writes the body to RESP_FILE. Never fails itself:
# under set -e that would end the script without a message.
factry_api() {
    local method="$1"
    local path="$2"
    local data="$3"

    local args=(-s -o "$RESP_FILE" -w "%{http_code}" -b "$COOKIE_FILE" -X "$method"
        -H "x-xsrf-token: $XSRF")
    if [ -n "$ORG_UUID" ]; then
        args+=(-H "x-organization-uuid: $ORG_UUID")
    fi
    if [ -n "$data" ]; then
        args+=(-H "Content-Type: application/json" -d "$data")
    fi

    : > "$RESP_FILE"
    HTTP_CODE=$(curl "${args[@]}" "${FACTRY_URL}/api${path}") || HTTP_CODE="000"
}

# ── Factry credentials ────────────────────────────────────────────
echo "Factry Historian login required."
read -r -p "  Username: " FACTRY_USER
read -r -s -p "  Password: " FACTRY_PASS
echo ""

# ── Login ─────────────────────────────────────────────────────────
# The body goes through stdin so the password never shows in the process list.
: > "$RESP_FILE"
HTTP_CODE=$(printf '%s\n%s' "$FACTRY_USER" "$FACTRY_PASS" | python3 -c "
import json, sys
user, password = sys.stdin.read().split('\n', 1)
print(json.dumps({'Name': user, 'Password': password}))
" | curl -s -o "$RESP_FILE" -w "%{http_code}" \
    -c "$COOKIE_FILE" \
    -H "Content-Type: application/json" \
    -d @- \
    "${FACTRY_URL}/api/auth/local/login") || HTTP_CODE="000"
unset FACTRY_PASS

if [ "$HTTP_CODE" != "200" ]; then
    fail "Login to $FACTRY_URL failed"
fi

XSRF=$(grep XSRF "$COOKIE_FILE" | awk '{print $NF}') || true
echo "Logged in to Factry."

# ── Get organization UUID ─────────────────────────────────────────
# Factry 8.2 refuses most API calls without an x-organization-uuid header.
echo "Looking up organization..."
factry_api GET "/current-user/organizations"
[ "$HTTP_CODE" = "200" ] || fail "Could not list organizations"
ORG_UUID=$(FACTRY_ORG="${FACTRY_ORG:-}" python3 -c "
import json, os, sys
try:
    orgs = json.load(open(sys.argv[1]))
except ValueError:
    sys.exit('ERROR: Unexpected answer to the organization lookup.')
wanted = os.environ['FACTRY_ORG']
if wanted:
    match = [o for o in orgs if wanted in (o.get('UUID'), o.get('Name'))]
else:
    match = sorted(orgs, key=lambda o: bool(o.get('IsRoot')))
if match:
    print(match[0]['UUID'])
else:
    sys.exit('ERROR: No organization matches ' + repr(wanted) + '. Available: '
             + ', '.join(o.get('Name', '?') for o in orgs))
" "$RESP_FILE") || true

if [ -z "$ORG_UUID" ]; then
    exit 1
fi
echo "  Using organization: $ORG_UUID"

# ── Get time series database UUID ─────────────────────────────────
echo "Looking up time series database..."
factry_api GET "/timeseries-databases"
[ "$HTTP_CODE" = "200" ] || fail "Could not list time series databases"
TSDB_UUID=$(python3 -c "
import json, sys
dbs = json.load(open(sys.argv[1]))
# Pick the first non-internal database, or the first one
for db in dbs:
    if not db.get('Name', '').startswith('_internal'):
        print(db['UUID'])
        sys.exit(0)
if dbs:
    print(dbs[0]['UUID'])
" "$RESP_FILE" 2>/dev/null) || true

if [ -z "$TSDB_UUID" ]; then
    echo "ERROR: No time series database found. Create one in Factry first."
    exit 1
fi
echo "  Using TSDB: $TSDB_UUID"

# ── Create collector ──────────────────────────────────────────────
# Without a default database Factry leaves new measurements Discovered and stores nothing.
echo "Creating collector '$COLLECTOR_NAME'..."
BODY=$(COLLECTOR_NAME="$COLLECTOR_NAME" TSDB_UUID="$TSDB_UUID" python3 -c "
import json, os
print(json.dumps({
    'Name': os.environ['COLLECTOR_NAME'],
    'Description': 'Ignition historian module',
    'DefaultDatabaseUUID': os.environ['TSDB_UUID'],
    'Status': 'Active',
}))
")
factry_api POST "/collectors" "$BODY"
COLLECTOR_UUID=$(python3 -c "
import json, sys
print(json.load(open(sys.argv[1])).get('UUID', ''))
" "$RESP_FILE" 2>/dev/null) || true

if [ "$HTTP_CODE" != "200" ] || [ -z "$COLLECTOR_UUID" ]; then
    fail "Failed to create collector '$COLLECTOR_NAME'"
fi
echo "  Collector UUID: $COLLECTOR_UUID"

# ── Get token ─────────────────────────────────────────────────────
echo "Getting collector token..."
factry_api GET "/collectors/${COLLECTOR_UUID}/token"
# The answer is {"token": "..."}; a bare JSON string or plain text is accepted too.
# Written next to the other temp files first, so a failure never leaves half a token behind.
if [ "$HTTP_CODE" = "200" ] && python3 -c "
import json, re, sys
raw = open(sys.argv[1]).read()
try:
    d = json.loads(raw)
except ValueError:
    d = raw.strip()
if isinstance(d, dict):
    d = d.get('token') or d.get('Token') or ''
if not isinstance(d, str) or not re.fullmatch(r'eyJ[\w-]*\.[\w-]*\.[\w-]*', d):
    sys.exit(1)
open(sys.argv[2], 'w').write(d)
" "$RESP_FILE" "$WORK_DIR/token" 2>/dev/null; then
    mv "$WORK_DIR/token" "$TOKEN_FILE"
    echo "  Token saved to $TOKEN_FILE"
else
    echo "ERROR: Collector '$COLLECTOR_NAME' ($COLLECTOR_UUID) was created, but its token could not be read (HTTP $HTTP_CODE)."
    echo "  Copy the token from the collector in the Factry UI, or delete the collector there"
    echo "  before running this script again (the name is taken)."
    exit 1
fi
