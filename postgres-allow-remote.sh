#!/bin/bash
# Hardened postgres init: restrict remote access to Docker bridge subnet only
# and enable connection audit logging.
set -e

PG_CONF="/var/lib/postgresql/data/postgresql.conf"
HBA_CONF="/var/lib/postgresql/data/pg_hba.conf"

# ── Listen on all interfaces inside Docker (network-level access is controlled
#    by Docker networking — backend-internal network has no external routing)
grep -q "^listen_addresses = '\*'" "$PG_CONF" \
  || echo "listen_addresses = '*'" >> "$PG_CONF"

# ── pg_stat_statements
if grep -q "^shared_preload_libraries" "$PG_CONF"; then
    sed -i "s|^shared_preload_libraries.*|shared_preload_libraries = 'pg_stat_statements'|" "$PG_CONF"
else
    echo "shared_preload_libraries = 'pg_stat_statements'" >> "$PG_CONF"
fi

# ── Audit logging: log all connections and DDL changes
cat >> "$PG_CONF" << 'EOF'
log_connections = on
log_disconnections = on
log_duration = off
log_statement = 'ddl'
log_hostname = off
log_line_prefix = '%m [%p] %u@%d '
EOF

# ── HBA: Docker bridge subnets ONLY — no 0.0.0.0/0
#    172.0.0.0/8 covers Docker's default + custom bridge networks.
#    10.0.0.0/8  covers Docker overlay networks.
#    Public internet access is blocked at the Docker network level (internal: true)
#    AND here as defense-in-depth.
cat > "$HBA_CONF" << 'EOF'
# Managed by postgres-allow-remote.sh — do not edit manually
# TYPE  DATABASE  USER  ADDRESS            METHOD
local   all       all                      trust
host    all       all   127.0.0.1/32       scram-sha-256
host    all       all   ::1/128            scram-sha-256
host    all       all   172.0.0.0/8        scram-sha-256
host    all       all   10.0.0.0/8         scram-sha-256
EOF
