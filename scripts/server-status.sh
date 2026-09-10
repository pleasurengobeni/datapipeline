#!/bin/bash
# server-status.sh — readable server health overview

RESET="\033[0m"
BOLD="\033[1m"
RED="\033[1;31m"
YELLOW="\033[1;33m"
GREEN="\033[1;32m"
CYAN="\033[1;36m"

bar() {
  local pct=$1 width=30
  local filled=$(( pct * width / 100 ))
  local empty=$(( width - filled ))
  local color=$GREEN
  (( pct >= 80 )) && color=$YELLOW
  (( pct >= 90 )) && color=$RED
  printf "${color}["
  printf '%0.s█' $(seq 1 $filled 2>/dev/null || true)
  printf '%0.s░' $(seq 1 $empty  2>/dev/null || true)
  printf "] %d%%${RESET}" "$pct"
}

echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║           SERVER STATUS OVERVIEW                 ║${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════╝${RESET}"
echo ""

# ── Hostname / uptime ──────────────────────────────────────────────────────────
echo -e "${BOLD}HOST${RESET}"
echo -e "  Hostname : $(hostname)"
echo -e "  Uptime   : $(uptime -p 2>/dev/null || uptime)"
echo -e "  Date     : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""

# ── CPU ───────────────────────────────────────────────────────────────────────
echo -e "${BOLD}CPU${RESET}"
CPUS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo "?")
echo -e "  Cores    : ${CPUS}"

# Load average
LOAD=$(uptime | awk -F'load average[s]?:' '{print $2}' | sed 's/^ *//')
L1=$(echo "$LOAD" | awk -F'[, ]+' '{print $1}')
L5=$(echo "$LOAD" | awk -F'[, ]+' '{print $2}')
L15=$(echo "$LOAD" | awk -F'[, ]+' '{print $3}')
echo -e "  Load avg : 1m=${L1}  5m=${L5}  15m=${L15}"

# CPU usage (cross-platform)
if command -v mpstat &>/dev/null; then
  CPU_IDLE=$(mpstat 1 1 | awk '/Average/ {print $NF}')
  CPU_USED=$(echo "100 - $CPU_IDLE" | bc | awk '{printf "%d", $1}')
  printf "  Usage    : "; bar "$CPU_USED"; echo ""
else
  echo -e "  Usage    : (install sysstat for live CPU %)"
fi
echo ""

# ── Memory ────────────────────────────────────────────────────────────────────
echo -e "${BOLD}MEMORY${RESET}"
if command -v free &>/dev/null; then
  read -r TOTAL USED FREE <<< $(free -m | awk '/^Mem/ {print $2, $3, $4}')
  PCT=$(( USED * 100 / TOTAL ))
  printf "  %-9s: " "Used"
  bar "$PCT"; echo ""
  echo -e "  Used     : ${USED} MB / ${TOTAL} MB  (${FREE} MB free)"
else
  # macOS fallback
  PAGES_FREE=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
  PAGES_TOTAL=$(( $(sysctl -n hw.memsize) / 4096 ))
  MEM_FREE_MB=$(( PAGES_FREE * 4096 / 1024 / 1024 ))
  MEM_TOTAL_MB=$(( $(sysctl -n hw.memsize) / 1024 / 1024 ))
  MEM_USED_MB=$(( MEM_TOTAL_MB - MEM_FREE_MB ))
  PCT=$(( MEM_USED_MB * 100 / MEM_TOTAL_MB ))
  printf "  %-9s: " "Used"
  bar "$PCT"; echo ""
  echo -e "  Used     : ${MEM_USED_MB} MB / ${MEM_TOTAL_MB} MB  (${MEM_FREE_MB} MB free)"
fi
echo ""

# ── Disk ──────────────────────────────────────────────────────────────────────
echo -e "${BOLD}DISK${RESET}"
df -h | awk 'NR==1 || /^\/dev/' | while IFS= read -r line; do
  if [[ "$line" == Filesystem* ]]; then
    printf "  %-30s %-6s %-6s %-6s %s\n" "Filesystem" "Size" "Used" "Avail" "Use%"
  else
    FS=$(echo "$line" | awk '{print $1}')
    SZ=$(echo "$line" | awk '{print $2}')
    USED_D=$(echo "$line" | awk '{print $3}')
    AVAIL=$(echo "$line" | awk '{print $4}')
    PCT_RAW=$(echo "$line" | awk '{print $5}' | tr -d '%')
    COLOR=$GREEN
    (( PCT_RAW >= 80 )) && COLOR=$YELLOW
    (( PCT_RAW >= 90 )) && COLOR=$RED
    printf "  ${COLOR}%-30s %-6s %-6s %-6s %s%%${RESET}\n" "$FS" "$SZ" "$USED_D" "$AVAIL" "$PCT_RAW"
  fi
done
echo ""

# ── Docker containers ─────────────────────────────────────────────────────────
echo -e "${BOLD}DOCKER CONTAINERS${RESET}"
if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
  docker ps -a --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" | \
  awk -F'\t' '{
    name=$1; stat=$2; ports=$3
    color="\033[1;32m"
    if (stat ~ /Exited|Error|unhealthy/) color="\033[1;31m"
    else if (stat ~ /starting|Created/) color="\033[1;33m"
    printf "%s%-40s %-35s %s\033[0m\n", color, name, stat, ports
  }'
  echo ""
  RUNNING=$(docker ps -q | wc -l | xargs)
  TOTAL_C=$(docker ps -aq | wc -l | xargs)
  echo -e "  Running: ${GREEN}${RUNNING}${RESET} / Total: ${TOTAL_C}"
else
  echo -e "  ${YELLOW}Docker not available or not running${RESET}"
fi
echo ""

# ── Network ports ─────────────────────────────────────────────────────────────
echo -e "${BOLD}LISTENING PORTS (relevant services)${RESET}"
PORTS=(5432 55432 6379 8080 8090 5001 8501 8050 9090 5050)
for p in "${PORTS[@]}"; do
  if (echo >/dev/tcp/localhost/$p) 2>/dev/null; then
    echo -e "  ${GREEN}✔${RESET}  :${p}  open"
  else
    echo -e "  ${RED}✘${RESET}  :${p}  closed"
  fi
done
echo ""

echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════${RESET}"
echo ""
