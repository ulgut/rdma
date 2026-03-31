#!/bin/bash
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "Usage: $0 host1 [host2 ...]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

for host in "$@"; do
    echo "Cleaning $host..."
    ssh "$host" "sudo rm -rf /local/rdma /local/logs && mkdir -p /local/rdma /local/logs"
    echo "Syncing to $host..."
    rsync -az \
        --exclude build/ \
        --exclude .git/ \
        --exclude '.idea/' \
        --exclude 'cmake-build-*/' \
        "$PROJECT_DIR/" "$host:/local/rdma/"
    echo "  Done: $host"
done

echo ""
echo "Running setup.sh on all nodes..."
for host in "$@"; do
    echo "--- $host ---"
    ssh "$host" "cd /local/rdma && bash scripts/setup.sh" 2>&1 | tail -5
    echo ""
done

echo "=== IB IP Mapping ==="
for host in "$@"; do
    ib_ip=$(ssh "$host" "ifconfig ibp8s0 2>/dev/null | grep 'inet ' | awk '{print \$2}'" || echo "NOT CONFIGURED")
    printf "  %-20s -> %s\n" "$host" "$ib_ip"
done
