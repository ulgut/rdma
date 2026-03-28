#!/bin/bash
set -euo pipefail

IS_CLIENT=${IS_CLIENT:-0}
MACHINE_ID=${MACHINE_ID:-0}
MPMC_IS_PRODUCER=${MPMC_IS_PRODUCER:-1}
STRATEGY=${STRATEGY:-}
CLIENTS_PER_MACHINE=${CLIENTS_PER_MACHINE:-}
NUM_OPS=${NUM_OPS:-}
MPMC_ACTIVE_WINDOW=${MPMC_ACTIVE_WINDOW:-}
NODE_ID=${NODE_ID:-}

# If NODE_ID not explicitly set, derive from IB IP
if [ -z "$NODE_ID" ]; then
    RAW_IP=$(ifconfig enp8s0d1 2>/dev/null | grep 'inet ' | awk '{print $2}')
    if [ -z "$RAW_IP" ]; then
        # Try ibp8s0 as fallback
        RAW_IP=$(ifconfig ibp8s0 2>/dev/null | grep 'inet ' | awk '{print $2}')
    fi

    if [ -z "$RAW_IP" ]; then
        echo "Error: Could not find IB IP address"
        exit 1
    fi

    # Map IP to node ID based on position in cluster
    # Servers: .16=0, .17=1, .18=2  Clients: .19=3, .20=4
    LAST_OCTET=$(echo "$RAW_IP" | cut -d'.' -f4)
    NODE_ID=$((LAST_OCTET - 16))
fi

echo "NODE_ID=$NODE_ID | IS_CLIENT=$IS_CLIENT | MACHINE_ID=$MACHINE_ID | STRATEGY=${STRATEGY:-default} | MPMC_IS_PRODUCER=$MPMC_IS_PRODUCER | CLIENTS_PER_MACHINE=${CLIENTS_PER_MACHINE:-default} | NUM_OPS=${NUM_OPS:-default} | MPMC_ACTIVE_WINDOW=${MPMC_ACTIVE_WINDOW:-default}"

# Build env string, only pass vars that are set
ENV="NODE_ID=$NODE_ID IS_CLIENT=$IS_CLIENT MACHINE_ID=$MACHINE_ID MPMC_IS_PRODUCER=$MPMC_IS_PRODUCER"
[ -n "$STRATEGY" ] && ENV="$ENV STRATEGY=$STRATEGY"
[ -n "$CLIENTS_PER_MACHINE" ] && ENV="$ENV CLIENTS_PER_MACHINE=$CLIENTS_PER_MACHINE"
[ -n "$NUM_OPS" ] && ENV="$ENV NUM_OPS=$NUM_OPS"
[ -n "$MPMC_ACTIVE_WINDOW" ] && ENV="$ENV MPMC_ACTIVE_WINDOW=$MPMC_ACTIVE_WINDOW"

cd /local/rdma/build || exit
sudo env $ENV ./rdma
