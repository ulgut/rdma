#!/bin/bash
set -euo pipefail

IS_CLIENT=${IS_CLIENT:-0}
MACHINE_ID=${MACHINE_ID:-0}
MPMC_IS_PRODUCER=${MPMC_IS_PRODUCER:-1}

RAW_ID=$(ifconfig enp8s0d1 | grep 'inet ' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$RAW_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

NODE_ID=$((RAW_ID - 1))

echo "NODE_ID=$NODE_ID | IS_CLIENT=$IS_CLIENT | MACHINE_ID=$MACHINE_ID | MPMC_IS_PRODUCER=$MPMC_IS_PRODUCER"

cd /local/rdma/build || exit
sudo NODE_ID=$NODE_ID IS_CLIENT=$IS_CLIENT MACHINE_ID=$MACHINE_ID MPMC_IS_PRODUCER=$MPMC_IS_PRODUCER ./rdma
