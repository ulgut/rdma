#!/usr/bin/env python3
"""MPMC sweep experiment orchestrator.

Runs from your local machine. Orchestrates server replicas + 2 client machines.
Requires: passwordless SSH + sudo on remote nodes, binary already built.

Usage:
    python3 experiment.py
    python3 experiment.py --strategy mpmc_simple,mpmc_synra
    python3 experiment.py --clients 1,2,4,8,16 --window 1,4 --ops 500000
"""

import argparse
import subprocess
import tempfile
import time
from pathlib import Path

SERVERS = ["apt126.apt.emulab.net", "apt072.apt.emulab.net", "apt136.apt.emulab.net"]
PRODUCER = "apt123.apt.emulab.net"
CONSUMER = "apt081.apt.emulab.net"
SSH_USER = "tuglu"
ALL_HOSTS = SERVERS + [PRODUCER, CONSUMER]

SINGLE_SERVER_STRATEGIES = {"mpmc_simple"}

HDR = "strategy,client_machine_id,clients,locks,active_window,zipf_skew,total_ops,wall_s,goodput,mean_us,p50_us,p90_us,p99_us,p99.9_us,max_us"


def ssh(host, cmd, **kwargs):
    return subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=no", f"{SSH_USER}@{host}", cmd],
        **kwargs,
    )


def kill_rdma():
    procs = [ssh(h, "sudo pkill -9 rdma 2>/dev/null; true") for h in ALL_HOSTS]
    for p in procs:
        p.wait()
    time.sleep(1)


def run_trial(strategy, clients, window, ops):
    total = clients * 2
    ops_aligned = (ops // total) * total
    env = f"STRATEGY={strategy} CLIENTS_PER_MACHINE={clients} NUM_OPS={ops_aligned} MPMC_ACTIVE_WINDOW={window}"
    num_servers = 1 if strategy in SINGLE_SERVER_STRATEGIES else len(SERVERS)

    kill_rdma()

    # Start servers
    server_procs = []
    for i in range(num_servers):
        p = ssh(SERVERS[i], f"cd /local/rdma && sudo {env} bash scripts/run.sh")
        server_procs.append(p)
    time.sleep(3)

    # Start clients, capture output
    prod_out = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".txt")
    cons_out = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".txt")

    prod = ssh(PRODUCER, f"cd /local/rdma && sudo IS_CLIENT=1 MACHINE_ID=0 MPMC_IS_PRODUCER=1 {env} bash scripts/run.sh",
               stdout=prod_out, stderr=subprocess.STDOUT)
    cons = ssh(CONSUMER, f"cd /local/rdma && sudo IS_CLIENT=1 MACHINE_ID=1 MPMC_IS_PRODUCER=0 {env} bash scripts/run.sh",
               stdout=cons_out, stderr=subprocess.STDOUT)

    prod_rc = prod.wait()
    cons_rc = cons.wait()
    kill_rdma()

    prod_out.seek(0)
    cons_out.seek(0)
    prod_text = prod_out.read()
    cons_text = cons_out.read()
    Path(prod_out.name).unlink()
    Path(cons_out.name).unlink()

    if prod_rc != 0 or cons_rc != 0:
        return None, prod_text, cons_text

    csv_lines = []
    for text in (prod_text, cons_text):
        for line in text.splitlines():
            if line.startswith("CSV: "):
                csv_lines.append(line[5:])
    return csv_lines, prod_text, cons_text


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="mpmc_simple")
    p.add_argument("--clients", default="1,2,4,8,16")
    p.add_argument("--window", default="1")
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument("-o", "--output", default="results.csv")
    args = p.parse_args()

    strategies = args.strategy.split(",")
    client_counts = [int(c) for c in args.clients.split(",")]
    windows = [int(w) for w in args.window.split(",")]

    results_file = Path(args.output)
    results_file.write_text(f"{HDR}\n")

    print(HDR)
    print(f"# strategy=[{args.strategy}] clients=[{args.clients}] window=[{args.window}] ops={args.ops}")
    print()

    for strat in strategies:
        for win in windows:
            for cli in client_counts:
                num_srv = 1 if strat in SINGLE_SERVER_STRATEGIES else len(SERVERS)
                print(f">>> {strat}  clients={cli*2}  window={win}  servers={num_srv}")

                csv_lines, prod_text, cons_text = run_trial(strat, cli, win, args.ops)

                if csv_lines is None:
                    print(f"!!! FAILED")
                    print(prod_text)
                    print(cons_text)
                elif csv_lines:
                    for line in csv_lines:
                        print(line)
                    with open(results_file, "a") as f:
                        f.write("\n".join(csv_lines) + "\n")
                    goodputs = [line.split(",")[8] for line in csv_lines]
                    labels = ["Producer", "Consumer"]
                    summary = "  |  ".join(f"{l}: {g} ops/s" for l, g in zip(labels, goodputs))
                    print(f"    {summary}")
                print()

    print(f"Results saved to {results_file}")


if __name__ == "__main__":
    main()
