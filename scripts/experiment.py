#!/usr/bin/env python3
"""MPMC sweep experiment orchestrator.

Runs from your local machine. Orchestrates server replicas + producer/consumer machines.
Requires: passwordless SSH + sudo on remote nodes, binary already built.

Sweep: for each strategy, doubles client count (4, 8, 16, ..., 256 per side),
and for each client count tests fixed ops-per-client values (100K, 1M, 10M, 50M, 100M).

Usage:
    python3 experiment.py
    python3 experiment.py --clients 4,8,16,32
    python3 experiment.py --strategy mpmc_mu --ops-per-client 1000000,10000000
"""

import argparse
import subprocess
import tempfile
import time
from pathlib import Path

# "tuglu@apt083.apt.emulab.net", "tuglu@apt081.apt.emulab.net", "tuglu@apt138.apt.emulab.net", "tuglu@apt176.apt.emulab.net", "tuglu@apt072.apt.emulab.net", "tuglu@apt161.apt.emulab.net", "tuglu@apt150.apt.emulab.net", "tuglu@apt139.apt.emulab.net", "tuglu@apt180.apt.emulab.net", "tuglu@apt177.apt.emulab.net", "tuglu@apt136.apt.emulab.net"
# tuglu@apt083.apt.emulab.net tuglu@apt081.apt.emulab.net tuglu@apt138.apt.emulab.net tuglu@apt176.apt.emulab.net tuglu@apt072.apt.emulab.net tuglu@apt161.apt.emulab.net tuglu@apt150.apt.emulab.net tuglu@apt139.apt.emulab.net tuglu@apt180.apt.emulab.net tuglu@apt177.apt.emulab.net tuglu@apt136.apt.emulab.net
SERVERS = ["tuglu@apt083.apt.emulab.net", "tuglu@apt081.apt.emulab.net", "tuglu@apt138.apt.emulab.net"]
PRODUCERS = list(set(["tuglu@apt176.apt.emulab.net", "tuglu@apt072.apt.emulab.net", "tuglu@apt161.apt.emulab.net", "tuglu@apt150.apt.emulab.net"]))
CONSUMERS = list(set(["tuglu@apt139.apt.emulab.net", "tuglu@apt180.apt.emulab.net", "tuglu@apt177.apt.emulab.net", "tuglu@apt136.apt.emulab.net"]))
ALL_HOSTS = SERVERS + PRODUCERS + CONSUMERS

STRATEGIES = ["mpmc_simple", "mpmc_synra", "mpmc_mu"]
SINGLE_SERVER_STRATEGIES = {"mpmc_simple"}

HDR = "strategy,client_machine_id,clients,locks,active_window,zipf_skew,total_ops,wall_s,goodput,mean_us,p50_us,p90_us,p99_us,p99.9_us,max_us"


def ssh(host, cmd, **kwargs):
    return subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=no", host, cmd],
        **kwargs,
    )


def kill_rdma():
    procs = [ssh(h, "sudo pkill -9 -f './rdma' 2>/dev/null; sudo killall -9 rdma 2>/dev/null; true") for h in ALL_HOSTS]
    for p in procs:
        p.wait()
    time.sleep(1)


def run_trial(strategy, clients_per_machine, window, ops_per_client):
    """Run a single trial. ops_per_client is the ops each client thread performs."""
    total_machines = len(PRODUCERS) + len(CONSUMERS)
    total_clients = clients_per_machine * total_machines
    total_ops = ops_per_client * total_clients
    env = (
        f"STRATEGY={strategy} CLIENTS_PER_MACHINE={clients_per_machine}"
        f" NUM_OPS={total_ops} MPMC_ACTIVE_WINDOW={window}"
        f" TOTAL_CLIENT_MACHINES={total_machines}"
    )
    num_servers = 1 if strategy in SINGLE_SERVER_STRATEGIES else len(SERVERS)

    kill_rdma()

    # Start servers
    server_procs = []
    for i in range(num_servers):
        p = ssh(SERVERS[i], f"cd /local/rdma && sudo {env} bash scripts/run.sh")
        server_procs.append(p)
    if num_servers > 0:
        time.sleep(3)

    # Start all producers and consumers, capture output
    client_procs = []
    client_outputs = []

    machine_id = 0
    for host in PRODUCERS:
        out = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".txt")
        p = ssh(host,
                f"cd /local/rdma && sudo IS_CLIENT=1 MACHINE_ID={machine_id} MPMC_IS_PRODUCER=1 {env} bash scripts/run.sh",
                stdout=out, stderr=subprocess.STDOUT)
        client_procs.append(p)
        client_outputs.append(out)
        machine_id += 1

    for host in CONSUMERS:
        out = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".txt")
        p = ssh(host,
                f"cd /local/rdma && sudo IS_CLIENT=1 MACHINE_ID={machine_id} MPMC_IS_PRODUCER=0 {env} bash scripts/run.sh",
                stdout=out, stderr=subprocess.STDOUT)
        client_procs.append(p)
        client_outputs.append(out)
        machine_id += 1

    # Wait for all clients
    rcs = [p.wait() for p in client_procs]
    kill_rdma()

    # Collect output
    texts = []
    for out in client_outputs:
        out.seek(0)
        texts.append(out.read())
        Path(out.name).unlink()

    if any(rc != 0 for rc in rcs):
        return None, texts

    csv_lines = []
    for text in texts:
        for line in text.splitlines():
            if line.startswith("CSV: "):
                csv_lines.append(line[5:])
    return csv_lines, texts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default=",".join(STRATEGIES),
                   help="Comma-separated strategies (default: all three)")
    p.add_argument("--clients", default="4,8,16,32,64,128,256",
                   help="Total clients per side, comma-separated (default: 4,8,16,32,64,128,256)")
    p.add_argument("--ops-per-client", default="100000,1000000,10000000",
                   help="Ops per client, comma-separated (default: 100K,1M,10M,50M,100M)")
    p.add_argument("--window", type=int, default=1)
    p.add_argument("-o", "--output", default="results.csv")
    args = p.parse_args()

    strategies = args.strategy.split(",")
    n_prod_machines = len(PRODUCERS)
    n_cons_machines = len(CONSUMERS)
    n_machines = n_prod_machines + n_cons_machines

    # Parse client counts (per side) and convert to clients_per_machine
    client_counts = []
    for c in args.clients.split(","):
        total_per_side = int(c)
        per_machine = total_per_side // max(n_prod_machines, n_cons_machines)
        if per_machine < 1:
            print(f"WARNING: skipping clients={total_per_side}, not enough machines "
                  f"(need at least {max(n_prod_machines, n_cons_machines)})")
            continue
        actual_per_side = per_machine * max(n_prod_machines, n_cons_machines)
        client_counts.append((per_machine, actual_per_side))

    ops_per_client_values = [int(x) for x in args.ops_per_client.split(",")]
    total_trials = len(strategies) * len(client_counts) * len(ops_per_client_values)

    results_file = Path(args.output)
    results_file.write_text(f"{HDR}\n")

    print(HDR)
    print(f"# strategies={strategies}")
    print(f"# producer machines={n_prod_machines}  consumer machines={n_cons_machines}")
    print(f"# clients per side: {[c[1] for c in client_counts]}")
    print(f"# ops/client: {[f'{v:,}' for v in ops_per_client_values]}")
    print(f"# total trials: {total_trials}")
    print()

    # Kill any leftover rdma processes on all nodes before starting
    print("Killing any existing rdma processes on all nodes...")
    kill_rdma()
    print()

    trial_num = 0
    for strat in strategies:
        for per_machine, actual_per_side in client_counts:
            for opc in ops_per_client_values:
                trial_num += 1
                num_srv = 1 if strat in SINGLE_SERVER_STRATEGIES else len(SERVERS)
                total_clients = per_machine * n_machines
                total_ops = opc * total_clients
                print(f"[{trial_num}/{total_trials}] strategy={strat}  "
                      f"producers={actual_per_side} consumers={actual_per_side} "
                      f"ops_per_client={opc:,}  total_ops={total_ops:,}  "
                      f"replica_servers={num_srv} client_servers={n_machines}")

                csv_lines, texts = run_trial(strat, per_machine, args.window, opc)

                if csv_lines is None:
                    print(f"  !!! FAILED")
                    for text in texts:
                        print(text)
                elif csv_lines:
                    with open(results_file, "a") as f:
                        f.write("\n".join(csv_lines) + "\n")
                    # Summary: aggregate goodput and ops by role
                    prod_goodputs = []
                    cons_goodputs = []
                    total_pushes = 0
                    total_pops = 0
                    for line in csv_lines:
                        fields = line.split(",")
                        mid = int(fields[1])
                        gp = float(fields[8])
                        ops_count = int(fields[6])
                        if mid < n_prod_machines:
                            prod_goodputs.append(gp)
                            total_pushes += ops_count
                        else:
                            cons_goodputs.append(gp)
                            total_pops += ops_count
                    prod_total = sum(prod_goodputs)
                    cons_total = sum(cons_goodputs)
                    print(f"  Prod: {prod_total:,.0f} ops/s ({actual_per_side} clients)  |  "
                          f"Cons: {cons_total:,.0f} ops/s ({actual_per_side} clients)")
                    print(f"  Pushes: {total_pushes:,}  |  Pops: {total_pops:,}")
                print()

    print(f"Results saved to {results_file}")


if __name__ == "__main__":
    main()
