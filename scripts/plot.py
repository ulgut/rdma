#!/usr/bin/env python3
"""Plot MPMC benchmark results from a CSV file.

Usage:
    python3 scripts/plot.py results.csv
    python3 scripts/plot.py results.csv -o plots.png
"""

import argparse

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

QUEUE_SLOTS = 1024
SLOT_TURN_B = 8
SLOT_PAYLOAD_B = 56
SLOT_SIZE_B = SLOT_TURN_B + SLOT_PAYLOAD_B


def load(path):
    df = pd.read_csv(path)
    df["role"] = df["client_machine_id"].map({0: "producer", 1: "consumer"})
    return df


def print_summary(df):
    strategies = sorted(df["strategy"].unique())
    client_counts = sorted(df["clients"].unique())
    windows = sorted(df["active_window"].unique())

    print()
    print("=" * 72)
    print("  MPMC QUEUE BENCHMARK SUMMARY")
    print("=" * 72)
    print(f"  Strategies:     {', '.join(strategies)}")
    client_str = ', '.join(f"{int(c)} ({int(c)//2}P+{int(c)//2}C)" for c in client_counts)
    print(f"  Client counts:  {client_str}")
    print(f"  Active window:  {', '.join(str(int(w)) for w in windows)}")
    print(f"  Queue slots:    {QUEUE_SLOTS}")
    print(f"  Slot size:      {SLOT_SIZE_B}B ({SLOT_TURN_B}B turn + {SLOT_PAYLOAD_B}B payload)")

    # Compute ops/client from data
    ops_per_client = sorted(df["total_ops"].unique())
    print(f"  Ops/client:     {', '.join(str(int(o)) for o in ops_per_client)}")
    print("=" * 72)

    for strat in strategies:
        print(f"\n  [{strat}]")
        print(f"  {'clients':>8}  {'role':>10}  {'goodput':>12}  {'mean':>8}  {'p50':>8}  {'p90':>8}  {'p99':>8}  {'p99.9':>8}  {'max':>8}")
        print(f"  {'':>8}  {'':>10}  {'(ops/s)':>12}  {'(us)':>8}  {'(us)':>8}  {'(us)':>8}  {'(us)':>8}  {'(us)':>8}  {'(us)':>8}")
        print("  " + "-" * 86)
        sub = df[df["strategy"] == strat].sort_values(["clients", "client_machine_id"])
        for _, row in sub.iterrows():
            print(f"  {int(row['clients']):>8}  {row['role']:>10}  {row['goodput']:>12,.0f}  "
                  f"{row['mean_us']:>8.2f}  {row['p50_us']:>8.2f}  {row['p90_us']:>8.2f}  "
                  f"{row['p99_us']:>8.2f}  {row['p99.9_us']:>8.2f}  {row['max_us']:>8.2f}")

    # Overhead comparison
    if "mpmc_synra" in strategies and "mpmc_simple" in strategies:
        print(f"\n  [replication overhead: synra / simple]")
        print(f"  {'clients':>8}  {'role':>10}  {'goodput ratio':>14}  {'latency ratio':>14}")
        print("  " + "-" * 50)
        simple = df[df["strategy"] == "mpmc_simple"]
        synra = df[df["strategy"] == "mpmc_synra"]
        for cli in client_counts:
            for role in ["producer", "consumer"]:
                s = simple[(simple["clients"] == cli) & (simple["role"] == role)]
                r = synra[(synra["clients"] == cli) & (synra["role"] == role)]
                if s.empty or r.empty:
                    continue
                gp_ratio = r.iloc[0]["goodput"] / s.iloc[0]["goodput"]
                lat_ratio = r.iloc[0]["mean_us"] / s.iloc[0]["mean_us"]
                print(f"  {int(cli):>8}  {role:>10}  {gp_ratio:>14.3f}  {lat_ratio:>14.3f}")

    print()
    print("=" * 72)
    print()


def plot(df, out):
    strategies = df["strategy"].unique()
    windows = sorted(df["active_window"].unique())
    client_counts = sorted(df["clients"].unique())
    roles = ["producer", "consumer"]
    colors = {"producer": "#2563eb", "consumer": "#dc2626"}
    markers = {"producer": "o", "consumer": "s"}
    strat_alpha = lambda s: 0.5 if "synra" in s else 1.0
    strat_lw = lambda s: 1.5 if "synra" in s else 2.0

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # ── Title with experiment info ──
    client_str = ', '.join(f"{int(c)}({int(c)//2}P+{int(c)//2}C)" for c in client_counts)
    ops_per_client = sorted(df["total_ops"].unique())
    ops_str = ', '.join(str(int(o)) for o in ops_per_client)
    info_lines = [
        f"Queue: {QUEUE_SLOTS} slots x {SLOT_SIZE_B}B ({SLOT_TURN_B}B turn + {SLOT_PAYLOAD_B}B payload)   |   "
        f"Active window: {', '.join(str(w) for w in windows)}   |   "
        f"Ops/client: {ops_str}\n"
        f"Clients: {client_str}   |   "
        f"Strategies: {', '.join(strategies)}"
    ]
    fig.suptitle("MPMC Queue Benchmark\n" + info_lines[0],
                 fontsize=10, fontweight="bold", linespacing=1.5)

    # ── Goodput vs clients ──
    ax = axes[0][0]
    for strat in strategies:
        for role in roles:
            sub = df[(df["strategy"] == strat) & (df["role"] == role)]
            ls = "-" if role == "producer" else "--"
            ax.plot(sub["clients"], sub["goodput"], marker=markers[role],
                    color=colors[role], linestyle=ls, label=f"{strat} ({role})",
                    alpha=strat_alpha(strat), linewidth=strat_lw(strat))
    ax.set_xlabel("Total clients")
    ax.set_ylabel("Goodput (ops/s)")
    ax.set_title("Throughput vs. Client Count")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1e3:.0f}K"))
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)

    # ── Mean latency vs clients ──
    ax = axes[0][1]
    for strat in strategies:
        for role in roles:
            sub = df[(df["strategy"] == strat) & (df["role"] == role)]
            ls = "-" if role == "producer" else "--"
            ax.plot(sub["clients"], sub["mean_us"], marker=markers[role],
                    color=colors[role], linestyle=ls, label=f"{strat} ({role})",
                    alpha=strat_alpha(strat), linewidth=strat_lw(strat))
    ax.set_xlabel("Total clients")
    ax.set_ylabel("Mean latency (us)")
    ax.set_title("Mean Latency vs. Client Count")
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)

    # ── Latency percentiles (grouped bar) ──
    ax = axes[1][0]
    pcts = ["p50_us", "p90_us", "p99_us"]
    pct_labels = ["p50", "p90", "p99"]
    bars = []
    bar_labels = []
    for strat in strategies:
        for role in roles:
            sub = df[(df["strategy"] == strat) & (df["role"] == role)]
            row = sub.loc[sub["clients"].idxmax()]
            bars.append([row[p] for p in pcts])
            bar_labels.append(f"{strat}\n{role}\n({int(row['clients'])}c)")

    x = range(len(bars))
    w = 0.25
    for i, (pct, lbl) in enumerate(zip(zip(*bars), pct_labels)):
        offsets = [xi + (i - 1) * w for xi in x]
        ax.bar(offsets, pct, w, label=lbl, alpha=0.85)
    ax.set_xticks(list(x))
    ax.set_xticklabels(bar_labels, fontsize=7)
    ax.set_ylabel("Latency (us)")
    ax.set_title("Tail Latency at Max Client Count")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis="y")

    # ── Synra overhead (ratio) ──
    ax = axes[1][1]
    if "mpmc_synra" in strategies and "mpmc_simple" in strategies:
        simple = df[df["strategy"] == "mpmc_simple"].copy()
        synra = df[df["strategy"] == "mpmc_synra"].copy()
        for role in roles:
            s = simple[simple["role"] == role].set_index("clients")
            r = synra[synra["role"] == role].set_index("clients")
            common = s.index.intersection(r.index)
            if len(common) == 0:
                continue
            ratio = r.loc[common, "goodput"] / s.loc[common, "goodput"]
            ax.plot(common, ratio, marker=markers[role], color=colors[role],
                    linestyle="-", label=f"{role}", linewidth=2)
        ax.axhline(1.0, color="gray", linestyle=":", alpha=0.5)
        ax.set_xlabel("Total clients")
        ax.set_ylabel("synra / simple goodput ratio")
        ax.set_title("Replication Overhead")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "Need both mpmc_simple\nand mpmc_synra to\nshow overhead ratio",
                ha="center", va="center", transform=ax.transAxes, fontsize=10, color="gray")
        ax.set_title("Replication Overhead")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(out, dpi=150)
    print(f"Saved to {out}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("csv", help="Path to results CSV")
    p.add_argument("-o", "--output", default="plots.png")
    args = p.parse_args()

    df = load(args.csv)
    print_summary(df)
    plot(df, args.output)


if __name__ == "__main__":
    main()
