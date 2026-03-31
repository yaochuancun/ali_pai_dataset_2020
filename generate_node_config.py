#!/usr/bin/env python3
"""
Build a Volcano/K8s NodeList YAML from pai_machine_spec.csv (non-CPU rows only).

Optional positional N|all: how many nodes go into the single -o file — N = first N
nodes after filters then stop; all (or omit) = every matching row in one file.

CPU cores: allocatable.cpu / capacity.cpu = cap_cpu / cpu_divisor (default 1000 per
spec; this trace often stores core counts as 64/96 — use --cpu-divisor 1 then).

Memory: cap_mem with Gi suffix. NPU: cap_gpu cards, flex lists of 100 each, sums
for percentage and 128mi resources. HCCS/SIO topologies per cap_gpu rules.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


def hccs_group_strings(cap_gpu: int, *, hccs_8_mode: str) -> list[str]:
    """HCCS group labels as comma-separated device indices (strings)."""
    n = cap_gpu
    if n <= 0:
        return []
    if n == 2:
        return ["0,1"]
    if n == 8:
        if hccs_8_mode == "quads":
            return ["0,1,2,3", "4,5,6,7"]
        return ["0,1", "2,3", "4,5", "6,7"]
    # Other counts: consecutive pairs; odd last index alone
    groups: list[str] = []
    i = 0
    while i < n:
        if i + 1 < n:
            groups.append(f"{i},{i + 1}")
            i += 2
        else:
            groups.append(str(i))
            i += 1
    return groups


def sio_group_strings(cap_gpu: int) -> list[str]:
    return [str(i) for i in range(cap_gpu)]


def flex_list_json(n: int, per_value: int) -> str:
    d = {str(i): per_value for i in range(n)}
    return json.dumps(d, separators=(",", ":"))


def topologies_json(cap_gpu: int, *, hccs_8_mode: str) -> str:
    topo = [
        {"type": "HCCS", "groups": hccs_group_strings(cap_gpu, hccs_8_mode=hccs_8_mode)},
        {"type": "SIO", "groups": sio_group_strings(cap_gpu)},
    ]
    return json.dumps(topo, separators=(",", ":"))


def format_cpu_value(cores: float) -> str:
    if cores == int(cores):
        return str(int(cores))
    return repr(cores)


def emit_node_yaml(
    index: int,
    cap_cpu: float,
    cap_mem: int,
    cap_gpu: int,
    *,
    hccs_8_mode: str,
    per_slice: int,
) -> str:
    n = cap_gpu
    cpu_s = format_cpu_value(cap_cpu)
    mem_s = f"{cap_mem}Gi"

    if n > 0:
        pct_list = flex_list_json(n, per_slice)
        mem_list = flex_list_json(n, per_slice)
        topo = topologies_json(n, hccs_8_mode=hccs_8_mode)
        ann_lines = f"""      volcano.sh/flexnpu-mode: baremetal
      volcano.sh/flexnpu-core.percentage-list: '{pct_list}'
      volcano.sh/flexnpu-memory.128mi-list: '{mem_list}'
      topologies: '{topo}'"""
    else:
        ann_lines = "      volcano.sh/flexnpu-mode: baremetal"

    if n > 0:
        total_s = str(n * per_slice)
        alloc = f"""      allocatable:
        cpu: {cpu_s}
        memory: {mem_s}
        volcano.sh/flexnpu-core.percentage: "{total_s}"
        volcano.sh/flexnpu-memory.128mi: "{total_s}"
        pods: 110"""
        cap = f"""      capacity:
        cpu: {cpu_s}
        memory: {mem_s}
        volcano.sh/flexnpu-core.percentage: "{total_s}"
        volcano.sh/flexnpu-memory.128mi: "{total_s}"
        pods: 110"""
    else:
        alloc = f"""      allocatable:
        cpu: {cpu_s}
        memory: {mem_s}
        pods: 110"""
        cap = f"""      capacity:
        cpu: {cpu_s}
        memory: {mem_s}
        pods: 110"""

    return f"""  - name: node-{index}
    labels:
      beta.kubernetes.io/arch: arm64
      beta.kubernetes.io/os: simulated
      linc/nodeType: cloud
    annotations:
{ann_lines}
    status:
{alloc}
{cap}
"""


def nodelist_header() -> str:
    return """# yaml-language-server: $schema=../../schemas/cluster/cluster-input.schema.json
apiVersion: v1
kind: NodeList
spec:
  kubernetesVersion: "1.31"
  volcanoVersion: "1.12"
nodes:
"""


def parse_nodes_spec(value: str) -> int | str:
    """Return positive int (cap node count in one file) or 'all' (no cap)."""
    s = value.strip().lower()
    if s == "all":
        return "all"
    try:
        n = int(s, 10)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"expected a positive integer or 'all', got {value!r}"
        ) from e
    if n < 1:
        raise argparse.ArgumentTypeError("node count must be >= 1")
    return n


def main() -> int:
    root = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(
        description="Generate NodeList YAML from pai_machine_spec.csv",
        epilog="Example for this trace (cap_cpu is core count): %(prog)s --cpu-divisor 1 -o cluster.yaml",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=root / "pai_machine_spec.csv",
        help="Machine spec CSV (no header row; columns from pai_machine_spec.header)",
    )
    p.add_argument(
        "--header",
        type=Path,
        default=root / "pai_machine_spec.header",
        help="Single-line CSV header",
    )
    p.add_argument("-o", "--output", type=Path, default=root / "cluster_from_spec.yaml")
    p.add_argument(
        "--cpu-divisor",
        type=float,
        default=1000.0,
        help="cpu = cap_cpu / this (default 1000). Use 1 for core counts like 64/96 in this trace.",
    )
    p.add_argument(
        "--hccs-8-mode",
        choices=("quads", "pairs"),
        default="quads",
        help='For cap_gpu==8: "quads" => 0-3,4-7; "pairs" => 0-1,2-3,4-5,6-7',
    )
    p.add_argument(
        "--per-slice",
        type=int,
        default=100,
        help="Value per NPU in flexnpu-core.percentage-list and flexnpu-memory.128mi-list",
    )
    p.add_argument(
        "--skip-zero-gpu",
        action="store_true",
        help="Omit rows where cap_gpu is 0 after filtering non-CPU",
    )
    p.add_argument(
        "nodes",
        nargs="?",
        default=None,
        metavar="N|all",
        type=parse_nodes_spec,
        help=(
            "Nodes in the single -o file: positive integer = first N then stop; "
            "all = no limit. Omit defaults to all."
        ),
    )
    args = p.parse_args()

    fieldnames = args.header.read_text(encoding="utf-8").strip().split(",")
    rows: list[dict[str, str]] = []
    with args.csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            if row["gpu_type"].strip().upper() == "CPU":
                continue
            rows.append(row)

    warned_cpu = False
    blocks: list[str] = []
    node_index = 0
    limit: int | None = None
    if args.nodes is not None and isinstance(args.nodes, int):
        limit = args.nodes

    for row in rows:
        cap_cpu_i = int(row["cap_cpu"])
        cap_mem_i = int(row["cap_mem"])
        cap_gpu_i = int(row["cap_gpu"])
        if args.skip_zero_gpu and cap_gpu_i == 0:
            continue

        cores = cap_cpu_i / args.cpu_divisor
        if not warned_cpu and cores < 1 and cap_cpu_i >= 32:
            print(
                "Warning: cap_cpu / cpu_divisor < 1 for at least one row "
                f"(e.g. cap_cpu={cap_cpu_i}). For this trace, try --cpu-divisor 1.",
                file=sys.stderr,
            )
            warned_cpu = True

        blocks.append(
            emit_node_yaml(
                node_index,
                cores,
                cap_mem_i,
                cap_gpu_i,
                hccs_8_mode=args.hccs_8_mode,
                per_slice=args.per_slice,
            ).rstrip("\n")
        )
        node_index += 1
        if limit is not None and len(blocks) >= limit:
            break

    header = nodelist_header()
    if limit is not None and len(blocks) < limit:
        print(
            f"Warning: requested {limit} nodes but only {len(blocks)} rows matched filters.",
            file=sys.stderr,
        )
    args.output.write_text(header + "\n".join(blocks) + "\n", encoding="utf-8")
    print(f"Wrote {len(blocks)} nodes to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
