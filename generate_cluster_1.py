#!/usr/bin/env python3
"""Generate cluster_1.yaml from cluster_config_example/cluster.yaml layout."""

from __future__ import annotations

import json
from pathlib import Path

NUM_NODES = 15
NPU_COUNT = 6
CORE_PER_NPU = 100
MEMORY_PER_NPU = 512  # per-NPU value in volcano.sh/flexnpu-memory.128mi-list

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "cluster_1.yaml"


def build_topologies(n: int) -> str:
    if n == 6:
        hccs_groups = ["0,1", "2,3", "4,5"]
    else:
        # generic consecutive pairs
        hccs_groups = []
        i = 0
        while i < n:
            if i + 1 < n:
                hccs_groups.append(f"{i},{i + 1}")
                i += 2
            else:
                hccs_groups.append(str(i))
                i += 1
    sio_groups = [str(i) for i in range(n)]
    topo = [
        {"type": "HCCS", "groups": hccs_groups},
        {"type": "SIO", "groups": sio_groups},
    ]
    return json.dumps(topo, separators=(", ", ": "))


def build_flex_list(n: int, value: int) -> str:
    d = {str(i): value for i in range(n)}
    return json.dumps(d, separators=(",", ":"))


def node_block(index: int) -> str:
    n = NPU_COUNT
    core_total = n * CORE_PER_NPU
    mem_total = n * MEMORY_PER_NPU
    pct_list = build_flex_list(n, CORE_PER_NPU)
    mem_list = build_flex_list(n, MEMORY_PER_NPU)
    topo = build_topologies(n)
    ct, mt = str(core_total), str(mem_total)

    return f"""  - name: node-{index}
    labels:
      beta.kubernetes.io/arch: arm64
      beta.kubernetes.io/os: simulated
      linc/nodeType: cloud
    annotations:
      volcano.sh/flexnpu-mode: baremetal
      volcano.sh/flexnpu-core.percentage-list: '{pct_list}'
      volcano.sh/flexnpu-memory.128mi-list: '{mem_list}'
      topologies: '{topo}'
    status:
      allocatable:
        cpu: 96
        memory: 512Gi
        volcano.sh/flexnpu-core.percentage: "{ct}"
        volcano.sh/flexnpu-memory.128mi: "{mt}"
        pods: 110
      capacity:
        cpu: 96
        memory: 512Gi
        volcano.sh/flexnpu-core.percentage: "{ct}"
        volcano.sh/flexnpu-memory.128mi: "{mt}"
        pods: 110"""


def main() -> None:
    header = """# yaml-language-server: $schema=../../schemas/cluster/cluster-input.schema.json
apiVersion: v1
kind: NodeList
spec:
  kubernetesVersion: "1.31"
  volcanoVersion: "1.12"
nodes:
"""
    body = "\n".join(node_block(i).rstrip() for i in range(NUM_NODES))
    OUTPUT.write_text(header + body + "\n", encoding="utf-8")
    print(f"Wrote {NUM_NODES} nodes to {OUTPUT}")


if __name__ == "__main__":
    main()
