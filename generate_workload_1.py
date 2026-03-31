#!/usr/bin/env python3
"""Generate workload_1.yaml from workload_config_example/workload.yaml layout."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "workload_1.yaml"

# 3 jobs × 100 tasks, 2 × 50, 1 × 200, 1 × 600
REPLICAS_PER_JOB = [100, 100, 100, 50, 50, 200, 600]

NPU_GRANULARITY_PERCENT = 1
FLEXNPU_CORE = "20"
FLEXNPU_MEM = "30"
FLEXNPU_NUM = 1  # single NPU slice per pod; annotation maps container name -> count

CONTAINER_NAME = "task-container"
TASK_NAME = "worker"


def job_yaml(job_index: int, replicas: int) -> str:
    name = f"workload1-job-{job_index}"
    ann = '{{ "{0}": {1} }}'.format(CONTAINER_NAME, FLEXNPU_NUM)

    return f"""  - apiVersion: batch.volcano.sh/v1alpha1
    kind: Job
    metadata:
      labels:
        sub-time: '0'
      name: {name}
      namespace: default
      annotations:
        volcano.sh/flexnpu-num: '{ann}'
    spec:
      minAvailable: {replicas}
      schedulerName: volcano
      tasks:
        - name: {TASK_NAME}
          replicas: {replicas}
          spec:
            containers:
              - name: {CONTAINER_NAME}
                resources:
                  requests:
                    cpu: '0.3'
                    memory: 4Gi
                    volcano.sh/flexnpu-core.percentage: "{FLEXNPU_CORE}"
                    volcano.sh/flexnpu-memory.128mi: "{FLEXNPU_MEM}"
                  limits:
                    cpu: '0.3'
                    memory: 4Gi
                    volcano.sh/flexnpu-core.percentage: "{FLEXNPU_CORE}"
                    volcano.sh/flexnpu-memory.128mi: "{FLEXNPU_MEM}"
"""


def main() -> None:
    header = f"""# yaml-language-server: $schema=../../schemas/workload/workload-input.schema.json
apiVersion: v1
kind: JobList
spec:
  npuGranularityPercent: {NPU_GRANULARITY_PERCENT}
jobs:
"""
    blocks = [job_yaml(i, r).rstrip() for i, r in enumerate(REPLICAS_PER_JOB)]
    OUTPUT.write_text(header + "\n".join(blocks) + "\n", encoding="utf-8")
    print(f"Wrote {len(REPLICAS_PER_JOB)} jobs to {OUTPUT}")


if __name__ == "__main__":
    main()
