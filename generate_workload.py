#!/usr/bin/env python3
"""
Build workload_{date}.yaml from pai_job_table.csv + pai_task_table.csv.

- Jobs: status is Running (case-insensitive).
- Tasks: same job_name, plan_gpu non-zero (and parseable).
- inst_num -> replicas; plan_cpu/100 -> cpu; plan_mem -> memory (Gi);
  plan_gpu -> flexnpu-core.percentage and flexnpu-memory.128mi (same value).

Performance:
- Task scan defaults to **Polars** when `pip install polars` is available (often
  ~2–4× faster on multi-million rows); otherwise falls back to **csv.reader** with
  column indices + 8 MiB read buffer (already much faster than DictReader).
- Use `--stdlib-only` to force the stdlib path. YAML emission of tens of thousands
  of jobs is usually the next bottleneck (disk I/O).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

READ_BUFFER = 8 * 1024 * 1024  # 8 MiB
_WARN_MISMATCH_LIMIT = 50

_NON_DNS = re.compile(r"[^a-z0-9-]+")
_DNS_DUP = re.compile(r"-+")


def _header_map(header_row: list[str]) -> dict[str, int]:
    return {name.strip(): i for i, name in enumerate(header_row)}


def fast_float(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def dns_label(s: str) -> str:
    """RFC 1123-ish label: lowercase, safe chars, max 63."""
    s = s.strip().lower()
    s = _NON_DNS.sub("-", s)
    s = _DNS_DUP.sub("-", s).strip("-")
    return (s[:63] if s else "task") or "task"


def flexnpu_num_for_container(plan_gpu: float) -> int:
    if plan_gpu <= 0:
        return 1
    return max(1, int(math.ceil(plan_gpu / 100.0)))


def format_cpu(plan_cpu: float) -> str:
    v = plan_cpu / 100.0
    if v == int(v):
        return str(int(v))
    return repr(v)


def format_mem(plan_mem: float) -> str:
    if plan_mem == int(plan_mem):
        return f"{int(plan_mem)}Gi"
    return f"{plan_mem}Gi"


def format_flex(plan_gpu: float) -> str:
    if plan_gpu == int(plan_gpu):
        return str(int(plan_gpu))
    return repr(plan_gpu)


def load_running_jobs(path: Path) -> set[str]:
    out: set[str] = set()
    with path.open(newline="", encoding="utf-8", buffering=READ_BUFFER) as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return out
        col = _header_map(header)
        try:
            ji = col["job_name"]
            si = col["status"]
        except KeyError as e:
            raise SystemExit(f"job CSV missing column: {e}") from e
        for row in reader:
            if si >= len(row) or ji >= len(row):
                continue
            if row[si].strip().lower() != "running":
                continue
            jn = row[ji].strip()
            if jn:
                out.add(jn)
    return out


def aggregate_tasks_reader(
    task_path: Path,
    running: set[str],
    *,
    warn_mismatch: bool,
) -> dict[str, dict[str, dict]]:
    """
    job_name -> task_name -> {inst_num, plan_cpu, plan_mem, plan_gpu}
    """
    agg: dict[str, dict[str, dict]] = defaultdict(dict)
    warn_left = _WARN_MISMATCH_LIMIT if warn_mismatch else 0
    mismatch_count = 0

    in_running = running.__contains__
    with task_path.open(newline="", encoding="utf-8", buffering=READ_BUFFER) as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return agg
        col = _header_map(header)
        try:
            ji = col["job_name"]
            ti = col["task_name"]
            ii = col["inst_num"]
            pci = col["plan_cpu"]
            pmi = col["plan_mem"]
            pgi = col["plan_gpu"]
        except KeyError as e:
            raise SystemExit(f"task CSV missing column: {e}") from e

        for row in reader:
            if ji >= len(row):
                continue
            jn = row[ji].strip()
            if not in_running(jn):
                continue
            pg_raw = row[pgi] if pgi < len(row) else ""
            pg = fast_float(pg_raw)
            if pg is None or pg == 0.0:
                continue
            tn_raw = row[ti] if ti < len(row) else ""
            tn = tn_raw.strip() or "task"
            inst = fast_float(row[ii] if ii < len(row) else None)
            if inst is None:
                continue
            replicas = int(inst)
            if replicas < 1:
                continue
            pc = fast_float(row[pci] if pci < len(row) else None)
            pm = fast_float(row[pmi] if pmi < len(row) else None)
            if pc is None or pm is None:
                continue

            slot = agg[jn].get(tn)
            if slot is None:
                agg[jn][tn] = {
                    "inst_num": replicas,
                    "plan_cpu": pc,
                    "plan_mem": pm,
                    "plan_gpu": pg,
                }
            else:
                slot["inst_num"] += replicas
                if (
                    abs(slot["plan_cpu"] - pc) > 1e-6
                    or abs(slot["plan_mem"] - pm) > 1e-6
                    or abs(slot["plan_gpu"] - pg) > 1e-6
                ):
                    mismatch_count += 1
                    if warn_left > 0:
                        print(
                            f"Warning: resource mismatch for job={jn!r} task={tn!r} "
                            f"(keeping first row's plan_*); suppressing further details",
                            file=sys.stderr,
                        )
                        warn_left -= 1

    if warn_mismatch and mismatch_count > _WARN_MISMATCH_LIMIT:
        print(
            f"Warning: at least {mismatch_count} aggregate keys had plan_* mismatch "
            f"(only first {_WARN_MISMATCH_LIMIT} logged).",
            file=sys.stderr,
        )
    return agg


def aggregate_tasks_polars(
    task_path: Path,
    running: set[str],
) -> dict[str, dict[str, dict]]:
    import polars as pl

    rlist = list(running)
    df = pl.read_csv(
        task_path,
        columns=["job_name", "task_name", "inst_num", "plan_cpu", "plan_mem", "plan_gpu"],
        infer_schema_length=5000,
    )
    df = df.filter(pl.col("job_name").is_in(rlist))
    df = df.with_columns(
        [
            pl.col("plan_gpu").cast(pl.Float64, strict=False),
            pl.col("inst_num").cast(pl.Float64, strict=False),
            pl.col("plan_cpu").cast(pl.Float64, strict=False),
            pl.col("plan_mem").cast(pl.Float64, strict=False),
        ]
    )
    df = df.filter(
        pl.col("plan_gpu").is_not_null()
        & (pl.col("plan_gpu") != 0.0)
        & pl.col("inst_num").is_not_null()
        & (pl.col("inst_num") >= 1.0)
        & pl.col("plan_cpu").is_not_null()
        & pl.col("plan_mem").is_not_null()
    )
    g = df.group_by(["job_name", "task_name"], maintain_order=True).agg(
        [
            pl.col("inst_num").sum().cast(pl.Int64),
            pl.col("plan_cpu").first(),
            pl.col("plan_mem").first(),
            pl.col("plan_gpu").first(),
        ]
    )
    agg: dict[str, dict[str, dict]] = defaultdict(dict)
    for jn, tn, ins, pc, pm, pg in g.iter_rows():
        jn = str(jn).strip()
        tn = str(tn).strip() or "task"
        agg[jn][tn] = {
            "inst_num": int(ins),
            "plan_cpu": float(pc),
            "plan_mem": float(pm),
            "plan_gpu": float(pg),
        }
    return agg


def build_flexnpu_num_map(job_tasks: list[tuple[str, str, dict]]) -> str:
    parts: dict[str, int] = {}
    for _tn, cname, spec in job_tasks:
        parts[cname] = flexnpu_num_for_container(float(spec["plan_gpu"]))
    inner = json.dumps(parts, separators=(", ", ": "))
    return "{ " + inner[1:-1] + " }" if inner.startswith("{") else inner


def emit_job_yaml(
    job_key: str,
    job_tasks: list[tuple[str, str, dict]],
    flexnpu_ann: str,
) -> str:
    total_replicas = sum(spec["inst_num"] for _tn, _c, spec in job_tasks)
    lines: list[str] = [
        "  - apiVersion: batch.volcano.sh/v1alpha1",
        "    kind: Job",
        "    metadata:",
        "      labels:",
        "        sub-time: '0'",
        f"      name: {job_key}",
        "      namespace: default",
        "      annotations:",
        f"        volcano.sh/flexnpu-num: '{flexnpu_ann}'",
        "    spec:",
        f"      minAvailable: {total_replicas}",
        "      schedulerName: volcano",
        "      tasks:",
    ]
    for task_name, cname, spec in job_tasks:
        r = spec["inst_num"]
        cpu_s = format_cpu(float(spec["plan_cpu"]))
        mem_s = format_mem(float(spec["plan_mem"]))
        flex_s = format_flex(float(spec["plan_gpu"]))
        vol_task = dns_label(task_name)
        lines.extend(
            [
                f"        - name: {vol_task}",
                f"          replicas: {r}",
                "          spec:",
                "            containers:",
                f"              - name: {cname}",
                "                resources:",
                "                  requests:",
                f"                    cpu: '{cpu_s}'",
                f"                    memory: {mem_s}",
                f'                    volcano.sh/flexnpu-core.percentage: "{flex_s}"',
                f'                    volcano.sh/flexnpu-memory.128mi: "{flex_s}"',
                "                  limits:",
                f"                    cpu: '{cpu_s}'",
                f"                    memory: {mem_s}",
                f'                    volcano.sh/flexnpu-core.percentage: "{flex_s}"',
                f'                    volcano.sh/flexnpu-memory.128mi: "{flex_s}"',
            ]
        )
    return "\n".join(lines)


def main() -> int:
    root = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Generate workload YAML from PAI job/task tables")
    p.add_argument("--job-csv", type=Path, default=root / "pai_job_table.csv")
    p.add_argument("--task-csv", type=Path, default=root / "pai_task_table.csv")
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output path (default: workload_YYYYMMDD.yaml under script dir)",
    )
    p.add_argument("--date", default=None, help="Date suffix YYYYMMDD (default: today)")
    p.add_argument(
        "--npu-granularity-percent",
        type=int,
        default=25,
        help="JobList spec.npuGranularityPercent (default: 25, same as example)",
    )
    p.add_argument(
        "--stdlib-only",
        action="store_true",
        help="Do not use Polars; force stdlib csv.reader (even if polars is installed)",
    )
    p.add_argument(
        "--warn-mismatch",
        action="store_true",
        help="Log plan_* conflicts when merging duplicate (job_name, task_name) rows "
        f"(default off for speed; at most {_WARN_MISMATCH_LIMIT} messages)",
    )
    args = p.parse_args()

    if not args.job_csv.is_file():
        print(f"Missing job CSV: {args.job_csv}", file=sys.stderr)
        return 1
    if not args.task_csv.is_file():
        print(f"Missing task CSV: {args.task_csv}", file=sys.stderr)
        return 1

    d = args.date or date.today().strftime("%Y%m%d")
    out_path = args.output or (root / f"workload_{d}.yaml")

    print("Loading Running jobs...", file=sys.stderr)
    running = load_running_jobs(args.job_csv)
    print(f"  {len(running)} job_name keys", file=sys.stderr)

    print("Scanning tasks...", file=sys.stderr)
    by_job = None
    engine = "stdlib"
    if not args.stdlib_only:
        try:
            by_job = aggregate_tasks_polars(args.task_csv, running)
            engine = "polars"
        except ImportError:
            pass
    if by_job is None:
        by_job = aggregate_tasks_reader(
            args.task_csv, running, warn_mismatch=args.warn_mismatch
        )
    print(f"  engine={engine}", file=sys.stderr)

    jobs_with_tasks = {jn: tasks for jn, tasks in by_job.items() if tasks}
    print(f"  {len(jobs_with_tasks)} jobs with GPU tasks (plan_gpu != 0)", file=sys.stderr)

    print("Writing YAML...", file=sys.stderr)
    job_blocks: list[str] = []
    for jn in sorted(jobs_with_tasks.keys()):
        tasks_map = jobs_with_tasks[jn]
        job_tasks: list[tuple[str, str, dict]] = []
        used_cnames: set[str] = set()
        for tn in sorted(tasks_map.keys()):
            base = dns_label(tn)
            cname = f"c-{base}"
            suffix = 0
            while cname in used_cnames:
                suffix += 1
                cname = f"c-{base}-{suffix}"[:63]
            used_cnames.add(cname)
            job_tasks.append((tn, cname, tasks_map[tn]))

        flex_ann = build_flexnpu_num_map(job_tasks)
        job_blocks.append(emit_job_yaml(jn, job_tasks, flex_ann).rstrip())

    header = f"""# yaml-language-server: $schema=../../schemas/workload/workload-input.schema.json
apiVersion: v1
kind: JobList
spec:
  npuGranularityPercent: {args.npu_granularity_percent}
jobs:
"""
    out_path.write_text(header + "\n".join(job_blocks) + "\n", encoding="utf-8")
    print(f"Wrote {len(job_blocks)} jobs to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
