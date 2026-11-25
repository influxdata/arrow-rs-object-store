#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Aggregate run-matrix.sh CSV files (ops-<fs>.csv) into markdown tables.

`paired`: per cell, the ratio of ops/s between two variants measured in the same run and round,
then the median and min-max of those ratios over rounds. `full`: per cell and variant, median over
rounds of ops/s (with min-max) and of the per-round p50/p99/p999 latencies, plus per-round load and
normalized-throughput lines that show whether a round was disturbed.

A cell is `op depth state size conc max-ops cleanup chunk-size`. A round is identified by the CSV columns
`run` (one id per run-matrix.sh invocation) and `round`, so appended repeat invocations add rounds
instead of replacing earlier ones; rounds are printed as `r<round>` when the file holds one run id
and `<run>.r<round>` otherwise. A duplicate (cell, variant, run, round) row is an error. CSV files
written before the `cleanup`, `max_ops`, `run` and `chunk_size` columns existed are read with `?`, `?`,
`0` and `0`.

usage: aggregate.py [--baseline V] [--pairs A/B,...] paired|full|both CSV...
  --baseline V   variant every other variant is paired against (default: the first variant in
                 the file)
  --pairs ...    explicit A/B pairs instead of the default; variants are the CSV `variant` labels
"""
import argparse
import csv
import statistics as st
from collections import defaultdict

OPS = ["put", "create", "delete", "copy", "rename", "multipart"]
STATES = {"steady": 0, "shared": 1, "cold": 2, "spread": 3}
CELL_COLS = ["op", "depth", "state", "size", "conc", "max-ops", "cleanup", "chunk"]
# Columns added after the first CSVs were written, with the value used when a file lacks them.
OPTIONAL = {"cleanup": "?", "max_ops": "?", "run": "0", "chunk_size": "0"}


def cell_key(r):
    return (r["op"], int(r["depth"]), r["state"], int(r["size"]), int(r["conc"]),
            r.get("max_ops", OPTIONAL["max_ops"]), r.get("cleanup", OPTIONAL["cleanup"]),
            int(r.get("chunk_size", OPTIONAL["chunk_size"])))


def load(path):
    """fs -> (cells: key -> variant -> (run, round) -> row, variants in order of first appearance)."""
    per_fs = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            cells, variants = per_fs.setdefault(r["fs"], (defaultdict(lambda: defaultdict(dict)), []))
            key = cell_key(r)
            rk = (r.get("run", OPTIONAL["run"]), int(r["round"]))
            if rk in cells[key][r["variant"]]:
                raise SystemExit(
                    f"{path}: duplicate row for cell {' '.join(map(str, key))} variant {r['variant']} "
                    f"run {rk[0]} round {rk[1]}; the same run id was appended twice"
                )
            cells[key][r["variant"]][rk] = r
            if r["variant"] not in variants:
                variants.append(r["variant"])
    return per_fs


def round_labels(cells):
    """(run, round) -> label; the run id is shown only when the file holds more than one."""
    keys = {rk for by in cells.values() for rounds in by.values() for rk in rounds}
    multi = len({run for run, _ in keys}) > 1
    return {rk: f"{rk[0]}.r{rk[1]}" if multi else f"r{rk[1]}" for rk in keys}


def med(rows, col):
    return st.median(float(r[col]) for r in rows)


def fmt_us(v):
    return f"{v / 1000:.2f}" if v >= 1000 else f"{v / 1000:.3f}"


def cell_order(k):
    return (OPS.index(k[0]) if k[0] in OPS else len(OPS), k[3], STATES.get(k[2], 9), k[1], k[4], k[5], k[6], k[7])


def size_s(size):
    return {4096: "4K", 8192: "8K", 65536: "64K", 1048576: "1M", 5242880: "5M", 8388608: "8M"}.get(size, str(size))


def cell_cols(key):
    op, depth, state, size, conc, max_ops, cleanup, chunk = key
    return [op, str(depth), state, size_s(size), str(conc), max_ops, cleanup, size_s(chunk) if chunk else "one"]


def paired(by, a, b, col="ops_per_s"):
    rounds = sorted(set(by.get(a, {})) & set(by.get(b, {})))
    if not rounds:
        return None
    ratios = [float(by[a][r][col]) / float(by[b][r][col]) for r in rounds]
    return st.median(ratios), min(ratios), max(ratios), len(ratios)


def paired_table(fs, cells, variants, pairs):
    out = [f"### {fs}: paired ratios of ops/s (per run and round, same cell; median over rounds, min-max)\n"]
    head = CELL_COLS + [f"{a}/{b}" for a, b in pairs]
    head += [f"{a} p50 / {b} p50" for a, b in pairs[:1]] + [f"{v} p50 ms" for v in variants]
    out.append("| " + " | ".join(head) + " |")
    out.append("|" + "---|" * len(head))
    for key in sorted(cells, key=cell_order):
        by = cells[key]
        cols = []
        for a, b in pairs:
            p = paired(by, a, b)
            cols.append(f"{p[0]:.3f} ({p[1]:.2f}-{p[2]:.2f}) n={p[3]}" if p else "")
        for a, b in pairs[:1]:
            p50 = paired(by, a, b, "p50_us")
            cols.append(f"{p50[0]:.3f}" if p50 else "")
        for v in variants:
            rows = list(by.get(v, {}).values())
            cols.append(fmt_us(med(rows, "p50_us")) if rows else "")
        out.append("| " + " | ".join(cell_cols(key) + cols) + " |")
    return "\n".join(out)


def table(fs, cells, variants, baseline):
    out = [f"### {fs}: ops/s (median over rounds, min-max) and latency ms "
           "(median over rounds of per-round p50/p99/p999)\n"]
    head = CELL_COLS + ["var", "rounds", "ops/s med (min-max)", "p50 ms", "p99 ms", "p999 ms",
                        f"vs {baseline} ops/s"]
    out.append("| " + " | ".join(head) + " |")
    out.append("|" + "---|" * len(head))
    for key in sorted(cells, key=cell_order):
        by = cells[key]
        base = med(by[baseline].values(), "ops_per_s") if by.get(baseline) else None
        for v in variants:
            rows = list(by.get(v, {}).values())
            if not rows:
                continue
            tp = [float(r["ops_per_s"]) for r in rows]
            m = st.median(tp)
            rb = "1" if v == baseline else (f"{m / base:.3f}" if base else "")
            cols = [v, str(len(rows)), f"{m:.0f} ({min(tp):.0f}-{max(tp):.0f})", fmt_us(med(rows, "p50_us")),
                    fmt_us(med(rows, "p99_us")), fmt_us(med(rows, "p999_us")), rb]
            out.append("| " + " | ".join(cell_cols(key) + cols) + " |")
    return "\n".join(out)


def rows_of(cells):
    for by in cells.values():
        for rounds in by.values():
            yield from rounds.items()


def loads(cells):
    """(run, round) -> (min, max) of the 1-minute load average at run start."""
    by_round = defaultdict(list)
    for rk, r in rows_of(cells):
        try:
            by_round[rk].append(float(r["load1_start"]))
        except ValueError:
            pass
    return {k: (min(v), max(v)) for k, v in sorted(by_round.items()) if v}


def normalized(cells, labels):
    """Each run divided by its cell+variant median over rounds; median (p10-p90) per round."""
    norm = defaultdict(list)
    for by in cells.values():
        for rounds in by.values():
            m = st.median(float(r["ops_per_s"]) for r in rounds.values())
            for rk, r in rounds.items():
                norm[rk].append(float(r["ops_per_s"]) / m)
    return ", ".join(
        f"{labels[rk]}: {st.median(v):.3f} ({sorted(v)[len(v) // 10]:.2f}-{sorted(v)[len(v) * 9 // 10]:.2f})"
        for rk, v in sorted(norm.items())
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline")
    ap.add_argument("--pairs", help="comma-separated A/B pairs")
    ap.add_argument("mode", choices=["paired", "full", "both"])
    ap.add_argument("csv", nargs="+")
    args = ap.parse_args()
    for path in args.csv:
        for fs, (cells, variants) in load(path).items():
            baseline = args.baseline or variants[0]
            if baseline not in variants:
                raise SystemExit(f"{path}: baseline {baseline} not among variants {variants}")
            if args.pairs:
                pairs = [tuple(p.split("/")) for p in args.pairs.split(",")]
            else:
                pairs = [(v, baseline) for v in variants if v != baseline]
            if args.mode in ("paired", "both"):
                print(paired_table(fs, cells, variants, pairs))
                print()
            if args.mode in ("full", "both"):
                labels = round_labels(cells)
                print(table(fs, cells, variants, baseline))
                print()
                print(f"{fs}: 1-min load average at run start, per round (min-max over the round's runs):",
                      ", ".join(f"{labels[k]}: {lo:.1f}-{hi:.1f}" for k, (lo, hi) in loads(cells).items()))
                print(f"{fs}: per-round normalized throughput median (p10-p90):", normalized(cells, labels))
                print()


if __name__ == "__main__":
    main()
