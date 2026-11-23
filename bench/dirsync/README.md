<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Directory-fsync micro-benchmark for `LocalFileSystem`

Compares object_store git revs on the write paths of `LocalFileSystem` with `with_fsync(true)`:
how many directory fsyncs and other syscalls each design issues per operation, and what that costs
in throughput and latency on a given file system. Motivating issue:
[apache/arrow-rs-object-store#888](https://github.com/apache/arrow-rs-object-store/issues/888)
(durable directory creation / fsync of parent directories).

This crate is not a workspace member; the repository's own `cargo build/test/clippy` ignore it.

## What it measures

One **run** = one binary (`src/main.rs`) built against one object_store rev, one operation, one
layout, `conc` tokio tasks each looping sequentially through the public `ObjectStore` API. Latency
is measured around each `await`. A run prepares a fresh store root, does a discarded warm-up
(0.7 s), then a timed phase (3 s, or until `--max-ops` in total), appends one CSV row, and removes
the root (untimed).

Operations (`--op`): `put` (overwrite), `create` (`PutMode::Create`, unique names), `delete` of
pre-populated objects each alone in its leaf directory (with `--cleanup on` every delete also
removes that directory), `copy` (hard link into a sibling directory, rotating over
`--copy-sources` sources per writer), `rename` of pre-populated sources into a sibling directory,
`multipart` (`put_multipart`, `--parts` x `put_part(--size)`, `complete`).

Layouts (`--state`): `steady` (one pre-created, fsynced chain per writer), `shared` (one chain for
all writers), `cold` (every op creates a fresh chain of `--depth` directories), `spread`
(`{prefix}/n1/pt_wal/{file}`, prefix one of 4096 hashed values per op; 12288 directories
pre-created and `sync`ed, a working set larger than a 4096-entry cache).

A **cell** is `op depth state size conc max-ops cleanup`; the default list in `run-matrix.sh` is
24 cells (put steady/shared/cold at 1/8/64 writers, depth-6 cold, spread, create, delete, copy,
rename, multipart). A **variant** is a rev built by `build-variants.sh`; a variant may also be run
with `--fsync off` as the no-durability floor without a separate build.

The optional **strace pass** runs each variant over a fixed op count under
`strace -c -f -e trace=fsync,fdatasync,openat,...` and records the syscall table next to the CSV
row, so counts per op can be compared between designs (counts, not timings).

## Requirements

- Linux (the driver reads `/proc/loadavg` and calls `sync`; the directory-handle designs are
  Linux-only).
- Rust toolchain of this repository (`rust-toolchain.toml`), git worktrees, bash >= 4, python3.
- For the strace pass: `strace` >= 6.6 (`-c -f` over a multi-threaded tokio runtime; the
  reference runs used 6.19). `run-matrix.sh` warns below 6.6.
- Two file systems to compare file systems (for example an xfs and an ext4 mount); a single
  directory on `$TMPDIR` is enough for a smoke test. The bench root is **removed** before and
  after every run; never point it at a directory with data.
- Disk: the multipart cell writes 800 x 10 MiB per run; the delete cells pre-populate 125k files.

## Quick start

```sh
cd bench/dirsync

# 1. Build one driver binary per rev (any rev resolvable in this repository; fetch first).
#    Worktrees, generated manifests and targets go under $WORK (default ./work).
./build-variants.sh base=influxdata/base-object-store-0132 \
                    dirfd=influxdata/local-dirfd-cache-0132

# 2. Run a subset: one round, two cells, the second variant also as the no-fsync floor.
./run-matrix.sh --fs tmp=/tmp/dirsync-bench --out ./out --rounds 1 \
                --only '^put 3 steady 4096 (8|64) ' base dirfd floor=dirfd:nofsync

# 3. Aggregate (paired ratios against the first variant; --baseline / --pairs to change).
./aggregate.py both out/ops-tmp.csv
```

Full matrix on two file systems with the strace pass, 5 rounds:

```sh
export BENCH_ROOT_XFS=/mnt/xfs/dirsync-bench BENCH_ROOT_EXT4=/mnt/ext4/dirsync-bench
./run-matrix.sh --out ./out --rounds 5 --strace base dirfd floor=dirfd:nofsync | tee out/run.log
./aggregate.py paired out/ops-xfs.csv out/ops-ext4.csv
```

Options worth knowing (`run-matrix.sh --help` lists all): `--rotation cycle` (every variant runs
first in turn, needed for order-sensitive cells), `--sync-between-runs[=S]` (`sync` + pause before
every run), `--cell "..."` (own cell list), `--strace-only`, `--extra "--copy-sources 64"`,
`--dry-run`.

`build-variants.sh` picks the driver's cargo features from the rev's source: `ext-trait` when
`ObjectStoreExt` exists (object_store >= 0.13), `with-fsync` when `LocalFileSystem::with_fsync`
exists (this fork's 0.12.5 and 0.13.2 branches, upstream >= 0.14). A rev without `with_fsync`
builds but accepts only `--fsync off`. Checked against 0.12.5 and 0.13.2 revs of this fork.

## Reading the results

- `ops-<fs>.csv`: one row per run with the cell (`op`, `depth`, `state`, `size`, `conc`,
  `max_ops`, `cleanup`), `fsync`, `ops`, `wall_s`, `ops_per_s`, latency percentiles in
  microseconds, the 1-minute load average at start and end, and `run`, the id of the
  `run-matrix.sh` invocation (its start timestamp). Repeated invocations append to the file;
  `aggregate.py` treats (`run`, `round`) as the round, rejects a duplicate (cell, variant, run,
  round) row with an error, and reads files written before the `cleanup`/`max_ops`/`run` columns
  existed with `?`/`?`/`0` in those fields.
- `aggregate.py paired`: per cell, for each pair A/B, the ratio of ops/s **within the same run and
  round**, then the median and min-max over rounds, and the per-variant median p50. This is the
  statistic to read: it cancels the drift between rounds that a shared host produces.
- `aggregate.py full`: medians over rounds with min-max of ops/s, p50/p99/p999, each variant's
  ratio to the baseline's median, plus two lines per file system: the load average range per
  round and the per-round normalized throughput (every run divided by its cell+variant median
  over rounds). A round whose normalized median is far from 1.0 was disturbed as a whole; a wide
  p10-p90 means individual cells were.
- `strace-<fs>.txt`: per traced run the CSV row and the `strace -c` table; divide the counts by
  `max-ops` for per-op syscalls. The driver's own preparation and teardown are included
  (`unlinkat` from `remove_dir_all`, `openat`/`mkdir` from pre-population, `sync`).

## Pitfalls

- **Only paired within-round ratios are meaningful.** Absolute numbers depend on other tenants of
  the disk; in past runs the same cells moved 2-3x between days while paired ratios held. Read the
  medians over rounds with the min-max as the warning; a single round's ratio can be an outlier
  when interference changed in the middle of a cell.
- **Position effect in the cell.** Variants run back to back on the same directory; the first
  variant of a cell writes into a file system with no dirty data from the previous variant's
  teardown. Multipart (two 5 MiB parts, 2 fsyncs per op) at 1 writer gave the first variant about
  2x, and a no-fsync variant leaves ~8 GB of dirty data behind for whoever follows it. The
  default rotation (forward in odd rounds, reversed in even rounds) does not give middle
  variants a first position; use `--rotation cycle` and `--sync-between-runs` for such cells,
  or treat them as not interpretable.
- **Host noise sampling.** The CSV carries the load average at start and end of each run; the
  `full` aggregate prints the per-round load range and normalized throughput. Processes outside
  the container are not visible from inside; a `sync`-heavy neighbour shows up only as longer
  fsyncs. For a long matrix, log `top` or `/proc/loadavg` every minute alongside (`run.log`
  timestamps let you align them).
- **Copy with fsync off and ext4's link limit.** `copy` is a hard link, and ext4 allows 65000
  links per inode (`EMLINK`). A no-fsync variant made ~270k copies/s; with one source per
  writer that exceeds the limit within one 3 s run at 8 writers. The driver therefore rotates
  over `--copy-sources` sources per writer (default 16); raise it (`--extra "--copy-sources 64"`)
  for no-fsync copy at 1 writer, or skip that cell. xfs has no comparable limit.
- **`delete` and `rename` need `--max-ops`** (pre-populated sources); delete at 100000 ops
  finishes in well under 3 s on fast disks, so its timed phase is shorter than the others.
- **bash `set -u`** in scripts of this kind: do not reference a variable in the same `local`
  statement that declares it (`local a=$1 b=$a`); declare one per line. The scripts here follow
  that and pass shellcheck.
- `ext4` vs `xfs` is only like-for-like when both sit on comparable devices and fill levels; read
  each file system's ratios on their own.

## Files

- `src/main.rs`, `Cargo.toml`: the driver (path dependency on `../..` so `cargo fmt`/`clippy`
  work here; `build-variants.sh` rewrites that path per variant).
- `build-variants.sh`: worktree + build per `NAME=REV` into `$WORK/{src,build,target}/NAME`.
- `run-matrix.sh`: matrix driver, strace pass; writes `$OUT/ops-<fs>.csv`, `strace-<fs>.{txt,csv}`.
- `aggregate.py`: `paired` / `full` / `both` markdown tables from the CSVs.
