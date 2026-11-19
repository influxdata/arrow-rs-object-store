#!/usr/bin/env bash
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

# Run the op matrix (and optionally a syscall-count pass) over variants built by
# build-variants.sh, one file system after another, rotating the variant order per round.
set -euo pipefail

usage() {
  cat <<'EOF'
usage: run-matrix.sh [options] VARIANT ...

  VARIANT                  NAME | LABEL=NAME | LABEL=NAME:nofsync
                           NAME is a variant built by build-variants.sh; LABEL is the CSV
                           label (default NAME); :nofsync runs it with --fsync off (the floor)
options
  --fs NAME=ROOT           file system label and a directory on it; repeatable. Default: one
                           entry per BENCH_ROOT_<NAME> environment variable (label lowercased),
                           else "tmp=$TMPDIR/dirsync-bench"
  --rounds N               rounds (default 5)
  --rotation reverse|cycle variant order per round: reverse = given order in odd rounds,
                           reversed in even rounds (default); cycle = rotate left by round-1 so
                           every variant runs first in turn
  --only REGEX             keep only cells whose line "op depth state size conc max-ops cleanup"
                           matches REGEX (grep -E); applies to the strace cells too
  --cell "op depth state size conc max-ops cleanup"
                           replace the default matrix cell list (not the strace cells); repeatable
  --secs S --warmup S      timed and warm-up phase per run (default 3 and 0.7)
  --sync-between-runs[=S]  `sync` then sleep S seconds before every run (default 3)
  --strace                 after the matrix, count syscalls per op over a fixed op count
  --strace-only            only the syscall-count pass
  --strace-ops N           ops per traced run (default 6400; multipart N/40)
  --extra "ARGS"           extra flags passed to every dirsync-bench invocation
  --out DIR                output directory (default: $OUT, else ./out next to this script)
  --work DIR               build directory of build-variants.sh (default: $WORK, else ./work)
  --dry-run                print the commands instead of running them

outputs (per file system label FS)
  $OUT/ops-FS.csv          one row per run; input for aggregate.py
  $OUT/strace-FS.txt       per traced run: the CSV row and the syscall table
  $OUT/strace-FS.csv       CSV rows of the traced runs (counts, not timings)
EOF
}

BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${OUT:-$BENCH_DIR/out}
WORK=${WORK:-$BENCH_DIR/work}
ROUNDS=5
ROTATION=reverse
ONLY=
SECS=3
WARMUP=0.7
SYNC_PAUSE=      # empty = no sync between runs
STRACE=0
MATRIX=1
STRACE_OPS=6400
EXTRA=
DRY=0
fs_specs=()
cells=()
variants=()

# op depth state size conc max-ops cleanup
default_cells=(
  "put 3 steady 4096 1 0 off" "put 3 steady 4096 8 0 off" "put 3 steady 4096 64 0 off"
  "put 3 shared 4096 1 0 off" "put 3 shared 4096 8 0 off" "put 3 shared 4096 64 0 off"
  "put 3 cold 4096 1 20000 off" "put 3 cold 4096 8 20000 off" "put 3 cold 4096 64 20000 off"
  "put 6 cold 4096 64 20000 off"
  "put 3 spread 4096 8 0 off" "put 3 spread 4096 64 0 off"
  "create 3 steady 4096 8 0 off" "create 3 steady 4096 64 0 off"
  "create 3 cold 4096 8 20000 off" "create 3 cold 4096 64 20000 off"
  "delete 3 steady 4096 8 100000 on" "delete 3 steady 4096 64 100000 on"
  "copy 3 steady 4096 8 0 off" "copy 3 steady 4096 64 0 off"
  "rename 3 steady 4096 8 20000 off" "rename 3 steady 4096 64 20000 off"
  "multipart 3 steady 5242880 1 800 off" "multipart 3 steady 5242880 8 800 off"
)
# Syscall-count cells: op depth state size conc cleanup; max-ops from --strace-ops.
strace_cells=(
  "put 3 steady 4096 64 off" "put 3 shared 4096 64 off" "put 3 cold 4096 64 off" "put 3 spread 4096 64 off"
  "create 3 steady 4096 64 off" "create 3 cold 4096 64 off"
  "delete 3 steady 4096 64 on" "copy 3 steady 4096 64 off" "rename 3 steady 4096 64 off"
  "multipart 3 steady 5242880 8 off"
)
TRACE="fsync,fdatasync,openat,open,mkdir,mkdirat,rename,renameat,link,linkat,unlink,unlinkat,rmdir,futex,fstat,newfstatat,statx"

while [ $# -gt 0 ]; do
  case $1 in
    --fs) fs_specs+=("$2"); shift 2 ;;
    --rounds) ROUNDS=$2; shift 2 ;;
    --rotation) ROTATION=$2; shift 2 ;;
    --only) ONLY=$2; shift 2 ;;
    --cell) cells+=("$2"); shift 2 ;;
    --secs) SECS=$2; shift 2 ;;
    --warmup) WARMUP=$2; shift 2 ;;
    --sync-between-runs) SYNC_PAUSE=3; shift ;;
    --sync-between-runs=*) SYNC_PAUSE=${1#*=}; shift ;;
    --strace) STRACE=1; shift ;;
    --strace-only) STRACE=1; MATRIX=0; shift ;;
    --strace-ops) STRACE_OPS=$2; shift 2 ;;
    --extra) EXTRA=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option $1" >&2; usage >&2; exit 2 ;;
    *) variants+=("$1"); shift ;;
  esac
done
if [ ${#variants[@]} -eq 0 ]; then usage >&2; exit 2; fi
case $ROTATION in reverse|cycle) ;; *) echo "--rotation must be reverse or cycle" >&2; exit 2 ;; esac
if [ ${#cells[@]} -eq 0 ]; then cells=("${default_cells[@]}"); fi

# File systems: --fs, else BENCH_ROOT_* from the environment, else one tmp entry.
if [ ${#fs_specs[@]} -eq 0 ]; then
  while IFS='=' read -r var val; do
    fs_specs+=("$(echo "${var#BENCH_ROOT_}" | tr '[:upper:]' '[:lower:]')=$val")
  done < <(env | grep -E '^BENCH_ROOT_[A-Za-z0-9_]+=' | sort)
fi
if [ ${#fs_specs[@]} -eq 0 ]; then fs_specs=("tmp=${TMPDIR:-/tmp}/dirsync-bench"); fi

# Variants: label -> binary, label -> fsync.
declare -A BIN FSYNC
labels=()
for spec in "${variants[@]}"; do
  fsync=on
  case $spec in *:nofsync) fsync=off; spec=${spec%:nofsync} ;; esac
  label=${spec%%=*}
  name=${spec#*=}
  bin=$WORK/target/$name/release/dirsync-bench
  if [ ! -x "$bin" ] && [ $DRY -eq 0 ]; then
    echo "no binary for variant '$name' at $bin (run build-variants.sh $name=REV)" >&2; exit 1
  fi
  BIN[$label]=$bin
  FSYNC[$label]=$fsync
  labels+=("$label")
done

filter() { # cell lines on stdin, filtered by --only
  if [ -n "$ONLY" ]; then grep -E -- "$ONLY" || true; else cat; fi
}
mapfile -t cells < <(printf '%s\n' "${cells[@]}" | filter)
mapfile -t strace_cells < <(printf '%s\n' "${strace_cells[@]}" | filter)
if [ $MATRIX -eq 1 ] && [ ${#cells[@]} -eq 0 ]; then echo "warning: --only '$ONLY' selects no matrix cell" >&2; fi
if [ $STRACE -eq 1 ] && [ ${#strace_cells[@]} -eq 0 ]; then
  echo "warning: --only '$ONLY' selects no strace cell (they run at 64 writers, multipart at 8)" >&2
fi

order_for_round() { # prints the variant labels for round $1
  local r=$1
  local n=${#labels[@]}
  local i
  if [ "$ROTATION" = reverse ] && [ $((r % 2)) -eq 0 ]; then
    for ((i = n - 1; i >= 0; i--)); do echo "${labels[$i]}"; done
  elif [ "$ROTATION" = cycle ]; then
    for ((i = 0; i < n; i++)); do echo "${labels[$(( (i + r - 1) % n ))]}"; done
  else
    printf '%s\n' "${labels[@]}"
  fi
}

run() { # run or print a command
  if [ $DRY -eq 1 ]; then printf '%q ' "$@"; echo; else "$@"; fi
}

before_run() {
  if [ -n "$SYNC_PAUSE" ]; then run sync; run sleep "$SYNC_PAUSE"; fi
}

load() { cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || echo "?"; }

# shellcheck disable=SC2086  # $EXTRA is split on purpose
matrix() { # fs root
  local fs=$1
  local root=$2
  local csv=$OUT/ops-$fs.csv
  local r cell label op depth state size conc max cleanup
  for ((r = 1; r <= ROUNDS; r++)); do
    local -a order
    mapfile -t order < <(order_for_round "$r")
    for cell in "${cells[@]}"; do
      read -r op depth state size conc max cleanup <<<"$cell"
      for label in "${order[@]}"; do
        before_run
        run "${BIN[$label]}" --root "$root/run" --fs "$fs" --variant "$label" --fsync "${FSYNC[$label]}" \
          --op "$op" --depth "$depth" --state "$state" --size "$size" --conc "$conc" --cleanup "$cleanup" \
          --secs "$SECS" --warmup "$WARMUP" --max-ops "$max" --round "$r" --csv "$csv" $EXTRA \
          || echo "FAILED $fs $label '$cell' round $r"
      done
    done
    echo "$fs round $r done $(date +%T) order [${order[*]}] load $(load)"
  done
}

# shellcheck disable=SC2086
strace_pass() { # fs root
  local fs=$1
  local root=$2
  local sum=$OUT/strace-$fs.txt
  local csv=$OUT/strace-$fs.csv
  local tmp=$OUT/strace-tmp-$fs.txt
  local cell label ops op depth state size conc cleanup
  if [ $DRY -eq 0 ]; then : > "$sum"; fi
  for cell in "${strace_cells[@]}"; do
    read -r op depth state size conc cleanup <<<"$cell"
    ops=$STRACE_OPS
    if [ "$op" = multipart ]; then ops=$(( STRACE_OPS / 40 )); fi
    for label in "${labels[@]}"; do
      before_run
      if [ $DRY -eq 0 ]; then
        echo "### $fs op $op depth $depth $state size $size conc $conc max-ops $ops variant $label fsync ${FSYNC[$label]}" >> "$sum"
      fi
      run strace -c -f -e "trace=$TRACE" -o "$tmp" "${BIN[$label]}" --root "$root/strace" --fs "$fs" \
        --variant "$label" --fsync "${FSYNC[$label]}" --op "$op" --depth "$depth" --state "$state" \
        --size "$size" --conc "$conc" --cleanup "$cleanup" \
        --secs 1000 --warmup 0 --max-ops "$ops" --round 0 --csv "$csv" $EXTRA \
        || echo "FAILED strace $fs $label '$cell'"
      if [ $DRY -eq 0 ]; then
        { tail -n 1 "$csv"; cat "$tmp"; echo; } >> "$sum"
      fi
    done
  done
  rm -f "$tmp"
}

if [ $STRACE -eq 1 ] && [ $DRY -eq 0 ]; then
  if ! command -v strace >/dev/null; then echo "strace not found" >&2; exit 1; fi
  sv=$(strace -V 2>/dev/null | head -n 1 | grep -oE '[0-9]+\.[0-9]+' | head -n 1)
  if [ -n "$sv" ] && [ "$(printf '%s\n6.6\n' "$sv" | sort -V | head -n 1)" != "6.6" ]; then
    echo "warning: strace $sv is older than 6.6; -c -f counts may be incomplete (see README)" >&2
  fi
fi

mkdir -p "$OUT"
echo "start $(date +%T) load $(load); variants [${labels[*]}]; ${#cells[@]} cells; $ROUNDS rounds; rotation $ROTATION"
for spec in "${fs_specs[@]}"; do
  fs=${spec%%=*}
  root=${spec#*=}
  if [ $DRY -eq 0 ]; then mkdir -p "$root"; fi
  if [ $MATRIX -eq 1 ]; then matrix "$fs" "$root"; fi
  if [ $STRACE -eq 1 ]; then strace_pass "$fs" "$root"; echo "$fs strace done $(date +%T)"; fi
done
echo "all done $(date +%T) load $(load)"
