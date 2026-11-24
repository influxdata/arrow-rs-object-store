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

# Build the dirsync-bench driver against one object_store git rev per variant.
#
# For every NAME=REV: a detached worktree of this repository at REV under $WORK/src/NAME, a
# generated manifest under $WORK/build/NAME (this directory's Cargo.toml with the object_store
# path rewritten), and a release build into $WORK/target/NAME. Cargo features of the driver are
# chosen from the rev's source: `ext-trait` if src/lib.rs defines `ObjectStoreExt` (>= 0.13),
# `with-fsync` if src/local.rs defines `LocalFileSystem::with_fsync`.
set -euo pipefail

usage() {
  cat <<'EOF'
usage: build-variants.sh [--work DIR] [--repo DIR] [--check] NAME=REV ...

  NAME=REV    variant label and any git rev of object_store resolvable in --repo
              (branch, tag, SHA, remote-tracking ref such as influxdata/main)
  --work DIR  working directory for worktrees, manifests and targets
              (default: $WORK, else ./work next to this script)
  --repo DIR  repository to take worktrees from (default: the one containing this script)
  --check     `cargo check` instead of a release build

Binaries land at $WORK/target/NAME/release/dirsync-bench; run-matrix.sh looks for them there.
EOF
}

BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK=${WORK:-$BENCH_DIR/work}
REPO=${REPO:-}
CARGO_CMD=(build --release)
specs=()

while [ $# -gt 0 ]; do
  case $1 in
    --work) WORK=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    --check) CARGO_CMD=(check); shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option $1" >&2; usage >&2; exit 2 ;;
    *) specs+=("$1"); shift ;;
  esac
done
if [ ${#specs[@]} -eq 0 ]; then usage >&2; exit 2; fi
if [ -z "$REPO" ]; then REPO=$(git -C "$BENCH_DIR" rev-parse --show-toplevel); fi
mkdir -p "$WORK/src" "$WORK/build" "$WORK/target"
WORK=$(cd "$WORK" && pwd)

for spec in "${specs[@]}"; do
  case $spec in
    *=*) ;;
    *) echo "expected NAME=REV, got '$spec'" >&2; exit 2 ;;
  esac
  name=${spec%%=*}
  rev=${spec#*=}
  if ! sha=$(git -C "$REPO" rev-parse --verify --quiet "$rev^{commit}"); then
    echo "cannot resolve '$rev' in $REPO (fetch it first, e.g. git -C $REPO fetch <remote> <branch>)" >&2
    exit 1
  fi
  src=$WORK/src/$name
  if [ -e "$src/.git" ]; then
    if [ "$(git -C "$src" rev-parse HEAD)" != "$sha" ]; then
      git -C "$src" checkout --quiet --detach "$sha"
    fi
  else
    git -C "$REPO" worktree add --quiet --detach "$src" "$sha"
  fi

  version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$src/Cargo.toml" | head -n 1)
  features=()
  if grep -q 'pub trait ObjectStoreExt' "$src/src/lib.rs"; then features+=(ext-trait); fi
  if grep -q 'pub fn with_fsync' "$src/src/local.rs"; then features+=(with-fsync); fi
  feature_list=$(IFS=,; echo "${features[*]:-}")

  build=$WORK/build/$name
  mkdir -p "$build"
  # Same manifest as this directory's, pointed at the variant's worktree and at the shared source.
  sed -e "s|path = \"\.\./\.\.\"|path = \"$src\"|" \
      -e "s|path = \"src/main\.rs\"|path = \"$BENCH_DIR/src/main.rs\"|" \
      "$BENCH_DIR/Cargo.toml" > "$build/Cargo.toml"
  printf 'name=%s\nrev=%s\nsha=%s\nversion=%s\nfeatures=%s\n' \
    "$name" "$rev" "$sha" "$version" "$feature_list" > "$build/variant.txt"

  echo "== $name: $rev @ ${sha:0:12} (object_store $version, features: ${feature_list:-none})"
  CARGO_TARGET_DIR=$WORK/target/$name \
    cargo "${CARGO_CMD[@]}" --quiet --manifest-path "$build/Cargo.toml" \
      --no-default-features ${feature_list:+--features "$feature_list"}
  if [ "${CARGO_CMD[0]}" = build ]; then
    echo "   $WORK/target/$name/release/dirsync-bench"
  fi
done
