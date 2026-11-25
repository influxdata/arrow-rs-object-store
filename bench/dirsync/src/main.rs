// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Micro-benchmark driver for `LocalFileSystem` directory-fsync designs.
//!
//! `conc` tokio tasks each run a sequential loop of one operation through the public
//! `ObjectStore` API for `secs` seconds (after a discarded warm-up) or until `max-ops` in total.
//! One run = one CSV row. See `README.md` for the matrix driver and how to read results.
//!
//! `--chunk-size` builds every payload from chunks of that size (as `WriteMultipart::write` and
//! `BufWriter` do through `PutPayloadMut`, 8 KiB blocks); the default is one chunk. The payload is
//! built once before the warm-up and shared by the writers (`PutPayload::clone` is an `Arc` clone).
//!
//! Operations (`--op`):
//! - `put`: `put` (Overwrite) of `size` bytes.
//! - `create`: `put_opts` with `PutMode::Create`, unique names.
//! - `delete`: `delete` of pre-populated objects; every object sits alone in its leaf directory,
//!   so with automatic cleanup each delete also removes that directory.
//! - `copy` / `rename`: into a different pre-created directory of the same depth; `copy` rotates
//!   over `copy-sources` sources per writer (a hard link each; see README on ext4's link limit),
//!   `rename` consumes pre-populated sources.
//! - `multipart`: `put_multipart`, `parts` x `put_part(size)`, `complete`.
//!
//! Layouts (`--state`): `steady` (one directory chain per writer, pre-created and fsynced),
//! `shared` (one chain for all writers), `cold` (every put makes a fresh first-level directory, so
//! each put creates `depth` directories), `spread` (`{prefix}/n1/pt_wal/{file}` with the prefix one
//! of 4096 hashed values chosen per op; all 12288 directories pre-created, then `sync`).

use bytes::Bytes;
use hdrhistogram::Histogram;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
#[cfg(feature = "ext-trait")]
use object_store::ObjectStoreExt;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};

const USAGE: &str = "\
usage: dirsync-bench [--flag value]...
  --root DIR        store root; REMOVED before and after the run (default: $TMPDIR/dirsync-bench);
                    no `..`; after resolving symlinks it must be >= 2 levels below /
  --op OP           put | create | delete | copy | rename | multipart (default put)
  --state S         steady | shared | cold | spread (default steady)
  --depth N         directories below the root (default 3); with --conc > 1 at least 1 for
                    steady/cold put/create/multipart and 2 for delete/copy/rename
  --size BYTES      object or part size (default 4096)
  --chunk-size BYTES
                    build every payload (object or part) from chunks of this size, the last one
                    shorter if --size is not a multiple; 0 = one chunk (default 0)
  --parts N         parts per multipart upload (default 2)
  --conc N          concurrent writers (default 1)
  --secs S          timed phase length (default 3)
  --warmup S        discarded warm-up length (default 0.7)
  --max-ops N       stop after exactly N ops in total (quotient + remainder split over writers);
                    0 = time-bounded (default 0; delete/rename need it)
  --fsync on|off    LocalFileSystem::with_fsync (default on)
  --cleanup on|off  LocalFileSystem::with_automatic_cleanup (default off)
  --copy-sources K  copy rotates over K source files per writer (default 16)
  --fs NAME         label written to the CSV (default ?)
  --variant NAME    label written to the CSV (default ?)
  --round N         label written to the CSV (default 0)
  --run ID          label written to the CSV (default 0); run-matrix.sh passes one per invocation
  --csv FILE        append one row (header if new) to FILE (default stdout)
";

struct Args {
    root: String,
    op: String,
    fs: String,
    variant: String,
    depth: usize,
    state: String,
    size: usize,
    chunk_size: usize,
    parts: usize,
    conc: usize,
    secs: f64,
    warmup: f64,
    max_ops: u64,
    fsync: bool,
    cleanup: bool,
    copy_sources: u64,
    round: u32,
    run: String,
    csv: String,
}

const KNOWN: &[&str] = &[
    "root",
    "op",
    "fs",
    "variant",
    "depth",
    "state",
    "size",
    "chunk-size",
    "parts",
    "conc",
    "secs",
    "warmup",
    "max-ops",
    "fsync",
    "cleanup",
    "copy-sources",
    "round",
    "run",
    "csv",
];

fn parse() -> Args {
    let mut m: HashMap<String, String> = HashMap::new();
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        if k == "-h" || k == "--help" {
            print!("{USAGE}");
            std::process::exit(0);
        }
        let k = k.trim_start_matches("--").to_string();
        if !KNOWN.contains(&k.as_str()) {
            eprintln!("unknown flag --{k}\n{USAGE}");
            std::process::exit(2);
        }
        let v = it.next().unwrap_or_else(|| {
            eprintln!("missing value for --{k}");
            std::process::exit(2);
        });
        m.insert(k, v);
    }
    let g = |k: &str, d: &str| m.get(k).cloned().unwrap_or_else(|| d.to_string());
    let default_root = std::env::temp_dir().join("dirsync-bench");
    let args = Args {
        root: g("root", &default_root.to_string_lossy()),
        op: g("op", "put"),
        fs: g("fs", "?"),
        variant: g("variant", "?"),
        depth: g("depth", "3").parse().expect("--depth"),
        state: g("state", "steady"),
        size: g("size", "4096").parse().expect("--size"),
        chunk_size: g("chunk-size", "0").parse().expect("--chunk-size"),
        parts: g("parts", "2").parse().expect("--parts"),
        conc: g("conc", "1").parse().expect("--conc"),
        secs: g("secs", "3").parse().expect("--secs"),
        warmup: g("warmup", "0.7").parse().expect("--warmup"),
        max_ops: g("max-ops", "0").parse().expect("--max-ops"),
        fsync: g("fsync", "on") == "on",
        cleanup: g("cleanup", "off") == "on",
        copy_sources: g("copy-sources", "16").parse().expect("--copy-sources"),
        round: g("round", "0").parse().expect("--round"),
        run: g("run", "0"),
        csv: g("csv", "/dev/stdout"),
    };
    assert!(args.conc >= 1, "--conc must be >= 1");
    assert!(args.copy_sources >= 1, "--copy-sources must be >= 1");
    assert!(
        ["put", "create", "delete", "copy", "rename", "multipart"].contains(&args.op.as_str()),
        "unknown --op {}",
        args.op
    );
    assert!(
        ["steady", "shared", "cold", "spread"].contains(&args.state.as_str()),
        "unknown --state {}",
        args.state
    );
    // Writers are told apart by the first directory of their chain (steady/cold) or by the file
    // name (shared/spread); delete and copy/rename build that chain one level short of --depth.
    // Shallower chains make concurrent writers target the same names.
    if args.conc > 1 {
        let min_depth = match args.op.as_str() {
            "delete" | "copy" | "rename" => 2,
            _ if ["steady", "cold"].contains(&args.state.as_str()) => 1,
            _ => 0,
        };
        assert!(
            args.depth >= min_depth,
            "--op {} --state {} with --conc > 1 needs --depth >= {min_depth} (writers would collide)",
            args.op,
            args.state
        );
    }
    args
}

fn load1() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.split_whitespace().next().unwrap_or("?").to_string())
        .unwrap_or_else(|_| "?".into())
}

const PREFIXES: u64 = 4096;

fn spread_prefix(w: usize, i: u64) -> u64 {
    let h = (w as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(i)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    (h >> 52) % PREFIXES
}

/// The directory chain (without the file) for writer `w`, op `i`.
fn dir_parts(state: &str, depth: usize, w: usize, i: u64) -> Vec<String> {
    let mut parts = Vec::with_capacity(depth);
    match state {
        "spread" => {
            parts.push(format!("{:03x}", spread_prefix(w, i)));
            parts.push("n1".to_string());
            parts.push("pt_wal".to_string());
        }
        _ if depth >= 1 => {
            parts.push(match state {
                "cold" => format!("w{w}_{i}"),
                "shared" => "s".to_string(),
                _ => format!("w{w}"),
            });
            for l in 2..=depth {
                parts.push(format!("l{l}"));
            }
        }
        _ => {}
    }
    parts
}

fn join(mut parts: Vec<String>, file: String) -> Path {
    parts.push(file);
    Path::from_iter(parts.iter().map(|s| s.as_str()))
}

/// Object written by `put`/`create`/`multipart` for writer `w`, op `i`.
fn object_path(state: &str, depth: usize, w: usize, i: u64) -> Path {
    let file = match state {
        "shared" | "spread" => format!("w{w}_{i}.pt"),
        _ => format!("{i}.pt"),
    };
    join(dir_parts(state, depth, w, i), file)
}

/// Object deleted by `delete`: alone in its own leaf directory `d{i}` at `depth`.
fn delete_path(depth: usize, w: usize, i: u64) -> Path {
    let mut parts = dir_parts("steady", depth.saturating_sub(1), w, i);
    parts.push(format!("d{i}"));
    join(parts, format!("{i}.pt"))
}

/// Source and destination for `copy` (source `i % copy_sources` of writer `w`) and `rename`
/// (one source per op).
fn copy_paths(op: &str, depth: usize, w: usize, i: u64, copy_sources: u64) -> (Path, Path) {
    let src_dir = dir_parts("steady", depth, w, i);
    let mut dst_dir = dir_parts("steady", depth.saturating_sub(1), w, i);
    dst_dir.push("dst".to_string());
    let src = match op {
        "copy" => format!("src{}.pt", i % copy_sources),
        _ => format!("src{i}.pt"),
    };
    (join(src_dir, src), join(dst_dir, format!("{i}.pt")))
}

fn fsync_dir(p: &std::path::Path) -> std::io::Result<()> {
    File::open(p)?.sync_all()
}

/// `create_dir_all` for `dir` and fsync every directory from it up to `root`.
fn create_chain_synced(root: &std::path::Path, dir: &std::path::Path) {
    std::fs::create_dir_all(dir).unwrap();
    let mut d = dir.to_path_buf();
    loop {
        fsync_dir(&d).unwrap();
        if d == root || !d.pop() {
            break;
        }
    }
}

fn sync_all_fs() {
    let status = std::process::Command::new("sync").status().expect("sync");
    assert!(status.success());
}

/// `size` bytes of `0xA5` as one chunk (`chunk_size == 0`) or chunks of `chunk_size` bytes, the
/// last one `size % chunk_size` bytes when that is not 0.
fn build_payload(size: usize, chunk_size: usize) -> PutPayload {
    if chunk_size == 0 {
        return PutPayload::from(vec![0xA5u8; size]);
    }
    let full = Bytes::from(vec![0xA5u8; chunk_size]);
    let rest = Bytes::from(vec![0xA5u8; size % chunk_size]);
    (0..size / chunk_size)
        .map(|_| full.clone())
        .chain((!rest.is_empty()).then_some(rest))
        .collect()
}

struct Phase {
    hist: Histogram<u64>,
    ops: u64,
    wall: Duration,
}

async fn one_op(store: &LocalFileSystem, a: &Args, payload: &PutPayload, w: usize, i: u64) {
    match a.op.as_str() {
        "put" => {
            let path = object_path(&a.state, a.depth, w, i);
            store.put(&path, payload.clone()).await.expect("put failed");
        }
        "create" => {
            let path = object_path(&a.state, a.depth, w, i);
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            store
                .put_opts(&path, payload.clone(), opts)
                .await
                .expect("create failed");
        }
        "delete" => {
            let path = delete_path(a.depth, w, i);
            store.delete(&path).await.expect("delete failed");
        }
        "copy" => {
            let (from, to) = copy_paths("copy", a.depth, w, i, a.copy_sources);
            store.copy(&from, &to).await.expect("copy failed");
        }
        "rename" => {
            let (from, to) = copy_paths("rename", a.depth, w, i, a.copy_sources);
            store.rename(&from, &to).await.expect("rename failed");
        }
        "multipart" => {
            let path = object_path(&a.state, a.depth, w, i);
            let mut upload = store
                .put_multipart(&path)
                .await
                .expect("put_multipart failed");
            for _ in 0..a.parts {
                upload
                    .put_part(payload.clone())
                    .await
                    .expect("put_part failed");
            }
            upload.complete().await.expect("complete failed");
        }
        other => panic!("unknown op {other}"),
    }
}

async fn run_ops(
    store: Arc<LocalFileSystem>,
    a: Arc<Args>,
    payload: PutPayload,
    counters: &mut [u64],
    secs: f64,
    max_ops: u64,
) -> Phase {
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(secs);
    let mut handles = Vec::with_capacity(a.conc);
    for (w, &start_i) in counters.iter().enumerate() {
        let store = Arc::clone(&store);
        let payload = payload.clone();
        let a = Arc::clone(&a);
        let cap = quota(max_ops, a.conc, w);
        handles.push(tokio::spawn(async move {
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            let mut i = start_i;
            while Instant::now() < deadline && i - start_i < cap {
                let t = Instant::now();
                one_op(&store, &a, &payload, w, i).await;
                hist.record(t.elapsed().as_micros().clamp(1, 60_000_000) as u64)
                    .unwrap();
                i += 1;
            }
            (w, i, hist)
        }));
    }
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let mut ops = 0;
    for h in handles {
        let (w, i, hw) = h.await.unwrap();
        ops += i - counters[w];
        counters[w] = i;
        hist.add(&hw).unwrap();
    }
    Phase {
        hist,
        ops,
        wall: start.elapsed(),
    }
}

/// Op budget of writer `w` out of `conc`: the quotient of `max_ops`, plus one for the first
/// `max_ops % conc` writers, so the budgets sum to exactly `max_ops` (a budget may be 0).
/// `max_ops == 0` means unbounded.
fn quota(max_ops: u64, conc: usize, w: usize) -> u64 {
    if max_ops == 0 {
        return u64::MAX;
    }
    let conc = conc as u64;
    max_ops / conc + u64::from((w as u64) < max_ops % conc)
}

fn warm_cap(a: &Args) -> u64 {
    if a.max_ops == 0 {
        0
    } else {
        (a.max_ops / 4).max(a.conc as u64)
    }
}

/// Creates directories (and, for consuming ops, objects) before the warm-up.
fn prepare(store: &LocalFileSystem, a: &Args, root: &std::path::Path) {
    let fs_path = |p: &Path| store.path_to_filesystem(p).unwrap();
    match a.op.as_str() {
        "delete" | "rename" => {
            // Enough pre-populated objects for the warm-up and the timed phase, allocated per
            // writer exactly as `run_ops` consumes them.
            assert!(a.max_ops > 0, "{} needs --max-ops", a.op);
            for w in 0..a.conc {
                let n = quota(warm_cap(a), a.conc, w) + quota(a.max_ops, a.conc, w);
                for i in 0..n {
                    let p = match a.op.as_str() {
                        "delete" => delete_path(a.depth, w, i),
                        _ => copy_paths("rename", a.depth, w, i, a.copy_sources).0,
                    };
                    let fsp = fs_path(&p);
                    std::fs::create_dir_all(fsp.parent().unwrap()).unwrap();
                    std::fs::write(&fsp, b"x").unwrap();
                }
                if a.op == "rename" {
                    let (_, to) = copy_paths("rename", a.depth, w, 0, a.copy_sources);
                    std::fs::create_dir_all(fs_path(&to).parent().unwrap()).unwrap();
                }
            }
            sync_all_fs();
        }
        "copy" => {
            for w in 0..a.conc {
                for s in 0..a.copy_sources {
                    let (from, to) = copy_paths("copy", a.depth, w, s, a.copy_sources);
                    let from = fs_path(&from);
                    std::fs::create_dir_all(from.parent().unwrap()).unwrap();
                    std::fs::write(&from, vec![0xA5u8; a.size]).unwrap();
                    std::fs::create_dir_all(fs_path(&to).parent().unwrap()).unwrap();
                }
            }
            sync_all_fs();
        }
        _ => match a.state.as_str() {
            "spread" => {
                for prefix in 0..PREFIXES {
                    let dir = root.join(format!("{prefix:03x}")).join("n1").join("pt_wal");
                    std::fs::create_dir_all(dir).unwrap();
                }
                sync_all_fs();
            }
            "steady" | "shared" => {
                for w in 0..a.conc {
                    let p = object_path(&a.state, a.depth, w, 0);
                    let fsp = fs_path(&p);
                    create_chain_synced(root, fsp.parent().unwrap());
                }
            }
            _ => {}
        },
    }
}

fn write_row(a: &Args, p: &Phase, load_start: &str, load_end: &str) {
    use std::io::Write;
    let path = std::path::Path::new(&a.csv);
    let new = !path.exists()
        || std::fs::metadata(path)
            .map(|m| m.len() == 0)
            .unwrap_or(true);
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    if new && a.csv != "/dev/stdout" {
        writeln!(f, "fs,variant,op,depth,state,size,conc,round,ops,wall_s,ops_per_s,p50_us,p90_us,p99_us,p999_us,max_us,mean_us,load1_start,load1_end,fsync,cleanup,max_ops,run,chunk_size").unwrap();
    }
    let h = &p.hist;
    writeln!(
        f,
        "{},{},{},{},{},{},{},{},{},{:.3},{:.1},{},{},{},{},{},{:.1},{},{},{},{},{},{},{}",
        a.fs,
        a.variant,
        a.op,
        a.depth,
        a.state,
        a.size,
        a.conc,
        a.round,
        p.ops,
        p.wall.as_secs_f64(),
        p.ops as f64 / p.wall.as_secs_f64(),
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.9),
        h.value_at_quantile(0.99),
        h.value_at_quantile(0.999),
        h.max(),
        h.mean(),
        load_start,
        load_end,
        if a.fsync { "on" } else { "off" },
        if a.cleanup { "on" } else { "off" },
        a.max_ops,
        a.run,
        a.chunk_size,
    )
    .unwrap();
}

fn build_store(a: &Args, root: &std::path::Path) -> LocalFileSystem {
    let mut store = LocalFileSystem::new_with_prefix(root).unwrap();
    #[cfg(feature = "with-fsync")]
    if a.fsync {
        store = store.with_fsync(true);
    }
    #[cfg(not(feature = "with-fsync"))]
    assert!(
        !a.fsync,
        "built without the `with-fsync` feature (rev has no LocalFileSystem::with_fsync); pass --fsync off"
    );
    if a.cleanup {
        store = store.with_automatic_cleanup(true);
    }
    store
}

/// `--root` made absolute, its longest existing ancestor canonicalized (symlinks resolved) and
/// the not-yet-existing remainder appended; `..` components are rejected. All file system
/// operations use the returned path, so the depth check below sees what is removed.
fn resolve_root(root: &str) -> std::path::PathBuf {
    use std::path::Component;
    let root = std::path::absolute(root).expect("--root");
    assert!(
        !root.components().any(|c| c == Component::ParentDir),
        "refusing --root {}: `..` components are not allowed",
        root.display()
    );
    let mut existing = root.as_path();
    let mut rest = Vec::new();
    while !existing.exists() {
        rest.push(existing.file_name().expect("absolute path"));
        existing = existing.parent().expect("absolute path");
    }
    let mut resolved = std::fs::canonicalize(existing).expect("canonicalize --root ancestor");
    resolved.extend(rest.iter().rev());
    resolved
}

fn main() {
    let a = Arc::new(parse());
    let root = resolve_root(&a.root);
    assert!(
        root.components().count() >= 3,
        "refusing to remove --root {} (resolves to {}): too close to the file system root",
        a.root,
        root.display()
    );
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    fsync_dir(&root).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let store = Arc::new(build_store(&a, &root));
        prepare(&store, &a, &root);
        let payload = build_payload(a.size, a.chunk_size);
        let mut counters = vec![0u64; a.conc];
        let _warm = run_ops(
            Arc::clone(&store),
            Arc::clone(&a),
            payload.clone(),
            &mut counters,
            a.warmup,
            warm_cap(&a),
        )
        .await;
        let load_start = load1();
        let p = run_ops(
            Arc::clone(&store),
            Arc::clone(&a),
            payload,
            &mut counters,
            a.secs,
            a.max_ops,
        )
        .await;
        let load_end = load1();
        write_row(&a, &p, &load_start, &load_end);
    });
    drop(rt);
    let _ = std::fs::remove_dir_all(&root);
}
