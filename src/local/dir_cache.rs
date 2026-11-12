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

//! Open directory handles for durable writes, cached per store.
//!
//! Every directory on the path from the store root to a written entry is held open, and each
//! handle records the changes made to the directory's entries through it and the changes an
//! fsync has covered. A durable write changes entries through the handles (`openat`, `mkdirat`,
//! `renameat`, `linkat`, `unlinkat`) and then fsyncs every handle on its chain that is not yet
//! covered, whoever made the change. Nothing is shared between stores or processes.

use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use nix::errno::Errno;
use nix::fcntl::{open, openat, renameat, AtFlags, OFlag, AT_FDCWD};
use nix::sys::resource::{getrlimit, Resource};
use nix::sys::stat::{fstat, mkdirat, stat, Mode};
use nix::unistd::{linkat, unlinkat, UnlinkatFlags};
use parking_lot::Mutex;

/// Handles kept open at most, before the clamp to a quarter of the soft `RLIMIT_NOFILE`. Clean
/// handles beyond the capacity are evicted; dirty ones never are, so the cache can exceed it.
const CAPACITY: usize = 4096;
const MIN_CAPACITY: usize = 16;

const DIR_FLAGS: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_CLOEXEC);

/// Why an operation through the cache failed.
#[derive(Debug)]
pub(super) enum DirError {
    /// A cached handle refers to a directory that has been removed or replaced; `dir` is its path
    /// relative to the root. Evict it and retry the operation from the root.
    Stale { dir: PathBuf },
    /// A directory on the chain could not be opened.
    Open { dir: PathBuf, source: io::Error },
    /// A directory on the chain could not be created.
    Create { dir: PathBuf, source: io::Error },
    /// A directory on the chain could not be fsynced.
    Sync { dir: PathBuf, source: io::Error },
}

pub(super) struct DirCache {
    root: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    capacity: usize,
    /// Keyed by the path relative to the root; the root itself is the empty path.
    dirs: HashMap<PathBuf, Arc<DirHandle>>,
    /// Eviction order (second chance): the root is never in it.
    clock: VecDeque<PathBuf>,
}

impl std::fmt::Debug for DirCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirCache")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

pub(super) struct DirHandle {
    /// Relative to the root.
    rel: PathBuf,
    /// Absolute, for error reports and staleness checks.
    path: PathBuf,
    fd: File,
    used: AtomicBool,
    state: Mutex<DirState>,
    #[cfg(test)]
    pub(super) fsyncs: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) fail_syncs: AtomicBool,
}

#[derive(Default)]
struct DirState {
    /// Changes begun through this handle and not finished.
    in_flight: u32,
    /// Changes finished through this handle.
    changes: u64,
    /// The `changes` value a successful fsync started at.
    synced: u64,
}

impl DirHandle {
    fn new(rel: PathBuf, path: PathBuf, fd: OwnedFd) -> Self {
        Self {
            rel,
            path,
            fd: File::from(fd),
            used: AtomicBool::new(true),
            state: Mutex::default(),
            #[cfg(test)]
            fsyncs: Default::default(),
            #[cfg(test)]
            fail_syncs: Default::default(),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn rel(&self) -> &Path {
        &self.rel
    }

    fn is_root(&self) -> bool {
        self.rel.as_os_str().is_empty()
    }

    /// Runs `op`, which changes this directory's entries through its descriptor, recording the
    /// change from before it runs until an fsync that starts after it finishes succeeds. A
    /// panic counts as a change.
    pub(super) fn change<T>(&self, op: impl FnOnce(&File) -> nix::Result<T>) -> nix::Result<T> {
        self.state.lock().in_flight += 1;
        let mut guard = Changing {
            handle: self,
            changed: true,
        };
        let result = op(&self.fd);
        guard.changed = result.is_ok();
        result
    }

    /// Whether an fsync is owed: a change finished since the last successful fsync started, or
    /// one is still running.
    pub(super) fn is_dirty(&self) -> bool {
        let state = self.state.lock();
        state.in_flight > 0 || state.synced < state.changes
    }

    /// Fsyncs the directory if it is dirty. A failure leaves it dirty.
    pub(super) fn sync(&self) -> io::Result<()> {
        let covered = {
            let state = self.state.lock();
            if state.in_flight == 0 && state.synced >= state.changes {
                return Ok(());
            }
            state.changes
        };
        #[cfg(test)]
        {
            self.fsyncs.fetch_add(1, Ordering::Relaxed);
            if self.fail_syncs.load(Ordering::Relaxed) {
                return Err(io::Error::other("fsync failed"));
            }
        }
        self.fd.sync_all()?;
        let mut state = self.state.lock();
        state.synced = state.synced.max(covered);
        Ok(())
    }

    /// `ENOENT` from an entry operation through this handle means the directory is gone (every
    /// entry operation in a removed directory reports it) or the entry is. The handle is stale
    /// when its path no longer names its inode.
    pub(super) fn stale(&self, e: Errno) -> Option<DirError> {
        (e == Errno::ENOENT && !self.is_root() && !self.is_current()).then(|| DirError::Stale {
            dir: self.rel.clone(),
        })
    }

    fn is_current(&self) -> bool {
        match (fstat(&self.fd), stat(&self.path)) {
            (Ok(open), Ok(named)) => open.st_dev == named.st_dev && open.st_ino == named.st_ino,
            _ => false,
        }
    }
}

struct Changing<'a> {
    handle: &'a DirHandle,
    changed: bool,
}

impl Drop for Changing<'_> {
    fn drop(&mut self) {
        let mut state = self.handle.state.lock();
        state.in_flight -= 1;
        if self.changed {
            state.changes += 1;
        }
    }
}

impl DirCache {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            inner: Mutex::new(Inner {
                capacity: capacity(),
                dirs: HashMap::new(),
                clock: VecDeque::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn set_capacity(&self, capacity: usize) {
        self.inner.lock().capacity = capacity;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.inner.lock().dirs.len()
    }

    /// Splits an absolute path under the root into its parent directory (relative to the root)
    /// and its file name. `None` when the path is not under the root or has no file name.
    pub(super) fn split<'a>(&self, path: &'a Path) -> Option<(&'a Path, &'a OsStr)> {
        let rel = path.strip_prefix(&self.root).ok()?;
        let name = rel.file_name()?;
        Some((rel.parent()?, name))
    }

    /// The handles from the root down to `rel`, opening (and with `create`, making) the
    /// directories not yet cached.
    pub(super) fn resolve(
        &self,
        rel: &Path,
        create: bool,
    ) -> Result<Vec<Arc<DirHandle>>, DirError> {
        let mut chain = vec![self.root_handle()?];
        let mut key = PathBuf::new();
        for component in rel.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            key.push(name);
            let parent = chain.last().expect("the root is always present");
            match self.child(parent, &key, name, create) {
                Ok(handle) => chain.push(handle),
                Err(e) => {
                    // Directories this call created would otherwise keep their parents dirty,
                    // and dirty handles are never evicted.
                    if create {
                        let _ = sync_chain(&chain);
                    }
                    return Err(e);
                }
            }
        }
        Ok(chain)
    }

    /// The cached handle for `rel`, if any.
    #[cfg(test)]
    pub(super) fn cached(&self, rel: &Path) -> Option<Arc<DirHandle>> {
        self.inner.lock().dirs.get(rel).cloned()
    }

    /// Forgets `rel` and every directory under it.
    pub(super) fn evict(&self, rel: &Path) {
        let mut inner = self.inner.lock();
        inner.dirs.retain(|key, _| !key.starts_with(rel));
    }

    fn root_handle(&self) -> Result<Arc<DirHandle>, DirError> {
        let key = PathBuf::new();
        if let Some(root) = self.inner.lock().dirs.get(&key) {
            return Ok(Arc::clone(root));
        }
        let fd = open(&self.root, DIR_FLAGS, Mode::empty()).map_err(|e| DirError::Open {
            dir: self.root.clone(),
            source: e.into(),
        })?;
        let handle = Arc::new(DirHandle::new(key.clone(), self.root.clone(), fd));
        let mut inner = self.inner.lock();
        Ok(Arc::clone(inner.dirs.entry(key).or_insert(handle)))
    }

    fn child(
        &self,
        parent: &Arc<DirHandle>,
        key: &Path,
        name: &OsStr,
        create: bool,
    ) -> Result<Arc<DirHandle>, DirError> {
        if let Some(handle) = self.inner.lock().dirs.get(key) {
            handle.used.store(true, Ordering::Relaxed);
            return Ok(Arc::clone(handle));
        }
        let path = parent.path.join(name);
        let open_error = |e: Errno| {
            parent.stale(e).unwrap_or_else(|| DirError::Open {
                dir: path.clone(),
                source: e.into(),
            })
        };
        let fd = match openat(&parent.fd, name, DIR_FLAGS, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::ENOENT) if create => {
                // The entry change is recorded on the parent before it becomes visible.
                parent
                    .change(
                        |fd| match mkdirat(fd, name, Mode::from_bits_truncate(0o777)) {
                            Err(Errno::EEXIST) => Ok(()),
                            other => other,
                        },
                    )
                    .map_err(|e| {
                        parent.stale(e).unwrap_or_else(|| DirError::Create {
                            dir: path.clone(),
                            source: e.into(),
                        })
                    })?;
                openat(&parent.fd, name, DIR_FLAGS, Mode::empty()).map_err(open_error)?
            }
            Err(e) => return Err(open_error(e)),
        };
        let handle = Arc::new(DirHandle::new(key.to_path_buf(), path, fd));
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.dirs.get(key) {
            return Ok(Arc::clone(existing));
        }
        inner.insert(key.to_path_buf(), Arc::clone(&handle));
        Ok(handle)
    }
}

impl Inner {
    fn insert(&mut self, key: PathBuf, handle: Arc<DirHandle>) {
        // Second chance: a handle used since it was last examined, or dirty, is kept. Two passes
        // clear every `used` flag, so after them only dirty handles remain and the cache grows
        // beyond its capacity rather than dropping a change record.
        let mut examined = 0;
        while self.dirs.len() >= self.capacity && examined < 2 * self.clock.len() {
            let Some(candidate) = self.clock.pop_front() else {
                break;
            };
            let Some(existing) = self.dirs.get(&candidate) else {
                continue;
            };
            examined += 1;
            if existing.used.swap(false, Ordering::Relaxed) || existing.is_dirty() {
                self.clock.push_back(candidate);
                continue;
            }
            self.dirs.remove(&candidate);
        }
        self.dirs.insert(key.clone(), handle);
        self.clock.push_back(key);
    }
}

/// [`CAPACITY`] clamped to a quarter of the soft open-file limit, so the cache leaves room for
/// the process's other descriptors.
fn capacity() -> usize {
    let quarter = getrlimit(Resource::RLIMIT_NOFILE)
        .map(|(soft, _)| soft / 4)
        .ok()
        .and_then(|q| usize::try_from(q).ok())
        .unwrap_or(CAPACITY);
    CAPACITY.min(quarter).max(MIN_CAPACITY)
}

/// Fsyncs every dirty directory on `chain`, deepest first.
pub(super) fn sync_chain(chain: &[Arc<DirHandle>]) -> Result<(), DirError> {
    for dir in chain.iter().rev() {
        dir.sync().map_err(|source| DirError::Sync {
            dir: dir.path.clone(),
            source,
        })?;
    }
    Ok(())
}

/// Creates the entry `name` in `dir` exclusively, for reading and writing.
pub(super) fn create_new(dir: &DirHandle, name: &OsStr) -> nix::Result<File> {
    let flags = OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_CLOEXEC;
    dir.change(|fd| openat(fd, name, flags, Mode::from_bits_truncate(0o666)))
        .map(File::from)
}

/// Renames `from` in `from_dir` to `to` in `to_dir`, recording the change on both.
pub(super) fn rename_between(
    from_dir: &DirHandle,
    from: &OsStr,
    to_dir: &DirHandle,
    to: &OsStr,
) -> nix::Result<()> {
    if std::ptr::eq(from_dir, to_dir) {
        return from_dir.change(|fd| renameat(fd, from, fd, to));
    }
    from_dir.change(|from_fd| to_dir.change(|to_fd| renameat(from_fd, from, to_fd, to)))
}

/// Hard-links `from` to `to` within `dir`.
pub(super) fn link_within(dir: &DirHandle, from: &OsStr, to: &OsStr) -> nix::Result<()> {
    dir.change(|fd| linkat(fd, from, fd, to, AtFlags::empty()))
}

/// Hard-links the file at the absolute path `from` to `name` in `dir`. Like
/// [`std::fs::hard_link`], a symlink `from` is linked itself, not followed.
pub(super) fn link_from_path(dir: &DirHandle, from: &Path, name: &OsStr) -> nix::Result<()> {
    dir.change(|fd| linkat(AT_FDCWD, from, fd, name, AtFlags::empty()))
}

/// Unlinks the file `name` in `dir`.
pub(super) fn unlink_in(dir: &DirHandle, name: &OsStr) -> nix::Result<()> {
    dir.change(|fd| unlinkat(fd, name, UnlinkatFlags::NoRemoveDir))
}

/// [`unlink_in`] without recording the change, for a removal nobody fsyncs: a recorded change
/// keeps the handle dirty, and dirty handles are never evicted.
pub(super) fn unlink_unsynced(dir: &DirHandle, name: &OsStr) -> nix::Result<()> {
    unlinkat(&dir.fd, name, UnlinkatFlags::NoRemoveDir)
}

/// Removes the empty directory `name` in `dir`, unrecorded like [`unlink_unsynced`]: no delete
/// fsyncs the directories it removes, and whoever recreates one records that.
pub(super) fn rmdir_in(dir: &DirHandle, name: &OsStr) -> nix::Result<()> {
    unlinkat(&dir.fd, name, UnlinkatFlags::RemoveDir)
}
