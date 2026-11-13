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
use std::sync::{Arc, Weak};

use nix::errno::Errno;
use nix::fcntl::{open, openat, renameat, AtFlags, OFlag, AT_FDCWD};
use nix::sys::resource::{getrlimit, Resource};
use nix::sys::stat::{fstat, mkdirat, stat, Mode};
use nix::unistd::{linkat, unlinkat, UnlinkatFlags};
use parking_lot::Mutex;

/// Handles kept open at most, before the clamp to a quarter of the soft `RLIMIT_NOFILE`. Clean
/// handles beyond the capacity are evicted; dirty ones and ones an operation holds never are, so
/// the cache can exceed it.
const CAPACITY: usize = 4096;
const MIN_CAPACITY: usize = 16;

const DIR_FLAGS: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_CLOEXEC);

/// A directory's identity: device and inode.
type Inode = (u64, u64);

/// Why an operation through the cache failed.
#[derive(Debug)]
pub(super) enum DirError {
    /// A cached handle refers to a directory that has been removed. Evict it and retry the
    /// operation from the root.
    Stale { dir: Arc<DirHandle> },
    /// A directory on the chain could not be opened.
    Open { dir: PathBuf, source: io::Error },
    /// A directory on the chain could not be created.
    Create { dir: PathBuf, source: io::Error },
    /// A directory on the chain could not be fsynced.
    Sync { dir: PathBuf, source: io::Error },
    /// The directory holding the root could not be fsynced.
    RootParentSync { dir: PathBuf, source: io::Error },
}

pub(super) struct DirCache {
    root: PathBuf,
    inner: Mutex<Inner>,
    /// Whether the directory holding the root was fsynced since the root was opened.
    root_parent_synced: AtomicBool,
    /// Makes every `ENOENT` look like a removed directory.
    #[cfg(test)]
    force_stale: AtomicBool,
}

struct Inner {
    capacity: usize,
    /// Keyed by the path relative to the root; the root itself is the empty path.
    dirs: HashMap<PathBuf, Arc<DirHandle>>,
    /// Eviction order (second chance); entries no longer cached are skipped. The root is never
    /// in it.
    clock: VecDeque<Weak<DirHandle>>,
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
    inode: Inode,
    /// The inode of the directory holding this one's entry when it was opened; `None` for the
    /// root.
    parent: Option<Inode>,
    used: AtomicBool,
    state: Mutex<DirState>,
    #[cfg(test)]
    pub(super) fsyncs: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub(super) fail_syncs: AtomicBool,
}

impl std::fmt::Debug for DirHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirHandle")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
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
    fn new(rel: PathBuf, path: PathBuf, fd: OwnedFd, parent: Option<Inode>) -> nix::Result<Self> {
        let fd = File::from(fd);
        let inode = inode_of(&fd)?;
        Ok(Self {
            rel,
            path,
            fd,
            inode,
            parent,
            used: AtomicBool::new(true),
            state: Mutex::default(),
            #[cfg(test)]
            fsyncs: Default::default(),
            #[cfg(test)]
            fail_syncs: Default::default(),
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn rel(&self) -> &Path {
        &self.rel
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

    /// A removed directory keeps its inode while this descriptor is open, with no links left.
    /// Only a successful `fstat` counts: some filesystems do not report links.
    fn is_removed(&self) -> bool {
        fstat(&self.fd).is_ok_and(|s| s.st_nlink == 0)
    }

    /// Whether `path` still names this directory. A directory moved elsewhere is not removed,
    /// so only this tells its handle from the one at its old path.
    fn is_current(&self) -> bool {
        stat(&self.path)
            .map(|named| (named.st_dev, named.st_ino) == self.inode)
            .unwrap_or(false)
    }
}

fn inode_of(fd: &File) -> nix::Result<Inode> {
    fstat(fd).map(|s| (s.st_dev, s.st_ino))
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
            root_parent_synced: AtomicBool::new(false),
            #[cfg(test)]
            force_stale: AtomicBool::new(false),
        }
    }

    #[cfg(test)]
    pub(super) fn force_stale(&self, on: bool) {
        self.force_stale.store(on, Ordering::Relaxed);
    }

    /// `ENOENT` from an entry operation through `dir` means the directory is gone (every entry
    /// operation in a removed directory reports it) or the entry is. Only a directory with no
    /// links left whose path no longer names it counts as gone; anything else is the entry.
    pub(super) fn stale(&self, dir: &Arc<DirHandle>, e: Errno) -> Option<DirError> {
        if e != Errno::ENOENT {
            return None;
        }
        #[cfg(test)]
        if self.force_stale.load(Ordering::Relaxed) {
            return Some(DirError::Stale {
                dir: Arc::clone(dir),
            });
        }
        (dir.is_removed() && !dir.is_current()).then(|| DirError::Stale {
            dir: Arc::clone(dir),
        })
    }

    #[cfg(test)]
    pub(super) fn set_capacity(&self, capacity: usize) {
        self.inner.lock().capacity = capacity;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.inner.lock().dirs.len()
    }

    #[cfg(test)]
    pub(super) fn clock_len(&self) -> usize {
        self.inner.lock().clock.len()
    }

    /// Splits an absolute path under the root into its parent directory (relative to the root)
    /// and its file name. `None` when the path is not under the root or has no file name.
    pub(super) fn split<'a>(&self, path: &'a Path) -> Option<(&'a Path, &'a OsStr)> {
        let rel = path.strip_prefix(&self.root).ok()?;
        let name = rel.file_name()?;
        Some((rel.parent()?, name))
    }

    /// The handles from the root down to `rel`, opening (and with `create`, making) the
    /// directories not yet cached. `best_effort_root` is the store's root-parent policy.
    pub(super) fn resolve(
        &self,
        rel: &Path,
        create: bool,
        best_effort_root: bool,
    ) -> Result<Vec<Arc<DirHandle>>, DirError> {
        let mut chain = vec![self.root_handle(best_effort_root)?];
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

    /// The cached handles from the root down `rel` as far as they reach, and the rest of `rel`.
    /// Opens nothing but the root.
    pub(super) fn resolve_cached(
        &self,
        rel: &Path,
        best_effort_root: bool,
    ) -> Result<(Vec<Arc<DirHandle>>, PathBuf), DirError> {
        let mut chain = vec![self.root_handle(best_effort_root)?];
        let mut key = PathBuf::new();
        let mut rest = PathBuf::new();
        for component in rel.components() {
            let Component::Normal(name) = component else {
                continue;
            };
            key.push(name);
            if rest.as_os_str().is_empty() {
                let parent = chain.last().expect("the root is always present");
                if let Some(handle) = self.cached_child(parent, &key) {
                    chain.push(handle);
                    continue;
                }
            }
            rest.push(name);
        }
        Ok((chain, rest))
    }

    /// The cached handle for `key` if it was opened under `parent`.
    fn cached_child(&self, parent: &DirHandle, key: &Path) -> Option<Arc<DirHandle>> {
        let inner = self.inner.lock();
        let handle = inner.dirs.get(key)?;
        if handle.parent != Some(parent.inode) {
            return None;
        }
        handle.used.store(true, Ordering::Relaxed);
        Some(Arc::clone(handle))
    }

    /// The cached handle for `rel`, if any.
    #[cfg(test)]
    pub(super) fn cached(&self, rel: &Path) -> Option<Arc<DirHandle>> {
        self.inner.lock().dirs.get(rel).cloned()
    }

    /// Forgets `handle` if it is the one cached for its directory. Handles under it are replaced
    /// as they are found under another parent or removed.
    pub(super) fn evict(&self, handle: &Arc<DirHandle>) {
        let removed = {
            let mut inner = self.inner.lock();
            match inner.dirs.get(&handle.rel) {
                Some(cached) if Arc::ptr_eq(cached, handle) => inner.dirs.remove(&handle.rel),
                _ => None,
            }
        };
        if removed.is_some() && handle.rel.as_os_str().is_empty() {
            self.root_parent_synced.store(false, Ordering::Relaxed);
        }
        // Closes the descriptor, outside the lock.
        drop(removed);
    }

    /// Forgets the handle at `rel` if its directory was removed.
    pub(super) fn evict_removed(&self, rel: &Path) {
        let cached = self.inner.lock().dirs.get(rel).cloned();
        if let Some(handle) = cached {
            if handle.is_removed() {
                self.evict(&handle);
            }
        }
    }

    /// Forgets the handle at `rel` if its path no longer names its directory, after the
    /// directory was moved away. Handles under it are replaced as they are found under another
    /// parent.
    pub(super) fn evict_moved(&self, rel: &Path) {
        let cached = self.inner.lock().dirs.get(rel).cloned();
        if let Some(handle) = cached {
            if !handle.is_current() {
                self.evict(&handle);
            }
        }
    }

    /// The root handle, opened on first use. A root removed behind the cache is reported stale
    /// by the operation that notices and recreated here on the retry, as the path-based code
    /// does. Whoever opens an uncached root fsyncs the directory holding its entry: another
    /// thread may have just created it and not synced it yet; with `best_effort_root` a parent
    /// that cannot be opened is skipped, and a later caller without it does the fsync.
    fn root_handle(&self, best_effort_root: bool) -> Result<Arc<DirHandle>, DirError> {
        let key = PathBuf::new();
        let cached = self.inner.lock().dirs.get(&key).cloned();
        if let Some(root) = cached {
            if !best_effort_root && !self.root_parent_synced.load(Ordering::Relaxed) {
                self.sync_root_parent(false)?;
            }
            return Ok(root);
        }
        let open_error = |e: Errno| DirError::Open {
            dir: self.root.clone(),
            source: e.into(),
        };
        let fd = match open(&self.root, DIR_FLAGS, Mode::empty()) {
            Err(Errno::ENOENT) => {
                // Creates and fsyncs every missing ancestor and the first existing one.
                super::create_parent_dirs(&self.root.join("#"), Errno::ENOENT.into(), true)
                    .map_err(|e| DirError::Create {
                        dir: self.root.clone(),
                        source: io::Error::other(e),
                    })?;
                open(&self.root, DIR_FLAGS, Mode::empty()).map_err(open_error)?
            }
            other => other.map_err(open_error)?,
        };
        self.sync_root_parent(best_effort_root)?;
        let handle =
            DirHandle::new(key.clone(), self.root.clone(), fd, None).map_err(open_error)?;
        let handle = Arc::new(handle);
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.dirs.get(&key) {
            let existing = Arc::clone(existing);
            drop(inner);
            // Closes the losing descriptor, outside the lock.
            drop(handle);
            return Ok(existing);
        }
        inner.dirs.insert(key, Arc::clone(&handle));
        drop(inner);
        Ok(handle)
    }

    /// Fsyncs the directory holding the root and records the success; with `best_effort_root` a
    /// directory that cannot be opened is left alone, unrecorded.
    fn sync_root_parent(&self, best_effort_root: bool) -> Result<(), DirError> {
        let Some(parent) = self.root.parent() else {
            return Ok(());
        };
        match super::fsync_dir(parent) {
            Ok(()) => {
                self.root_parent_synced.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(e) if best_effort_root && e.kind() == io::ErrorKind::PermissionDenied => Ok(()),
            Err(source) => Err(DirError::RootParentSync {
                dir: parent.to_path_buf(),
                source,
            }),
        }
    }

    /// The handle for `key`, the entry `name` of `parent`. A cached handle opened under another
    /// directory than `parent` is not it: the one it was opened under was removed or replaced.
    pub(super) fn child(
        &self,
        parent: &Arc<DirHandle>,
        key: &Path,
        name: &OsStr,
        create: bool,
    ) -> Result<Arc<DirHandle>, DirError> {
        if let Some(handle) = self.cached_child(parent, key) {
            return Ok(handle);
        }
        let path = parent.path.join(name);
        let open_error = |e: Errno| {
            self.stale(parent, e).unwrap_or_else(|| DirError::Open {
                dir: path.clone(),
                source: e.into(),
            })
        };
        let fd = loop {
            match openat(&parent.fd, name, DIR_FLAGS, Mode::empty()) {
                Ok(fd) => break fd,
                Err(Errno::ENOENT) if create => {}
                Err(e) => return Err(open_error(e)),
            }
            // The entry change is recorded on the parent before it becomes visible.
            let made = parent
                .change(
                    |fd| match mkdirat(fd, name, Mode::from_bits_truncate(0o777)) {
                        Ok(()) => Ok(true),
                        Err(Errno::EEXIST) => Ok(false),
                        Err(e) => Err(e),
                    },
                )
                .map_err(|e| {
                    self.stale(parent, e).unwrap_or_else(|| DirError::Create {
                        dir: path.clone(),
                        source: e.into(),
                    })
                })?;
            match openat(&parent.fd, name, DIR_FLAGS, Mode::empty()) {
                Ok(fd) => break fd,
                // Removed again since this call made it: make it again.
                Err(Errno::ENOENT) if made => continue,
                Err(e) => return Err(open_error(e)),
            }
        };
        let handle = DirHandle::new(key.to_path_buf(), path.clone(), fd, Some(parent.inode))
            .map_err(open_error)?;
        let handle = Arc::new(handle);
        let mut inner = self.inner.lock();
        let existing = inner.dirs.get(key).cloned();
        if let Some(existing) = existing {
            if existing.parent == Some(parent.inode) {
                return Ok(existing);
            }
            // The cached handle was opened under another directory of this name. If that one
            // still exists, `parent` is the one that was moved away, and the cached handle is the
            // right one for this path; this one serves the chain without being cached.
            drop(inner);
            if !existing.is_removed() && existing.is_current() {
                return Ok(handle);
            }
            inner = self.inner.lock();
            if !inner
                .dirs
                .get(key)
                .is_some_and(|cached| Arc::ptr_eq(cached, &existing))
            {
                return Ok(handle);
            }
        }
        let evicted = inner.insert(key.to_path_buf(), Arc::clone(&handle));
        drop(inner);
        // Closes the descriptors, outside the lock.
        drop(evicted);
        Ok(handle)
    }
}

impl Inner {
    /// Caches `handle` under `key`, replacing what was there, and returns the handles this
    /// dropped from the cache.
    fn insert(&mut self, key: PathBuf, handle: Arc<DirHandle>) -> Vec<Arc<DirHandle>> {
        let mut dropped = Vec::new();
        if self.clock.len() > 2 * self.dirs.len() {
            self.compact();
        }
        // Second chance: a handle used since it was last examined, dirty, or held by an operation
        // (every clone is made under this lock, so a count above the cache's own and this
        // upgrade means one) is kept. Two passes clear every `used` flag, so after them only such
        // handles remain and the cache grows beyond its capacity rather than dropping one.
        let mut examined = 0;
        while self.dirs.len() >= self.capacity && examined < 2 * self.clock.len() {
            let Some(candidate) = self.clock.pop_front() else {
                break;
            };
            let Some(existing) = candidate.upgrade() else {
                continue;
            };
            if !self
                .dirs
                .get(&existing.rel)
                .is_some_and(|cached| Arc::ptr_eq(cached, &existing))
            {
                continue;
            }
            examined += 1;
            if existing.used.swap(false, Ordering::Relaxed)
                || existing.is_dirty()
                || Arc::strong_count(&existing) > 2
            {
                self.clock.push_back(candidate);
                continue;
            }
            dropped.extend(self.dirs.remove(&existing.rel));
        }
        self.clock.push_back(Arc::downgrade(&handle));
        dropped.extend(self.dirs.insert(key, handle));
        dropped
    }

    /// Drops the clock entries of handles no longer cached.
    fn compact(&mut self) {
        let dirs = &self.dirs;
        self.clock.retain(|entry| {
            entry.upgrade().is_some_and(|handle| {
                dirs.get(&handle.rel)
                    .is_some_and(|cached| Arc::ptr_eq(cached, &handle))
            })
        });
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
