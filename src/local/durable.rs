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

//! The write operations of a [`LocalFileSystem`] with fsync enabled, on the directory handles of
//! [`dir_cache`]: every entry change is recorded on its directory before it becomes visible, and
//! every handle on the changed chains that still owes an fsync gets one before the operation
//! returns, also when it fails.

use std::ffi::{OsStr, OsString};

use nix::errno::Errno;

use super::dir_cache::{self, DirCache, DirError, DirHandle};
use super::*;

/// Errors of one attempt of an operation: a stale handle (the operation is retried) or a final
/// error.
enum Attempt {
    Stale(Arc<DirHandle>),
    Failed(crate::Error),
}

impl From<DirError> for Attempt {
    fn from(e: DirError) -> Self {
        match e {
            DirError::Stale { dir } => Self::Stale(dir),
            e => Self::Failed(e.into()),
        }
    }
}

impl From<crate::Error> for Attempt {
    fn from(e: crate::Error) -> Self {
        Self::Failed(e)
    }
}

impl From<Error> for Attempt {
    fn from(e: Error) -> Self {
        Self::Failed(e.into())
    }
}

impl From<DirError> for crate::Error {
    fn from(e: DirError) -> Self {
        match e {
            // A second attempt from the root still found the directory gone.
            DirError::Stale { dir } => Error::NotFound {
                source: io::Error::from(ErrorKind::NotFound),
                path: dir.path().to_path_buf(),
            },
            DirError::Open { dir, source } if source.kind() == ErrorKind::NotFound => {
                Error::NotFound { source, path: dir }
            }
            DirError::Open { dir, source } => Error::UnableToOpenFile { source, path: dir },
            DirError::Create { dir, source } => Error::UnableToCreateDir { source, path: dir },
            DirError::Sync { dir, source } => Error::UnableToSyncFile { source, path: dir },
        }
        .into()
    }
}

/// Retries an operation makes at most, beyond one per directory on its chain (a cascade of
/// removed ancestors evicts one per retry), so an operation ends if a directory looks removed on
/// every attempt.
const MAX_RETRIES: usize = 16;

/// Runs `op`, an operation on chains `depth` directories long at most, retrying it from the root
/// each time it finds a cached directory removed. A handle is evicted at most once per operation;
/// a fresh one found removed is another removal.
fn retrying<T>(
    cache: &DirCache,
    depth: &std::path::Path,
    mut op: impl FnMut() -> Result<T, Attempt>,
) -> Result<T> {
    let limit = depth.components().count() + MAX_RETRIES;
    let mut evicted: Vec<Arc<DirHandle>> = Vec::new();
    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(Attempt::Failed(e)) => return Err(e),
            Err(Attempt::Stale(dir)) => {
                if evicted.len() >= limit || evicted.iter().any(|seen| Arc::ptr_eq(seen, &dir)) {
                    return Err(DirError::Stale { dir }.into());
                }
                cache.evict(&dir);
                evicted.push(dir);
            }
        }
    }
}

/// `e` from an entry operation through `dir`: a stale `dir` to retry, or `other`.
fn entry_error(
    cache: &DirCache,
    dir: &Arc<DirHandle>,
    e: Errno,
    other: impl FnOnce(io::Error) -> Error,
) -> Attempt {
    cache
        .stale(dir, e)
        .map(Attempt::from)
        .unwrap_or_else(|| other(e.into()).into())
}

/// The parent directory of `path` relative to the store root, and its file name.
fn split<'a>(
    cache: &DirCache,
    path: &'a std::path::Path,
) -> Result<(&'a std::path::Path, &'a OsStr)> {
    cache.split(path).ok_or_else(|| {
        Error::UnableToOpenFile {
            source: io::Error::from(ErrorKind::InvalidInput),
            path: path.to_path_buf(),
        }
        .into()
    })
}

fn last(chain: &[Arc<DirHandle>]) -> &Arc<DirHandle> {
    chain.last().expect("the root is always present")
}

/// The handles down to the existing directory `rel`; a missing one is reported as `path` not found.
fn resolve_existing(
    cache: &DirCache,
    rel: &std::path::Path,
    path: &std::path::Path,
) -> Result<Vec<Arc<DirHandle>>, Attempt> {
    cache.resolve(rel, false).map_err(|e| match e {
        DirError::Open { source, .. } if source.kind() == ErrorKind::NotFound => Error::NotFound {
            path: path.to_path_buf(),
            source,
        }
        .into(),
        e => e.into(),
    })
}

/// The handles down to `rel`, the destination directory of a copy or rename of `from`, made only
/// when `from` exists so a missing source leaves no directories behind.
fn resolve_destination(
    cache: &DirCache,
    rel: &std::path::Path,
    from: &std::path::Path,
) -> Result<Vec<Arc<DirHandle>>, Attempt> {
    match cache.resolve(rel, false) {
        Err(DirError::Open { source, .. }) if source.kind() == ErrorKind::NotFound => {
            if !from.exists() {
                return Err(Error::NotFound {
                    path: from.to_path_buf(),
                    source,
                }
                .into());
            }
            Ok(cache.resolve(rel, true)?)
        }
        chain => Ok(chain?),
    }
}

/// A staging entry `name#N` in a directory, removed when dropped unless it was published by
/// rename. The removal is best effort, as every staging cleanup is: a failed removal or a crash
/// leaves the entry behind, and `list` hides it by its name.
struct Staging {
    dir: Arc<DirHandle>,
    name: OsString,
    keep: bool,
}

impl Staging {
    /// Creates the first free `name#N` in `dir` with `create`, returning its result and the entry.
    fn new<T>(
        dir: &Arc<DirHandle>,
        name: &OsStr,
        mut create: impl FnMut(&OsStr) -> nix::Result<T>,
    ) -> nix::Result<(T, Self)> {
        let mut id = 1;
        loop {
            let mut staging = name.to_os_string();
            staging.push("#");
            staging.push(id.to_string());
            match create(&staging) {
                Ok(created) => {
                    return Ok((
                        created,
                        Self {
                            dir: Arc::clone(dir),
                            name: staging,
                            keep: false,
                        },
                    ));
                }
                Err(Errno::EEXIST) => id += 1,
                Err(e) => return Err(e),
            }
        }
    }

    fn path(&self) -> PathBuf {
        self.dir.path().join(&self.name)
    }

    /// Publishes the entry as `name`: by rename for `Overwrite`, by hard link for `Create`
    /// (`EEXIST` when `name` exists).
    fn publish(mut self, mode: &PutMode, name: &OsStr) -> nix::Result<()> {
        match mode {
            PutMode::Overwrite => {
                dir_cache::rename_between(&self.dir, &self.name, &self.dir, name)?;
                self.keep = true;
                Ok(())
            }
            PutMode::Create => dir_cache::link_within(&self.dir, &self.name, name),
            PutMode::Update(_) => unreachable!(),
        }
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.keep {
            let _ = dir_cache::unlink_unsynced(&self.dir, &self.name);
        }
    }
}

/// Creates a staging file for `path` in `parent`, the directory of `path`.
fn stage_file(
    cache: &DirCache,
    parent: &Arc<DirHandle>,
    name: &OsStr,
    path: &std::path::Path,
) -> Result<(File, Staging), Attempt> {
    Staging::new(parent, name, |s| dir_cache::create_new(parent, s)).map_err(|e| {
        entry_error(cache, parent, e, |source| Error::UnableToOpenFile {
            source,
            path: path.to_path_buf(),
        })
    })
}

/// `e` from publishing `path` in create mode: `AlreadyExists` for `EEXIST`, else `other`.
fn exists_or(e: Errno, path: &std::path::Path, other: impl FnOnce(io::Error) -> Error) -> Error {
    match e {
        Errno::EEXIST => Error::AlreadyExists {
            path: path.to_string_lossy().to_string(),
            source: e.into(),
        },
        e => other(e.into()),
    }
}

/// Fsyncs `chain` after `result`, whatever it is: the changes a failed operation made would
/// otherwise keep the handles dirty, and dirty handles are never evicted.
fn synced<T, E: From<DirError>>(chain: &[Arc<DirHandle>], result: Result<T, E>) -> Result<T, E> {
    let synced = dir_cache::sync_chain(chain);
    let value = result?;
    synced?;
    Ok(value)
}

fn put_result(metadata: Metadata) -> PutResult {
    PutResult {
        e_tag: Some(get_etag(&metadata)),
        version: None,
    }
}

/// Writes `payload` to a staging file in `path`'s directory, syncs and closes it, and publishes
/// it as `path` by rename (`Overwrite`) or hard link (`Create`).
pub(super) fn put(
    config: &Config,
    path: &std::path::Path,
    payload: PutPayload,
    mode: &PutMode,
) -> Result<PutResult> {
    let cache = &config.dirs;
    let (rel_parent, name) = split(cache, path)?;
    let metadata = retrying(cache, rel_parent, || {
        let chain = cache.resolve(rel_parent, true)?;
        let parent = last(&chain);
        let published = stage_file(cache, parent, name, path).and_then(|(mut file, staging)| {
            let staging_path = staging.path();
            payload
                .iter()
                .try_for_each(|x| file.write_all(x))
                .map_err(|source| Error::UnableToCopyDataToFile { source })?;
            let metadata = staged_metadata(&file, &staging_path)?;
            sync_and_close(file, &staging_path, true)?;
            let rename_error = |source| Error::UnableToRenameFile { source };
            staging.publish(mode, name).map_err(|e| match mode {
                PutMode::Create => exists_or(e, path, rename_error),
                _ => rename_error(e.into()),
            })?;
            Ok(metadata)
        });
        synced(&chain, published)
    })?;
    Ok(put_result(metadata))
}

/// Hard-links `from` to `to`, via a staging link and a rename for `Overwrite`.
pub(super) fn copy(
    config: &Config,
    from: &std::path::Path,
    to: &std::path::Path,
    mode: CopyMode,
) -> Result<()> {
    let cache = &config.dirs;
    let (rel_parent, name) = split(cache, to)?;
    retrying(cache, rel_parent, || {
        let chain = resolve_destination(cache, rel_parent, from)?;
        let parent = last(&chain);
        let linked = match mode {
            CopyMode::Create => dir_cache::link_from_path(parent, from, name),
            CopyMode::Overwrite => {
                Staging::new(parent, name, |s| dir_cache::link_from_path(parent, from, s))
                    .and_then(|((), staging)| staging.publish(&PutMode::Overwrite, name))
            }
        };
        let linked = linked.map_err(|e| match e {
            Errno::ENOENT => entry_error(cache, parent, e, |source| Error::NotFound {
                path: from.to_path_buf(),
                source,
            }),
            e => {
                let copy_error = |source| Error::UnableToCopyFile {
                    from: from.to_path_buf(),
                    to: to.to_path_buf(),
                    source,
                };
                match mode {
                    CopyMode::Create => exists_or(e, to, copy_error),
                    CopyMode::Overwrite => copy_error(e.into()),
                }
                .into()
            }
        });
        synced(&chain, linked)
    })
}

/// Renames `from` to `to`, replacing `to`, and fsyncs both directories.
pub(super) fn rename(config: &Config, from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    let cache = &config.dirs;
    let (from_rel, from_name) = split(cache, from)?;
    let (to_rel, to_name) = split(cache, to)?;
    let deeper = std::cmp::max_by_key(from_rel, to_rel, |rel| rel.components().count());
    retrying(cache, deeper, || {
        let from_chain = resolve_existing(cache, from_rel, from)?;
        let to_chain = resolve_destination(cache, to_rel, from)?;
        let (from_dir, to_dir) = (last(&from_chain), last(&to_chain));
        let renamed =
            dir_cache::rename_between(from_dir, from_name, to_dir, to_name).map_err(|e| match e {
                Errno::ENOENT => cache
                    .stale(from_dir, e)
                    .or_else(|| cache.stale(to_dir, e))
                    .map(Attempt::from)
                    .unwrap_or_else(|| {
                        Error::NotFound {
                            path: from.to_path_buf(),
                            source: e.into(),
                        }
                        .into()
                    }),
                e => Error::UnableToCopyFile {
                    from: from.to_path_buf(),
                    to: to.to_path_buf(),
                    source: e.into(),
                }
                .into(),
            });
        // A renamed directory's handle still answers to its old path, whatever the fsyncs do.
        if renamed.is_ok() {
            cache.evict_moved(&from_rel.join(from_name));
        }
        synced(&from_chain, synced(&to_chain, renamed))
    })
}

/// Deletes `path`; with `sync` its directory is fsynced afterwards, with `automatic_cleanup` the
/// directories left empty are removed (and leave the cache). Without `sync` no directory below
/// the cached chain needs a handle: the object and the directories left empty are removed through
/// the deepest cached ancestor.
pub(super) fn delete(
    config: &Config,
    path: &std::path::Path,
    automatic_cleanup: bool,
    sync: bool,
) -> Result<()> {
    let cache = &config.dirs;
    let (rel_parent, name) = split(cache, path)?;
    retrying(cache, rel_parent, || {
        let (chain, below) = if sync {
            (resolve_existing(cache, rel_parent, path)?, PathBuf::new())
        } else {
            cache.resolve_cached(rel_parent)?
        };
        let parent = last(&chain);
        let unlink = if sync {
            dir_cache::unlink_in
        } else {
            dir_cache::unlink_unsynced
        };
        unlink(parent, below.join(name).as_os_str()).map_err(|e| match e {
            Errno::ENOENT => entry_error(cache, parent, e, |source| Error::NotFound {
                path: path.to_path_buf(),
                source,
            }),
            e => Error::UnableToDeleteFile {
                path: path.to_path_buf(),
                source: e.into(),
            }
            .into(),
        })?;
        if sync {
            dir_cache::sync_chain(&chain)?;
        }
        if automatic_cleanup {
            // Deepest first, stopping at the first directory that is not empty (or is the root).
            let below_removed = below
                .ancestors()
                .filter(|dir| !dir.as_os_str().is_empty())
                .all(|dir| {
                    let removed = dir_cache::rmdir_in(parent, dir.as_os_str()).is_ok();
                    if removed {
                        // A handle below an evicted level may still be cached.
                        cache.evict_removed(&parent.rel().join(dir));
                    }
                    removed
                });
            if !below_removed {
                return Ok(());
            }
            for (dir, grandparent) in chain.iter().skip(1).zip(chain.iter()).rev() {
                let Some(name) = dir.rel().file_name() else {
                    break;
                };
                if dir_cache::rmdir_in(grandparent, name).is_err() {
                    break;
                }
                cache.evict(dir);
            }
        }
        Ok(())
    })
}

/// A multipart upload's staging file, published by [`Upload::complete`].
pub(super) struct Upload {
    chain: Vec<Arc<DirHandle>>,
    staging: Staging,
    name: OsString,
}

impl std::fmt::Debug for Upload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("staging", &self.staging.path())
            .finish_non_exhaustive()
    }
}

/// Creates the staging file for a multipart upload to `dest`. Its directory stays dirty until the
/// upload completes or is discarded.
pub(super) fn stage(config: &Config, dest: &std::path::Path) -> Result<(File, Upload)> {
    let cache = &config.dirs;
    let (rel_parent, name) = split(cache, dest)?;
    retrying(cache, rel_parent, || {
        let chain = cache.resolve(rel_parent, true)?;
        let parent = last(&chain);
        match stage_file(cache, parent, name, dest) {
            Ok((file, staging)) => Ok((
                file,
                Upload {
                    chain,
                    staging,
                    name: name.to_os_string(),
                },
            )),
            Err(e) => synced(&chain, Err(e)),
        }
    })
}

impl Upload {
    /// Syncs and closes `file`, renames it into place and fsyncs the chain.
    pub(super) fn complete(self, file: File) -> Result<Metadata> {
        let Self {
            chain,
            staging,
            name,
        } = self;
        let staging_path = staging.path();
        let published = staged_metadata(&file, &staging_path).and_then(|metadata| {
            sync_and_close(file, &staging_path, true)?;
            staging
                .publish(&PutMode::Overwrite, &name)
                .map_err(|e| Error::UnableToRenameFile { source: e.into() })?;
            Ok(metadata)
        });
        synced(&chain, published)
    }

    /// Removes the staging file and fsyncs the chain, leaving the handles clean.
    pub(super) fn discard(self) -> Result<()> {
        let Self {
            chain, mut staging, ..
        } = self;
        staging.keep = true;
        let removed = dir_cache::unlink_unsynced(&staging.dir, &staging.name).map_err(|e| {
            Error::UnableToDeleteFile {
                source: e.into(),
                path: staging.path(),
            }
            .into()
        });
        synced(&chain, removed)
    }
}
