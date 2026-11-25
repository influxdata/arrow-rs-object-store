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

//! The process-wide registry of directories that durable writes of the path-based code are
//! creating, and of directories a creation could not fsync.
//!
//! On the Unix targets without the directory-handle cache, a durable write that creates its
//! parent directories registers them here before creating any, so a concurrent durable write into
//! one of them waits until they are fsynced instead of returning while their entries are not yet
//! durable. A creation that fails after creating some of them records those as unsynced; the next
//! durable write under them fsyncs them, and the directories holding their entries, before it
//! returns. Directories are matched by path within this process.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, MutexGuard};

use super::{Error, fsync_dir};
use crate::Result;

/// Directories [`create_parent_dirs`] is creating and fsyncing, with the number of calls doing
/// so, and directories a creation could not fsync. A durable write into one waits for, or makes,
/// them durable before returning.
pub(super) struct DirRegistry {
    creating: BTreeMap<PathBuf, usize>,
    pub(super) unsynced: BTreeSet<PathBuf>,
}

static DIRS: Mutex<DirRegistry> = Mutex::new(DirRegistry {
    creating: BTreeMap::new(),
    unsynced: BTreeSet::new(),
});
static DIRS_CREATED: Condvar = Condvar::new();

pub(super) fn lock_dirs() -> MutexGuard<'static, DirRegistry> {
    DIRS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Registration in [`DIRS`], removed on drop. Unless `synced` is set, the directories that exist
/// by then are recorded as unsynced.
pub(super) struct CreatingDirs {
    dirs: Vec<PathBuf>,
    synced: bool,
}

impl CreatingDirs {
    pub(super) fn register(dirs: Vec<PathBuf>) -> Self {
        let mut registry = lock_dirs();
        for dir in &dirs {
            *registry.creating.entry(dir.clone()).or_default() += 1;
        }
        Self {
            dirs,
            synced: false,
        }
    }

    /// Registers `dir` too, unless it already is.
    fn add(&mut self, dir: &Path) {
        if self.dirs.iter().any(|d| d == dir) {
            return;
        }
        *lock_dirs().creating.entry(dir.to_path_buf()).or_default() += 1;
        self.dirs.push(dir.to_path_buf());
    }
}

impl Drop for CreatingDirs {
    fn drop(&mut self) {
        // A path that is not a directory has no entries to sync; recording it would keep it in the
        // process-global set for good. One whose state cannot be determined might be one.
        let unsynced: Vec<_> = if self.synced {
            Vec::new()
        } else {
            self.dirs
                .iter()
                .filter(|dir| match std::fs::metadata(dir) {
                    Ok(meta) => meta.is_dir(),
                    Err(e) => e.kind() != ErrorKind::NotFound,
                })
                .cloned()
                .collect()
        };
        let mut registry = lock_dirs();
        for dir in &self.dirs {
            if let Some(count) = registry.creating.get_mut(dir) {
                *count -= 1;
                if *count == 0 {
                    registry.creating.remove(dir);
                }
            }
        }
        registry.unsynced.extend(unsynced);
        DIRS_CREATED.notify_all();
    }
}

/// Waits until no ancestor directory of `path` is being created. Returns the highest ancestor
/// left unsynced, if any.
fn wait_for_ancestor_creation(path: &Path) -> Option<PathBuf> {
    let mut registry = lock_dirs();
    while path
        .ancestors()
        .skip(1)
        .any(|dir| registry.creating.contains_key(dir))
    {
        registry = DIRS_CREATED
            .wait(registry)
            .unwrap_or_else(|e| e.into_inner());
    }
    path.ancestors()
        .skip(1)
        .filter(|dir| registry.unsynced.contains(*dir))
        .last()
        .map(PathBuf::from)
}

/// Fsyncs the parent directory of `path` so a change to its directory entries (e.g. one just made
/// by a rename or hard link) is durable, along with any unsynced ancestors and their parents.
/// A no-op when `path` has no parent.
pub(super) fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    let unsynced = wait_for_ancestor_creation(path);
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let highest = unsynced
        .as_deref()
        .and_then(|dir| dir.parent())
        .unwrap_or(parent);
    for dir in parent.ancestors() {
        fsync_dir(dir)?;
        if dir == highest {
            break;
        }
    }
    if let Some(unsynced) = unsynced {
        lock_dirs()
            .unsynced
            .retain(|dir| !(path.starts_with(dir) && dir.starts_with(&unsynced)));
    }
    Ok(())
}

/// Creates `parent` and its missing ancestors for a durable write, registering them first so
/// concurrent writes into them wait for their fsyncs. Every directory created here is fsynced,
/// along with the directory holding each one's entry, so the new directory entries are durable.
pub(super) fn create_parent_dirs(parent: &Path) -> Result<()> {
    let create_dir_error = |source| Error::UnableToCreateDir {
        source,
        path: parent.into(),
    };

    // Register the missing directories before creating any, so concurrent writes into them wait
    // for their fsyncs.
    let mut missing = Vec::new();
    let mut dir = parent;
    while !dir.exists() {
        missing.push(dir.to_path_buf());
        match dir.parent() {
            Some(p) => dir = p,
            None => break,
        }
    }
    let mut creating = CreatingDirs::register(missing);

    // What exists can change between the check above and the creation (a concurrent delete with
    // automatic cleanup removes emptied ancestors), so sync what this call created rather than
    // what it found missing. Another caller may have created a directory in between the created
    // ones; it holds an entry made here, so it is synced too.
    let mut created = Vec::new();
    create_dir_all_recording(parent, &mut creating, &mut created).map_err(create_dir_error)?;
    if let (Some(highest), Some(deepest)) = (created.first(), created.last()) {
        let holding_highest = highest.parent();
        for dir in deepest.ancestors() {
            fsync_dir(dir).map_err(|source| Error::UnableToSyncFile {
                source,
                path: dir.into(),
            })?;
            if Some(dir) == holding_highest {
                break;
            }
        }
    }
    creating.synced = true;
    Ok(())
}

/// Creates `dir` and its missing ancestors one at a time, as [`std::fs::create_dir_all`] does,
/// registering each in `creating` before creating it. Appends the directories it created to
/// `created`, highest first; one another caller created in the meantime is not appended.
fn create_dir_all_recording(
    dir: &Path,
    creating: &mut CreatingDirs,
    created: &mut Vec<PathBuf>,
) -> io::Result<()> {
    creating.add(dir);
    match create_dir(dir) {
        Ok(()) => {
            created.push(dir.to_path_buf());
            return Ok(());
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(_) if dir.is_dir() => return Ok(()),
        Err(e) => return Err(e),
    }
    match dir.parent() {
        Some(p) => create_dir_all_recording(p, creating, created)?,
        None => return Err(io::Error::other("failed to create whole tree")),
    }
    match create_dir(dir) {
        Ok(()) => {
            created.push(dir.to_path_buf());
            Ok(())
        }
        Err(_) if dir.is_dir() => Ok(()),
        Err(e) => Err(e),
    }
}

/// [`std::fs::create_dir`], preceded by the test hook.
fn create_dir(dir: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(hook) = BEFORE_CREATE_DIR
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_mut()
    {
        hook(dir);
    }
    std::fs::create_dir(dir)
}

/// Runs before every [`create_dir`], with the directory about to be created.
#[cfg(test)]
#[allow(clippy::type_complexity)]
pub(super) static BEFORE_CREATE_DIR: Mutex<Option<Box<dyn FnMut(&Path) + Send>>> = Mutex::new(None);
