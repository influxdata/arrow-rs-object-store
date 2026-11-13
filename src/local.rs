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

//! An object store implementation for a local filesystem
use std::fs::{metadata, symlink_metadata, File, Metadata, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;
use std::{collections::BTreeSet, io};
use std::{collections::VecDeque, path::PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt};
use futures::{FutureExt, TryStreamExt};
use parking_lot::Mutex;
use url::Url;
use walkdir::{DirEntry, WalkDir};

use crate::{
    maybe_spawn_blocking,
    path::{absolute_path_to_url, Path},
    util::InvalidGetRange,
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};

// With fsync enabled, writes go through cached directory handles where `ENOENT` from an entry
// operation in a removed directory is known from the kernel sources (Linux); every other target
// keeps the path-based code below. Every `cfg` on this condition belongs to this gate.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod dir_cache;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod durable;
#[cfg(any(target_os = "linux", target_os = "android"))]
use dir_cache::DirCache;

/// Whether a durable copy may replace the destination (`copy`) or must create it
/// (`copy_if_not_exists`).
#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyMode {
    Overwrite,
    Create,
}

/// A specialized `Error` for filesystem object store-related errors
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Unable to walk dir: {}", source)]
    UnableToWalkDir { source: walkdir::Error },

    #[error("Unable to access metadata for {}: {}", path, source)]
    Metadata {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        path: String,
    },

    #[error("Unable to copy data to file: {}", source)]
    UnableToCopyDataToFile { source: io::Error },

    #[error("Unable to rename file: {}", source)]
    UnableToRenameFile { source: io::Error },

    #[error("Unable to create dir {}: {}", path.display(), source)]
    UnableToCreateDir { source: io::Error, path: PathBuf },

    #[error("Unable to create file {}: {}", path.display(), source)]
    UnableToCreateFile { source: io::Error, path: PathBuf },

    #[error("Unable to delete file {}: {}", path.display(), source)]
    UnableToDeleteFile { source: io::Error, path: PathBuf },

    #[error("Unable to open file {}: {}", path.display(), source)]
    UnableToOpenFile { source: io::Error, path: PathBuf },

    #[error("Unable to read data from file {}: {}", path.display(), source)]
    UnableToReadBytes { source: io::Error, path: PathBuf },

    #[error("Out of range of file {}, expected: {}, actual: {}", path.display(), expected, actual)]
    OutOfRange {
        path: PathBuf,
        expected: u64,
        actual: u64,
    },

    #[error("Requested range was invalid")]
    InvalidRange { source: InvalidGetRange },

    #[error("Unable to copy file from {} to {}: {}", from.display(), to.display(), source)]
    UnableToCopyFile {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },

    #[error("NotFound")]
    NotFound { path: PathBuf, source: io::Error },

    #[error("Error seeking file {}: {}", path.display(), source)]
    Seek { source: io::Error, path: PathBuf },

    #[error("Unable to convert URL \"{}\" to filesystem path", url)]
    InvalidUrl { url: Url },

    #[error("AlreadyExists")]
    AlreadyExists { path: String, source: io::Error },

    #[error("Unable to canonicalize filesystem root: {}", path.display())]
    UnableToCanonicalize { path: PathBuf, source: io::Error },

    #[error("Filenames containing trailing '/#\\d+/' are not supported: {}", path)]
    InvalidPath { path: String },

    #[error("Unable to sync data to disk for {}: {}", path.display(), source)]
    UnableToSyncFile { source: io::Error, path: PathBuf },

    #[error(
        "Unable to sync the directory holding the store root, {}: {}; \
         LocalFileSystem::with_best_effort_root_parent_sync(true) skips it",
        path.display(),
        source
    )]
    UnableToSyncRootParent { source: io::Error, path: PathBuf },

    #[error("Upload aborted")]
    Aborted,
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound {
                path: path.to_string_lossy().to_string(),
                source: source.into(),
            },
            Error::AlreadyExists { path, source } => Self::AlreadyExists {
                path,
                source: source.into(),
            },
            _ => Self::Generic {
                store: "LocalFileSystem",
                source: Box::new(source),
            },
        }
    }
}

/// Explicitly close a file, checking for errors that would be silently ignored by Rust's `File::drop()`.
///
/// On network filesystems (e.g. NFS), `close()` can fail and indicate data loss.
fn close_file(file: File) -> std::result::Result<(), io::Error> {
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::io::IntoRawFd;
        close_raw_fd(file.into_raw_fd())
    }
    #[cfg(target_family = "windows")]
    {
        use std::os::windows::io::IntoRawHandle;
        close_raw_handle(file.into_raw_handle())
    }
    #[cfg(not(any(target_family = "unix", target_family = "windows")))]
    {
        drop(file);
        Ok(())
    }
}

/// Close a raw file descriptor, returning the error `close()` reports.
///
/// The descriptor must be one the caller owns (and so is not closed again elsewhere)
/// or one that is not open.
#[cfg(target_family = "unix")]
fn close_raw_fd(fd: std::os::unix::io::RawFd) -> std::result::Result<(), io::Error> {
    // SAFETY: per this function's contract, `fd` is owned by the caller or not open.
    match unsafe { nix::libc::close(fd) } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Close a raw handle, returning the error `CloseHandle` reports.
///
/// The handle must be one the caller owns (and so is not closed again elsewhere)
/// or one that is not valid.
#[cfg(target_family = "windows")]
fn close_raw_handle(handle: std::os::windows::io::RawHandle) -> std::result::Result<(), io::Error> {
    // SAFETY: per this function's contract, `handle` is owned by the caller or invalid.
    match unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) } {
        0 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// Local filesystem storage providing an [`ObjectStore`] interface to files on
/// local disk. Can optionally be created with a directory prefix
///
/// # Path Semantics
///
/// This implementation follows the [file URI] scheme outlined in [RFC 3986]. In
/// particular paths are delimited by `/`
///
/// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
/// [RFC 3986]: https://www.rfc-editor.org/rfc/rfc3986
///
/// # Path Semantics
///
/// [`LocalFileSystem`] will expose the path semantics of the underlying filesystem, which may
/// have additional restrictions beyond those enforced by [`Path`].
///
/// For example:
///
/// * Windows forbids certain filenames, e.g. `COM0`,
/// * Windows forbids folders with trailing `.`
/// * Windows forbids certain ASCII characters, e.g. `<` or `|`
/// * OS X forbids filenames containing `:`
/// * Leading `-` are discouraged on Unix systems where they may be interpreted as CLI flags
/// * Filesystems may have restrictions on the maximum path or path segment length
/// * Filesystem support for non-ASCII characters is inconsistent
///
/// Additionally some filesystems, such as NTFS, are case-insensitive, whilst others like
/// FAT don't preserve case at all. Further some filesystems support non-unicode character
/// sequences, such as unpaired UTF-16 surrogates, and [`LocalFileSystem`] will error on
/// encountering such sequences.
///
/// Finally, filenames matching the regex `/.*#\d+/`, e.g. `foo.parquet#123`, are not supported
/// by [`LocalFileSystem`] as they are used to provide atomic writes. Such files will be ignored
/// for listing operations, and attempting to address such a file will error.
///
/// # Tokio Compatibility
///
/// Tokio discourages performing blocking IO on a tokio worker thread, however,
/// no major operating systems have stable async file APIs. Therefore if called from
/// a tokio context, this will use [`tokio::runtime::Handle::spawn_blocking`] to dispatch
/// IO to a blocking thread pool, much like `tokio::fs` does under-the-hood.
///
/// If not called from a tokio context, this will perform IO on the current thread with
/// no additional complexity or overheads
///
/// # Symlinks
///
/// [`LocalFileSystem`] will follow symlinks as normal, however, it is worth noting:
///
/// * Broken symlinks will be silently ignored by listing operations
/// * No effort is made to prevent breaking symlinks when deleting files
/// * Symlinks that resolve to paths outside the root **will** be followed
/// * Mutating a file through one or more symlinks will mutate the underlying file
/// * Deleting a path that resolves to a symlink will only delete the symlink
///
/// # Cross-Filesystem Copy
///
/// [`LocalFileSystem::copy`] is implemented using [`std::fs::hard_link`], and therefore
/// does not support copying across filesystem boundaries.
///
#[derive(Debug)]
pub struct LocalFileSystem {
    config: Arc<Config>,
    // if you want to delete empty directories when deleting files
    automatic_cleanup: bool,
    // if true, fsync written files and their parent directories after writes
    fsync: bool,
    // if true, a root parent that cannot be opened is not fsynced
    best_effort_root_parent_sync: bool,
}

#[derive(Debug)]
struct Config {
    root: Url,
    /// Directory handles for durable writes; used only with `fsync`.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    dirs: DirCache,
}

impl Config {
    fn new(root: Url) -> Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let dirs = DirCache::new(
            root.to_file_path()
                .map_err(|_| Error::InvalidUrl { url: root.clone() })?,
        );
        Ok(Self {
            root,
            #[cfg(any(target_os = "linux", target_os = "android"))]
            dirs,
        })
    }
}

impl std::fmt::Display for LocalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalFileSystem({})", self.config.root)
    }
}

impl Default for LocalFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileSystem {
    /// Create new filesystem storage with no prefix
    pub fn new() -> Self {
        Self {
            config: Arc::new(Config::new(Url::parse("file:///").unwrap()).unwrap()),
            automatic_cleanup: false,
            fsync: false,
            best_effort_root_parent_sync: false,
        }
    }

    /// Create new filesystem storage with `prefix` applied to all paths
    ///
    /// Returns an error if the path does not exist
    ///
    pub fn new_with_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = std::fs::canonicalize(&prefix).map_err(|source| {
            let path = prefix.as_ref().into();
            Error::UnableToCanonicalize { source, path }
        })?;

        Ok(Self {
            config: Arc::new(Config::new(absolute_path_to_url(path)?)?),
            automatic_cleanup: false,
            fsync: false,
            best_effort_root_parent_sync: false,
        })
    }

    /// Return an absolute filesystem path of the given file location
    pub fn path_to_filesystem(&self, location: &Path) -> Result<PathBuf> {
        if !is_valid_file_path(location) {
            let path = location.as_ref().into();
            let error = Error::InvalidPath { path };
            return Err(error.into());
        }

        let path = self.config.prefix_to_filesystem(location)?;

        #[cfg(target_os = "windows")]
        let path = {
            let path = path.to_string_lossy();

            // Assume the first char is the drive letter and the next is a colon.
            let mut out = String::new();
            let drive = &path[..2]; // The drive letter and colon (e.g., "C:")
            let filepath = &path[2..].replace(':', "%3A"); // Replace subsequent colons
            out.push_str(drive);
            out.push_str(filepath);
            PathBuf::from(out)
        };

        Ok(path)
    }

    /// Enable automatic cleanup of empty directories when deleting files
    pub fn with_automatic_cleanup(mut self, automatic_cleanup: bool) -> Self {
        self.automatic_cleanup = automatic_cleanup;
        self
    }

    /// Enable `fsync` after writes for durability
    ///
    /// When enabled, [`LocalFileSystem`] calls [`File::sync_all`] on written files and fsyncs
    /// the affected parent directories before a write operation
    /// ([`put_opts`](ObjectStore::put_opts), [`copy`](ObjectStore::copy),
    /// [`copy_if_not_exists`](ObjectStore::copy_if_not_exists), [`rename`](ObjectStore::rename),
    /// and multipart upload completion) returns
    /// success. This guarantees that both the file contents and the directory entries pointing
    /// to them are durable on stable storage, matching the implicit durability contract of
    /// remote object stores such as S3 or GCS.
    ///
    /// This trades write throughput for durability and is **disabled by default**.
    ///
    /// Note that directory fsync is only performed on Unix; on other platforms (e.g. Windows)
    /// it is a no-op, as directories cannot be portably opened and synced.
    ///
    /// On Linux the store keeps the directories it writes to open (up to a few thousand
    /// descriptors, plus those of directories with changes not yet fsynced or in use by a write)
    /// and also fsyncs a directory whose entries another, still unfinished write changed. That
    /// coordination is per store: clones share it, while separate [`LocalFileSystem`] instances
    /// on the same root, other processes, writes through another path to the same directory (a
    /// symlink, or different case on a case-insensitive filesystem) and stores with `fsync` off
    /// are not coordinated and can return before a directory they created is durable. For the
    /// same reason, stores with different `fsync` settings must not share a root. Directories
    /// moved or re-targeted outside the store (a rename, a symlink change) are not detected.
    pub fn with_fsync(mut self, fsync: bool) -> Self {
        self.fsync = fsync;
        self
    }

    /// Skip the fsync of the directory holding the store root when that directory cannot be
    /// opened (traverse-only permissions, such as a `0711` home directory)
    ///
    /// With [`with_fsync`](Self::with_fsync) on Linux, the store fsyncs the root's parent
    /// directory when it first opens the root (and whenever it reopens a root that was removed
    /// and recreated), so the root's own directory entry is durable; a parent it cannot open fails
    /// every write with a sync error naming that directory. With this option such a permission
    /// error is skipped instead, and the root's directory entry may then not survive a power
    /// loss. Other errors are still reported. The option has no effect on other targets, where
    /// the path-based code fsyncs only the directories it creates. **Disabled by default**.
    pub fn with_best_effort_root_parent_sync(mut self, best_effort: bool) -> Self {
        self.best_effort_root_parent_sync = best_effort;
        self
    }
}

impl Config {
    /// Return an absolute filesystem path of the given location
    fn prefix_to_filesystem(&self, location: &Path) -> Result<PathBuf> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .expect("url path")
            // technically not necessary as Path ignores empty segments
            // but avoids creating paths with "//" which look odd in error messages.
            .pop_if_empty()
            .extend(location.parts());

        url.to_file_path()
            .map_err(|_| Error::InvalidUrl { url }.into())
    }

    /// Resolves the provided absolute filesystem path to a [`Path`] prefix
    fn filesystem_to_path(&self, location: &std::path::Path) -> Result<Path> {
        Ok(Path::from_absolute_path_with_base(
            location,
            Some(&self.root),
        )?)
    }
}

fn is_valid_file_path(path: &Path) -> bool {
    match path.filename() {
        Some(p) => match p.split_once('#') {
            Some((_, suffix)) if !suffix.is_empty() => {
                // Valid if contains non-digits
                !suffix.as_bytes().iter().all(|x| x.is_ascii_digit())
            }
            _ => true,
        },
        None => false,
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(crate::Error::NotImplemented);
        }

        if !opts.attributes.is_empty() {
            return Err(crate::Error::NotImplemented);
        }

        let path = self.path_to_filesystem(location)?;
        let fsync = self.fsync;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let (config, best_effort_root) =
            (Arc::clone(&self.config), self.best_effort_root_parent_sync);
        maybe_spawn_blocking(move || {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            if fsync {
                return durable::put(&config, best_effort_root, &path, payload, &opts.mode);
            }
            let (mut file, staging_path) = new_staged_upload(&path, fsync)?;
            if let Err(source) = payload.iter().try_for_each(|x| file.write_all(x)) {
                drop(file);
                let _ = std::fs::remove_file(&staging_path); // Attempt to cleanup
                return Err(Error::UnableToCopyDataToFile { source }.into());
            }
            // Atomically publish the staged file. When fsync is enabled the publish helpers flush
            // the file's contents and the destination's parent directory to disk first, so a
            // successful return is durable; the fsync calls are bundled into the helpers so a
            // file-system modification can never be left unsynced. The helpers clean up the
            // staging file on failure.
            let metadata = match opts.mode {
                PutMode::Overwrite => finish_staged_rename(file, &staging_path, &path, fsync)?,
                PutMode::Create => finish_staged_hard_link(file, &staging_path, &path, fsync)?,
                PutMode::Update(_) => unreachable!(),
            };

            Ok(PutResult {
                e_tag: Some(get_etag(&metadata)),
                version: None,
            })
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if !opts.attributes.is_empty() {
            return Err(crate::Error::NotImplemented);
        }

        let dest = self.path_to_filesystem(location)?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if self.fsync {
            let (config, best_effort_root) =
                (Arc::clone(&self.config), self.best_effort_root_parent_sync);
            let staged = dest.clone();
            let (file, upload) =
                maybe_spawn_blocking(move || durable::stage(&config, best_effort_root, &staged))
                    .await?;
            let staged = Staged::Durable(upload);
            return Ok(Box::new(LocalUpload::new(staged, dest, file, true)));
        }
        let (file, src) = new_staged_upload(&dest, self.fsync)?;
        let staged = Staged::Path(src);
        Ok(Box::new(LocalUpload::new(staged, dest, file, self.fsync)))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let path = self.path_to_filesystem(&location)?;
        maybe_spawn_blocking(move || {
            let (file, metadata) = open_file(&path)?;
            let meta = convert_metadata(metadata, location);
            options.check_preconditions(&meta)?;

            let range = match options.range {
                Some(r) => r
                    .as_range(meta.size)
                    .map_err(|source| Error::InvalidRange { source })?,
                None => 0..meta.size,
            };

            Ok(GetResult {
                payload: GetResultPayload::File(file, path),
                attributes: Attributes::default(),
                range,
                meta,
            })
        })
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let path = self.path_to_filesystem(location)?;
        maybe_spawn_blocking(move || {
            let (mut file, metadata) = open_file(&path)?;
            read_range(&mut file, metadata.len(), &path, range)
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let path = self.path_to_filesystem(location)?;
        let ranges = ranges.to_vec();
        maybe_spawn_blocking(move || {
            // Vectored IO might be faster
            let (mut file, metadata) = open_file(&path)?;
            ranges
                .into_iter()
                .map(|r| read_range(&mut file, metadata.len(), &path, r))
                .collect()
        })
        .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let config = Arc::clone(&self.config);
        let path = self.path_to_filesystem(location)?;
        let automatic_cleanup = self.automatic_cleanup;
        let fsync = self.fsync;
        let best_effort_root = self.best_effort_root_parent_sync;
        maybe_spawn_blocking(move || {
            // `with_fsync` does not apply to standalone deletes; only create-mode rename
            // fsyncs its internal source removal as part of the durable copy-and-delete.
            let durable = (fsync, false, best_effort_root);
            Self::delete_location(config, automatic_cleanup, path, durable)
        })
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_with_maybe_offset(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_with_maybe_offset(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let config = Arc::clone(&self.config);

        let prefix = prefix.cloned().unwrap_or_default();
        let resolved_prefix = config.prefix_to_filesystem(&prefix)?;

        maybe_spawn_blocking(move || {
            let walkdir = WalkDir::new(&resolved_prefix)
                .min_depth(1)
                .max_depth(1)
                .follow_links(true);

            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();

            for entry_res in walkdir.into_iter().map(convert_walkdir_result) {
                if let Some(entry) = entry_res? {
                    let is_directory = entry.file_type().is_dir();
                    let entry_location = config.filesystem_to_path(entry.path())?;
                    if !is_directory && !is_valid_file_path(&entry_location) {
                        continue;
                    }

                    let mut parts = match entry_location.prefix_match(&prefix) {
                        Some(parts) => parts,
                        None => continue,
                    };

                    let common_prefix = match parts.next() {
                        Some(p) => p,
                        None => continue,
                    };

                    drop(parts);

                    if is_directory {
                        common_prefixes.insert(prefix.child(common_prefix));
                    } else if let Some(metadata) = convert_entry(entry, entry_location)? {
                        objects.push(metadata);
                    }
                }
            }

            Ok(ListResult {
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;
        let fsync = self.fsync;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if fsync {
            let (config, best_effort_root) =
                (Arc::clone(&self.config), self.best_effort_root_parent_sync);
            return maybe_spawn_blocking(move || {
                durable::copy(&config, best_effort_root, &from, &to, CopyMode::Overwrite)
            })
            .await;
        }

        let mut id = 0;
        // In order to make this atomic we:
        //
        // - hard link to a hidden temporary file
        // - atomically rename this temporary file into place
        //
        // This is necessary because hard_link returns an error if the destination already exists
        maybe_spawn_blocking(move || loop {
            let staged = staged_upload_path(&to, &id.to_string());
            // Stage via a temporary hard link; the source is already durable so the
            // staging link itself needs no fsync (the publish rename below fsyncs the
            // shared parent directory).
            match std::fs::hard_link(&from, &staged) {
                // `rename` bundles in the fsync of `to`'s parent directory.
                Ok(_) => {
                    return rename(&staged, &to, fsync).map_err(|source| {
                        // Once renamed, `staged` may already be a new upload's file.
                        if let RenameError::Rename(_) = source {
                            let _ = std::fs::remove_file(&staged); // Attempt to clean up
                        }
                        let source = source.into();
                        Error::UnableToCopyFile { from, to, source }.into()
                    });
                }
                Err(source) => match source.kind() {
                    ErrorKind::AlreadyExists => id += 1,
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source, fsync)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;
        let fsync = self.fsync;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if fsync {
            let (config, best_effort_root) =
                (Arc::clone(&self.config), self.best_effort_root_parent_sync);
            return maybe_spawn_blocking(move || {
                durable::rename(&config, best_effort_root, &from, &to)
            })
            .await;
        }
        maybe_spawn_blocking(move || loop {
            // Unlike multipart `complete`, there is no freshly written file to
            // `sync_all` here: `from` is an existing, already-durable object and a
            // rename only mutates directory entries. `rename` bundles in the fsync of
            // both affected directories (destination, and source if it differs).
            match rename(&from, &to, fsync).map_err(io::Error::from) {
                Ok(_) => return Ok(()),
                Err(source) => match source.kind() {
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source, fsync)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = self.path_to_filesystem(from)?;
        let to = self.path_to_filesystem(to)?;
        let fsync = self.fsync;

        #[cfg(any(target_os = "linux", target_os = "android"))]
        if fsync {
            let (config, best_effort_root) =
                (Arc::clone(&self.config), self.best_effort_root_parent_sync);
            return maybe_spawn_blocking(move || {
                durable::copy(&config, best_effort_root, &from, &to, CopyMode::Create)
            })
            .await;
        }

        maybe_spawn_blocking(move || loop {
            // The source is an existing object that is already durable, so no file
            // sync is needed; `hard_link` bundles in the fsync of `to`'s parent dir.
            match hard_link(&from, &to, fsync) {
                Ok(_) => return Ok(()),
                Err(source) => match source.kind() {
                    ErrorKind::AlreadyExists => {
                        return Err(Error::AlreadyExists {
                            path: to.to_str().unwrap().to_string(),
                            source,
                        }
                        .into())
                    }
                    ErrorKind::NotFound => match from.exists() {
                        true => create_parent_dirs(&to, source, fsync)?,
                        false => return Err(Error::NotFound { path: from, source }.into()),
                    },
                    _ => return Err(Error::UnableToCopyFile { from, to, source }.into()),
                },
            }
        })
        .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_if_not_exists(from, to).await?;
        let config = Arc::clone(&self.config);
        let path = self.path_to_filesystem(from)?;
        let automatic_cleanup = self.automatic_cleanup;
        let durable = (self.fsync, self.fsync, self.best_effort_root_parent_sync);
        maybe_spawn_blocking(move || {
            Self::delete_location(config, automatic_cleanup, path, durable)
        })
        .await
    }
}

impl LocalFileSystem {
    /// Deletes `location`. `durable` is `(fsync, sync, best_effort_root_parent_sync)`: with
    /// `fsync` the store is durable and the change goes through its directory handles; with
    /// `sync` the parent directory is fsynced afterwards.
    fn delete_location(
        config: Arc<Config>,
        automatic_cleanup: bool,
        path: PathBuf,
        durable: (bool, bool, bool),
    ) -> Result<()> {
        let (fsync, sync, best_effort_root) = durable;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if fsync {
            return durable::delete(&config, best_effort_root, &path, automatic_cleanup, sync);
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let _ = (fsync, best_effort_root);
        if let Err(e) = std::fs::remove_file(&path) {
            Err(match e.kind() {
                ErrorKind::NotFound => Error::NotFound { path, source: e }.into(),
                _ => Error::UnableToDeleteFile { path, source: e }.into(),
            })
        } else {
            if sync {
                fsync_parent_dir(&path).map_err(|source| Error::UnableToSyncFile {
                    source,
                    path: path.clone(),
                })?;
            }

            if !automatic_cleanup {
                return Ok(());
            }

            let root = &config.root;
            let root = root
                .to_file_path()
                .map_err(|_| Error::InvalidUrl { url: root.clone() })?;

            // here we will try to traverse up and delete an empty dir if possible until we reach the root or get an error
            let mut parent = path.parent();

            while let Some(loc) = parent {
                if loc != root && std::fs::remove_dir(loc).is_ok() {
                    parent = loc.parent();
                } else {
                    break;
                }
            }

            Ok(())
        }
    }

    fn list_with_maybe_offset(
        &self,
        prefix: Option<&Path>,
        maybe_offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let config = Arc::clone(&self.config);

        let root_path = match prefix {
            Some(prefix) => match config.prefix_to_filesystem(prefix) {
                Ok(path) => path,
                Err(e) => return futures::future::ready(Err(e)).into_stream().boxed(),
            },
            None => config.root.to_file_path().unwrap(),
        };

        let walkdir = WalkDir::new(root_path)
            // Don't include the root directory itself
            .min_depth(1)
            .follow_links(true);

        let maybe_offset = maybe_offset.cloned();

        let s = walkdir.into_iter().flat_map(move |result_dir_entry| {
            // Apply offset filter before proceeding, to reduce statx file system calls
            // This matters for NFS mounts
            if let (Some(offset), Ok(entry)) = (maybe_offset.as_ref(), result_dir_entry.as_ref()) {
                let location = config.filesystem_to_path(entry.path());
                match location {
                    Ok(path) if path <= *offset => return None,
                    Err(e) => return Some(Err(e)),
                    _ => {}
                }
            }

            let entry = match convert_walkdir_result(result_dir_entry).transpose()? {
                Ok(entry) => entry,
                Err(e) => return Some(Err(e)),
            };

            if !entry.path().is_file() {
                return None;
            }

            match config.filesystem_to_path(entry.path()) {
                Ok(path) => match is_valid_file_path(&path) {
                    true => convert_entry(entry, path).transpose(),
                    false => None,
                },
                Err(e) => Some(Err(e)),
            }
        });

        // If no tokio context, return iterator directly as no
        // need to perform chunked spawn_blocking reads
        if tokio::runtime::Handle::try_current().is_err() {
            return futures::stream::iter(s).boxed();
        }

        // Otherwise list in batches of CHUNK_SIZE
        const CHUNK_SIZE: usize = 1024;

        let buffer = VecDeque::with_capacity(CHUNK_SIZE);
        futures::stream::try_unfold((s, buffer), |(mut s, mut buffer)| async move {
            if buffer.is_empty() {
                (s, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match s.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (s, buffer)
                })
                .await?;
            }

            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (s, buffer)))),
                None => Ok(None),
            }
        })
        .boxed()
    }
}

/// Creates the parent directories of `path` or returns an error based on `source` if no parent
///
/// When `fsync` is true, every directory created here is fsynced, up to and including the first
/// pre-existing ancestor (whose entry list also changed), so the new directory entries are durable.
fn create_parent_dirs(path: &std::path::Path, source: io::Error, fsync: bool) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        let path = path.to_path_buf();
        Error::UnableToCreateFile { path, source }
    })?;

    // Record the deepest already-existing ancestor *before* creating any directories, so that
    // afterwards we know exactly which directories are new and need to be fsynced.
    let first_existing = fsync.then(|| {
        let mut dir = parent;
        while !dir.exists() {
            match dir.parent() {
                Some(p) => dir = p,
                None => break,
            }
        }
        dir.to_path_buf()
    });

    std::fs::create_dir_all(parent).map_err(|source| {
        let path = parent.into();
        Error::UnableToCreateDir { source, path }
    })?;

    if let Some(first_existing) = first_existing {
        // Walk from `parent` up to `first_existing`, fsyncing each directory whose entries changed.
        let mut dir = parent;
        loop {
            fsync_dir(dir).map_err(|source| Error::UnableToSyncFile {
                source,
                path: dir.into(),
            })?;
            if dir == first_existing {
                break;
            }
            dir = match dir.parent() {
                Some(p) => p,
                None => break,
            };
        }
    }
    Ok(())
}

/// Renames `from` to `to`, then — when `fsync` is enabled — fsyncs the destination's parent
/// directory (and the source's too, if it differs) so the moved directory entries are durable.
///
/// The directory fsync is bundled in deliberately: every durable rename goes through here, so the
/// post-rename fsync can never be forgotten at an individual call site.
fn rename(from: &std::path::Path, to: &std::path::Path, fsync: bool) -> Result<(), RenameError> {
    std::fs::rename(from, to).map_err(RenameError::Rename)?;
    if fsync {
        fsync_parent_dir(to).map_err(RenameError::Fsync)?;
        // A cross-directory move also removes an entry from the source directory.
        if from.parent() != to.parent() {
            fsync_parent_dir(from).map_err(RenameError::Fsync)?;
        }
    }
    Ok(())
}

/// Where [`rename`] failed: after `Fsync`, `from` has been moved to `to`.
enum RenameError {
    Rename(io::Error),
    Fsync(io::Error),
}

impl From<RenameError> for io::Error {
    fn from(e: RenameError) -> Self {
        match e {
            RenameError::Rename(e) | RenameError::Fsync(e) => e,
        }
    }
}

/// Hard-links `original` to `link`, then — when `fsync` is enabled — fsyncs `link`'s parent
/// directory so the new directory entry is durable.
///
/// As with [`rename`], the directory fsync is bundled in so it cannot be forgotten at a call site.
fn hard_link(original: &std::path::Path, link: &std::path::Path, fsync: bool) -> io::Result<()> {
    std::fs::hard_link(original, link)?;
    if fsync {
        fsync_parent_dir(link)?;
    }
    Ok(())
}

/// Durably publishes the freshly-written staging file `file` (located at `src`) to `dest` via a
/// rename.
///
/// When `fsync` is enabled, the file's contents are flushed before — and `dest`'s parent
/// directory after — the rename, so a successful return is durable. The file is always closed
/// before the rename (checking for close errors): required for NFS error detection and for some
/// FUSE filesystems (e.g. Blobfuse) that only commit the data on close.
///
/// Returns the file's metadata. A failure before the rename succeeds removes the staging file.
/// After a successful rename its path may already belong to a new upload, so it is left alone.
fn finish_staged_rename(
    file: File,
    src: &std::path::Path,
    dest: &std::path::Path,
    fsync: bool,
) -> Result<Metadata> {
    let closed = staged_metadata(&file, src).and_then(|metadata| {
        sync_and_close(file, src, fsync)?;
        Ok(metadata)
    });
    let err = match closed {
        Ok(metadata) => match rename(src, dest, fsync) {
            Ok(()) => return Ok(metadata),
            Err(RenameError::Fsync(source)) => {
                return Err(Error::UnableToRenameFile { source }.into());
            }
            Err(RenameError::Rename(source)) => Error::UnableToRenameFile { source }.into(),
        },
        Err(e) => e,
    };
    let _ = std::fs::remove_file(src); // Attempt to cleanup
    Err(err)
}

/// Like [`finish_staged_rename`] but publishes via a hard link (`PutMode::Create` semantics): the
/// staging file is linked to `dest` and then removed, whether or not linking succeeds. Returns
/// [`Error::AlreadyExists`] if `dest` already exists.
fn finish_staged_hard_link(
    file: File,
    src: &std::path::Path,
    dest: &std::path::Path,
    fsync: bool,
) -> Result<Metadata> {
    let linked = staged_metadata(&file, src).and_then(|metadata| {
        sync_and_close(file, src, fsync)?;
        hard_link(src, dest, fsync).map_err(|source| match source.kind() {
            ErrorKind::AlreadyExists => Error::AlreadyExists {
                path: dest.to_str().unwrap().to_string(),
                source,
            },
            _ => Error::UnableToRenameFile { source },
        })?;
        Ok(metadata)
    });
    let _ = std::fs::remove_file(src); // Attempt to cleanup
    linked
}

/// The metadata of the staging `file` at `src`.
fn staged_metadata(file: &File, src: &std::path::Path) -> Result<Metadata> {
    file.metadata().map_err(|e| {
        Error::Metadata {
            source: e.into(),
            path: src.to_string_lossy().to_string(),
        }
        .into()
    })
}

/// Flushes the freshly-written `file`'s contents to disk (when `fsync` is enabled) and then
/// closes it, checking for close errors that dropping the [`File`] would silently ignore.
fn sync_and_close(file: File, path: &std::path::Path, fsync: bool) -> Result<()> {
    if fsync {
        file.sync_all().map_err(|source| Error::UnableToSyncFile {
            source,
            path: path.into(),
        })?;
    }
    close_file(file).map_err(|source| Error::UnableToCopyDataToFile { source })?;
    Ok(())
}

/// Fsyncs the parent directory of `path` so a change to its directory entries (e.g. one just made
/// by a rename or hard link) is durable. A no-op when `path` has no parent.
fn fsync_parent_dir(path: &std::path::Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => fsync_dir(parent),
        None => Ok(()),
    }
}

/// Fsyncs `dir_path` so that changes to its directory entries are durable.
///
/// This is only meaningful on Unix; on other platforms (e.g. Windows) directories cannot be
/// portably opened as a [`File`] and synced, so this is a no-op.
fn fsync_dir(dir_path: &std::path::Path) -> io::Result<()> {
    #[cfg(target_family = "unix")]
    {
        File::open(dir_path)?.sync_all()
    }
    #[cfg(not(target_family = "unix"))]
    {
        let _ = dir_path;
        Ok(())
    }
}

/// Generates a unique file path `{base}#{suffix}`, returning the opened `File` and `path`
///
/// Creates any directories if necessary, fsyncing them when `fsync` is enabled
fn new_staged_upload(base: &std::path::Path, fsync: bool) -> Result<(File, PathBuf)> {
    let mut multipart_id = 1;
    loop {
        let suffix = multipart_id.to_string();
        let path = staged_upload_path(base, &suffix);
        let mut options = OpenOptions::new();
        match options.read(true).write(true).create_new(true).open(&path) {
            Ok(f) => return Ok((f, path)),
            Err(source) => match source.kind() {
                ErrorKind::AlreadyExists => multipart_id += 1,
                ErrorKind::NotFound => create_parent_dirs(&path, source, fsync)?,
                _ => return Err(Error::UnableToOpenFile { source, path }.into()),
            },
        }
    }
}

/// Returns the unique upload for the given path and suffix
fn staged_upload_path(dest: &std::path::Path, suffix: &str) -> PathBuf {
    let mut staging_path = dest.as_os_str().to_owned();
    staging_path.push("#");
    staging_path.push(suffix);
    staging_path.into()
}

#[derive(Debug)]
struct LocalUpload {
    /// The upload state
    state: Arc<UploadState>,
    /// The staging file, until it is published or removed
    src: Option<Staged>,
    /// The next offset to write into the file
    offset: u64,
    /// Whether to fsync the file and its parent directory on completion
    fsync: bool,
}

/// A multipart upload's staging file.
#[derive(Debug)]
enum Staged {
    /// Its path; published and removed by path.
    Path(PathBuf),
    /// Its entry in the store's directory handles.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    Durable(durable::Upload),
}

impl Staged {
    fn discard(self) -> Result<()> {
        match self {
            Self::Path(src) => std::fs::remove_file(&src)
                .map_err(|source| Error::UnableToDeleteFile { source, path: src }.into()),
            #[cfg(any(target_os = "linux", target_os = "android"))]
            Self::Durable(upload) => upload.discard(),
        }
    }
}

#[derive(Debug)]
struct UploadState {
    dest: PathBuf,
    file: Mutex<Option<File>>,
}

impl LocalUpload {
    pub(crate) fn new(src: Staged, dest: PathBuf, file: File, fsync: bool) -> Self {
        Self {
            state: Arc::new(UploadState {
                dest,
                file: Mutex::new(Some(file)),
            }),
            src: Some(src),
            offset: 0,
            fsync,
        }
    }
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;

        let s = Arc::clone(&self.state);
        maybe_spawn_blocking(move || {
            let mut guard = s.file.lock();
            let file = guard.as_mut().ok_or(Error::Aborted)?;
            file.seek(SeekFrom::Start(offset)).map_err(|source| {
                let path = s.dest.clone();
                Error::Seek { source, path }
            })?;

            data.iter()
                .try_for_each(|x| file.write_all(x))
                .map_err(|source| Error::UnableToCopyDataToFile { source })?;

            Ok(())
        })
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let src = self.src.take().ok_or(Error::Aborted)?;
        let s = Arc::clone(&self.state);
        let fsync = self.fsync;
        maybe_spawn_blocking(move || {
            // Ensure no inflight writes
            let mut guard = s.file.lock();
            let file = guard.take().ok_or(Error::Aborted)?;

            // Durably publish the freshly-written staging file: flush its contents, close it, then
            // rename it into place and fsync the destination's parent directory (the fsync calls
            // are bundled into the helper and only run when fsync is enabled).
            let metadata = match src {
                Staged::Path(src) => finish_staged_rename(file, &src, &s.dest, fsync)?,
                #[cfg(any(target_os = "linux", target_os = "android"))]
                Staged::Durable(upload) => upload.complete(file)?,
            };

            Ok(PutResult {
                e_tag: Some(get_etag(&metadata)),
                version: None,
            })
        })
        .await
    }

    async fn abort(&mut self) -> Result<()> {
        let src = self.src.take().ok_or(Error::Aborted)?;
        maybe_spawn_blocking(move || src.discard()).await
    }
}

impl Drop for LocalUpload {
    fn drop(&mut self) {
        if let Some(src) = self.src.take() {
            // Try to clean up intermediate file ignoring any error
            match tokio::runtime::Handle::try_current() {
                Ok(r) => drop(r.spawn_blocking(move || src.discard())),
                Err(_) => drop(src.discard()),
            };
        }
    }
}

pub(crate) fn chunked_stream(
    mut file: File,
    path: PathBuf,
    range: Range<u64>,
    chunk_size: usize,
) -> BoxStream<'static, Result<Bytes, super::Error>> {
    futures::stream::once(async move {
        let (file, path) = maybe_spawn_blocking(move || {
            file.seek(SeekFrom::Start(range.start as _))
                .map_err(|source| Error::Seek {
                    source,
                    path: path.clone(),
                })?;
            Ok((file, path))
        })
        .await?;

        let stream = futures::stream::try_unfold(
            (file, path, range.end - range.start),
            move |(mut file, path, remaining)| {
                maybe_spawn_blocking(move || {
                    if remaining == 0 {
                        return Ok(None);
                    }

                    let to_read = remaining.min(chunk_size as u64);
                    let cap = usize::try_from(to_read).map_err(|_e| Error::InvalidRange {
                        source: InvalidGetRange::TooLarge {
                            requested: to_read,
                            max: usize::MAX as u64,
                        },
                    })?;
                    let mut buffer = Vec::with_capacity(cap);
                    let read = (&mut file)
                        .take(to_read)
                        .read_to_end(&mut buffer)
                        .map_err(|e| Error::UnableToReadBytes {
                            source: e,
                            path: path.clone(),
                        })?;

                    Ok(Some((buffer.into(), (file, path, remaining - read as u64))))
                })
            },
        );
        Ok::<_, super::Error>(stream)
    })
    .try_flatten()
    .boxed()
}

pub(crate) fn read_range(
    file: &mut File,
    file_len: u64,
    path: &PathBuf,
    range: Range<u64>,
) -> Result<Bytes> {
    // If none of the range is satisfiable we should error, e.g. if the start offset is beyond the
    // extents of the file
    if range.start >= file_len {
        return Err(Error::InvalidRange {
            source: InvalidGetRange::StartTooLarge {
                requested: range.start,
                length: file_len,
            },
        }
        .into());
    }

    // Don't read past end of file
    let to_read = range.end.min(file_len) - range.start;

    file.seek(SeekFrom::Start(range.start)).map_err(|source| {
        let path = path.into();
        Error::Seek { source, path }
    })?;

    let mut buf = Vec::with_capacity(to_read as usize);
    let read = file.take(to_read).read_to_end(&mut buf).map_err(|source| {
        let path = path.into();
        Error::UnableToReadBytes { source, path }
    })? as u64;

    if read != to_read {
        let error = Error::OutOfRange {
            path: path.into(),
            expected: to_read,
            actual: read,
        };

        return Err(error.into());
    }

    Ok(buf.into())
}

fn open_file(path: &PathBuf) -> Result<(File, Metadata)> {
    let ret = match File::open(path).and_then(|f| Ok((f.metadata()?, f))) {
        Err(e) => Err(match e.kind() {
            ErrorKind::NotFound => Error::NotFound {
                path: path.clone(),
                source: e,
            },
            _ => Error::UnableToOpenFile {
                path: path.clone(),
                source: e,
            },
        }),
        Ok((metadata, file)) => match !metadata.is_dir() {
            true => Ok((file, metadata)),
            false => Err(Error::NotFound {
                path: path.clone(),
                source: io::Error::new(ErrorKind::NotFound, "is directory"),
            }),
        },
    }?;
    Ok(ret)
}

fn convert_entry(entry: DirEntry, location: Path) -> Result<Option<ObjectMeta>> {
    match entry.metadata() {
        Ok(metadata) => Ok(Some(convert_metadata(metadata, location))),
        Err(e) => {
            if let Some(io_err) = e.io_error() {
                if io_err.kind() == ErrorKind::NotFound {
                    return Ok(None);
                }
            }
            Err(Error::Metadata {
                source: e.into(),
                path: location.to_string(),
            })?
        }
    }
}

fn last_modified(metadata: &Metadata) -> DateTime<Utc> {
    metadata
        .modified()
        .expect("Modified file time should be supported on this platform")
        .into()
}

fn get_etag(metadata: &Metadata) -> String {
    let inode = get_inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();

    // Use an ETag scheme based on that used by many popular HTTP servers
    // <https://httpd.apache.org/docs/2.2/mod/core.html#fileetag>
    // <https://stackoverflow.com/questions/47512043/how-etags-are-generated-and-configured>
    format!("{inode:x}-{mtime:x}-{size:x}")
}

fn convert_metadata(metadata: Metadata, location: Path) -> ObjectMeta {
    let last_modified = last_modified(&metadata);

    ObjectMeta {
        location,
        last_modified,
        size: metadata.len(),
        e_tag: Some(get_etag(&metadata)),
        version: None,
    }
}

#[cfg(unix)]
/// We include the inode when available to yield an ETag more resistant to collisions
/// and as used by popular web servers such as [Apache](https://httpd.apache.org/docs/2.2/mod/core.html#fileetag)
fn get_inode(metadata: &Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(metadata)
}

#[cfg(not(unix))]
/// On platforms where an inode isn't available, fallback to just relying on size and mtime
fn get_inode(_metadata: &Metadata) -> u64 {
    0
}

/// Convert walkdir results and converts not-found errors into `None`.
/// Convert broken symlinks to `None`.
fn convert_walkdir_result(
    res: std::result::Result<DirEntry, walkdir::Error>,
) -> Result<Option<DirEntry>> {
    match res {
        Ok(entry) => {
            // To check for broken symlink: call symlink_metadata() - it does not traverse symlinks);
            // if ok: check if entry is symlink; and try to read it by calling metadata().
            match symlink_metadata(entry.path()) {
                Ok(attr) => {
                    if attr.is_symlink() {
                        let target_metadata = metadata(entry.path());
                        match target_metadata {
                            Ok(_) => {
                                // symlink is valid
                                Ok(Some(entry))
                            }
                            Err(_) => {
                                // this is a broken symlink, return None
                                Ok(None)
                            }
                        }
                    } else {
                        Ok(Some(entry))
                    }
                }
                Err(_) => Ok(None),
            }
        }

        Err(walkdir_err) => match walkdir_err.io_error() {
            Some(io_err) => match io_err.kind() {
                ErrorKind::NotFound => Ok(None),
                _ => Err(Error::UnableToWalkDir {
                    source: walkdir_err,
                }
                .into()),
            },
            None => Err(Error::UnableToWalkDir {
                source: walkdir_err,
            }
            .into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use futures::TryStreamExt;
    use tempfile::TempDir;

    #[cfg(target_family = "unix")]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(target_family = "unix")]
    use tempfile::NamedTempFile;

    use crate::integration::*;

    use super::*;

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn file_test() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        copy_rename_nonexistent_object(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, false).await;
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn file_test_fsync() {
        // Run the full integration suite with fsync enabled to ensure the durability code
        // paths (file sync + directory fsync on put/copy/rename/multipart, including recursive
        // directory creation) behave identically to the default.
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path())
            .unwrap()
            .with_fsync(true);

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        copy_rename_nonexistent_object(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, false).await;
    }

    #[tokio::test]
    async fn fsync_creates_nested_dirs() {
        // Exercises the recursive directory fsync in `create_parent_dirs`: every directory
        // component is newly created, so each must be synced up to the pre-existing root.
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path())
            .unwrap()
            .with_fsync(true);

        let data = Bytes::from("arbitrary data");

        // `put` (overwrite) into a deeply nested, non-existent directory tree
        let location = Path::from("a/b/c/d/put_file");
        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();
        let read = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(read, data);

        // multipart upload into another nested tree
        let location = Path::from("e/f/g/multipart_file");
        let mut upload = integration.put_multipart(&location).await.unwrap();
        upload.put_part(data.clone().into()).await.unwrap();
        upload.complete().await.unwrap();
        let read = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(read, data);
    }

    /// The store's directory-handle cache.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    mod dir_cache_tests {
        use std::os::unix::fs::MetadataExt;
        use std::sync::atomic::Ordering;

        use super::*;
        use crate::local::dir_cache::{DirError, DirHandle};

        fn fsync_store(root: &TempDir) -> LocalFileSystem {
            LocalFileSystem::new_with_prefix(root.path())
                .unwrap()
                .with_fsync(true)
        }

        fn cached(store: &LocalFileSystem, rel: &str) -> Arc<DirHandle> {
            store
                .config
                .dirs
                .cached(std::path::Path::new(rel))
                .unwrap_or_else(|| panic!("{rel} is not cached"))
        }

        fn fsyncs(dir: &DirHandle) -> u64 {
            dir.fsyncs.load(Ordering::Relaxed)
        }

        fn fail_syncs(dir: &DirHandle, fail: bool) {
            dir.fail_syncs.store(fail, Ordering::Relaxed)
        }

        fn local_error(err: crate::Error) -> Error {
            match err {
                crate::Error::Generic { source, .. } => *source.downcast::<Error>().unwrap(),
                e => panic!("expected a LocalFileSystem error, got {e}"),
            }
        }

        async fn read(store: &LocalFileSystem, location: &str) -> Bytes {
            store
                .get(&Path::from(location))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        }

        /// Another call is creating `a/b`: it has made `a` but not yet fsynced the root, whose
        /// entry for `a` changed, when a write into `a` happens. That write fsyncs the root too.
        #[test]
        fn fsync_write_syncs_the_directory_an_in_progress_creation_left_unsynced() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let root = store
                .config
                .dirs
                .resolve(std::path::Path::new(""), false, false)
                .unwrap()
                .pop()
                .unwrap();

            root.change(|_| {
                fs::create_dir(tmp.path().join("a")).unwrap();
                // The creator has made `a` and is about to make `a/b`; the write runs now.
                futures::executor::block_on(store.put(&Path::from("a/first"), "1".into())).unwrap();
                assert_eq!(fsyncs(&cached(&store, "a")), 1);
                assert_eq!(fsyncs(&root), 1);
                assert!(root.is_dirty(), "the creation is still in flight");
                Ok(())
            })
            .unwrap();

            // The creation finished after the write's fsync of the root, so the root is still
            // owed one.
            assert!(root.is_dirty());
            futures::executor::block_on(store.put(&Path::from("a/second"), "2".into())).unwrap();
            assert_eq!(fsyncs(&root), 2);
            assert!(!root.is_dirty());
        }

        /// The directories a write is cached on are removed and recreated behind the cache's
        /// back (another process, or a clean-up by a store sharing the root): the write notices
        /// its stale handles and starts over from the root.
        #[tokio::test]
        async fn fsync_write_recovers_from_directories_removed_behind_the_cache() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let first = Path::from("a/b/first");
            store.put(&first, "1".into()).await.unwrap();
            let old_a = cached(&store, "a");
            let old_inode = fs::metadata(old_a.path()).unwrap().ino();

            let b = tmp.path().join("a").join("b");
            fs::remove_dir_all(b.parent().unwrap()).unwrap();
            fs::create_dir_all(&b).unwrap();

            store
                .put(&Path::from("a/b/second"), "2".into())
                .await
                .unwrap();
            assert_eq!(read(&store, "a/b/second").await, Bytes::from("2"));
            let new_a = cached(&store, "a");
            assert!(!Arc::ptr_eq(&old_a, &new_a));
            assert_ne!(fs::metadata(new_a.path()).unwrap().ino(), old_inode);
            assert!(matches!(
                store.get(&first).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
        }

        /// A directory whose fsync failed stays owed one; the next write under it fsyncs it.
        #[tokio::test]
        async fn fsync_write_syncs_the_directory_a_failed_sync_left_unsynced() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            store.put(&Path::from("a/first"), "1".into()).await.unwrap();
            let a = cached(&store, "a");
            assert!(!a.is_dirty());

            a.change(|_| Ok(())).unwrap();
            fail_syncs(&a, true);
            a.sync().unwrap_err();
            assert!(a.is_dirty());
            fail_syncs(&a, false);

            let before = fsyncs(&a);
            store
                .put(&Path::from("a/second"), "2".into())
                .await
                .unwrap();
            assert_eq!(fsyncs(&a), before + 1);
            assert!(!a.is_dirty());
        }

        /// A put whose parent directory fails to fsync reports it, naming the directory; the
        /// file is in place and the next put fsyncs the directory.
        #[tokio::test]
        async fn fsync_put_propagates_parent_sync_error() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            store
                .put(&Path::from("dir/first"), "1".into())
                .await
                .unwrap();
            let dir = cached(&store, "dir");

            fail_syncs(&dir, true);
            let puts = [
                ("dir/second", PutMode::Overwrite),
                ("dir/third", PutMode::Create),
            ];
            for (location, mode) in puts {
                let err = store
                    .put_opts(&Path::from(location), "x".into(), mode.into())
                    .await
                    .unwrap_err();
                match local_error(err) {
                    Error::UnableToSyncFile { path, .. } => assert_eq!(&path, dir.path()),
                    e => panic!("expected a sync error, got {e}"),
                }
                assert_eq!(read(&store, location).await, Bytes::from("x"));
            }
            assert!(dir.is_dirty());
            fail_syncs(&dir, false);

            store
                .put(&Path::from("dir/fourth"), "4".into())
                .await
                .unwrap();
            assert!(!dir.is_dirty());
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 4);
        }

        /// A put into a directory that cannot be opened fails before changing anything, naming
        /// the directory, and succeeds once it can be opened.
        #[tokio::test]
        async fn fsync_put_reports_a_parent_it_cannot_open() {
            let tmp = TempDir::new().unwrap();
            let dir = fs::canonicalize(tmp.path()).unwrap().join("dir");
            fs::create_dir(&dir).unwrap();
            let original_permissions = fs::metadata(&dir).unwrap().permissions();
            // Entries can be created in the directory, but it cannot be opened.
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o300)).unwrap();
            let store = fsync_store(&tmp);

            let err = store
                .put(&Path::from("dir/first"), "1".into())
                .await
                .unwrap_err();
            fs::set_permissions(&dir, original_permissions).unwrap();
            match local_error(err) {
                Error::UnableToOpenFile { path, source } => {
                    assert_eq!(path, dir);
                    assert_eq!(source.kind(), ErrorKind::PermissionDenied);
                }
                e => panic!("expected an open error, got {e}"),
            }
            assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);

            store
                .put(&Path::from("dir/first"), "1".into())
                .await
                .unwrap();
            assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        }

        /// The source removal of a create-mode rename fsyncs the source directory; its failure
        /// is reported, after the destination is in place.
        #[tokio::test]
        async fn fsync_rename_if_not_exists_propagates_source_delete_sync_error() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let source = Path::from("source_dir/source_file");
            let dest = Path::from("dest_dir/dest_file");
            store.put(&source, "data".into()).await.unwrap();
            let source_dir = cached(&store, "source_dir");

            fail_syncs(&source_dir, true);
            let err = store
                .rename_if_not_exists(&source, &dest)
                .await
                .unwrap_err();
            fail_syncs(&source_dir, false);
            match local_error(err) {
                Error::UnableToSyncFile { path, .. } => assert_eq!(&path, source_dir.path()),
                e => panic!("expected a sync error, got {e}"),
            }

            assert_eq!(
                read(&store, "dest_dir/dest_file").await,
                Bytes::from("data")
            );
            assert!(matches!(
                store.get(&source).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
            assert!(source_dir.is_dirty());
        }

        /// Copies and renames change entries through the cache: the directories they change are
        /// fsynced, also one another write is still creating, and no staging entry is left.
        #[tokio::test]
        async fn fsync_copy_and_rename_go_through_the_cache() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            store.put(&Path::from("a/f"), "1".into()).await.unwrap();
            let (root, a) = (cached(&store, ""), cached(&store, "a"));
            let root_fsyncs = fsyncs(&root);

            // Overwrite copy into a new directory: the root (new entry `b`) and `b` are fsynced.
            store
                .copy(&Path::from("a/f"), &Path::from("b/g"))
                .await
                .unwrap();
            let b = cached(&store, "b");
            assert_eq!(fsyncs(&b), 1);
            assert_eq!(fsyncs(&root), root_fsyncs + 1);
            assert!(!b.is_dirty());
            assert_eq!(read(&store, "b/g").await, Bytes::from("1"));

            // Overwrite copy onto an existing file replaces it; a create copy refuses to.
            store.put(&Path::from("a/f"), "2".into()).await.unwrap();
            store
                .copy(&Path::from("a/f"), &Path::from("b/g"))
                .await
                .unwrap();
            assert_eq!(read(&store, "b/g").await, Bytes::from("2"));
            let err = store
                .copy_if_not_exists(&Path::from("a/f"), &Path::from("b/g"))
                .await
                .unwrap_err();
            assert!(matches!(err, crate::Error::AlreadyExists { .. }), "{err}");
            assert_eq!(
                fs::read_dir(b.path()).unwrap().count(),
                1,
                "staging entry left"
            );
            store
                .copy_if_not_exists(&Path::from("a/f"), &Path::from("b/h"))
                .await
                .unwrap();
            assert_eq!(read(&store, "b/h").await, Bytes::from("2"));
            assert!(!b.is_dirty());

            // A cross-directory rename fsyncs both the destination and the source directory.
            let (a_fsyncs, root_fsyncs) = (fsyncs(&a), fsyncs(&root));
            store
                .rename(&Path::from("a/f"), &Path::from("c/i"))
                .await
                .unwrap();
            let c = cached(&store, "c");
            assert_eq!(fsyncs(&c), 1);
            assert_eq!(fsyncs(&a), a_fsyncs + 1);
            assert_eq!(fsyncs(&root), root_fsyncs + 1);
            assert!(!a.is_dirty() && !c.is_dirty());
            assert_eq!(read(&store, "c/i").await, Bytes::from("2"));
            assert!(matches!(
                store.get(&Path::from("a/f")).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));

            // Overwrite rename onto an existing file (`b/g` is a link of `c/i` until this put);
            // create-mode rename copies and deletes.
            store.put(&Path::from("c/i"), "3".into()).await.unwrap();
            store
                .rename(&Path::from("c/i"), &Path::from("b/g"))
                .await
                .unwrap();
            assert_eq!(read(&store, "b/g").await, Bytes::from("3"));
            assert_eq!(fs::read_dir(c.path()).unwrap().count(), 0);
            let (b_fsyncs, c_fsyncs) = (fsyncs(&b), fsyncs(&c));
            store
                .rename_if_not_exists(&Path::from("b/g"), &Path::from("c/j"))
                .await
                .unwrap();
            assert_eq!(fsyncs(&c), c_fsyncs + 1);
            assert_eq!(fsyncs(&b), b_fsyncs + 1);
            assert!(!b.is_dirty() && !c.is_dirty());

            // A missing source is reported without creating the destination's directories.
            let (from, to) = (Path::from("c/missing"), Path::from("new/x"));
            for result in [
                store.copy(&from, &to).await,
                store.rename(&from, &to).await,
                store.copy_if_not_exists(&from, &to).await,
            ] {
                assert!(matches!(result.unwrap_err(), crate::Error::NotFound { .. }));
            }
            assert!(!tmp.path().join("new").exists());

            // A copy into a directory another write is still creating fsyncs the root for it.
            let root_fsyncs = fsyncs(&root);
            root.change(|_| {
                fs::create_dir(tmp.path().join("z")).unwrap();
                futures::executor::block_on(store.copy(&Path::from("c/j"), &Path::from("z/k")))
                    .unwrap();
                assert_eq!(fsyncs(&root), root_fsyncs + 1);
                Ok(())
            })
            .unwrap();
        }

        /// A multipart upload stages, publishes and discards through the cache; the directories
        /// it changes are fsynced once, when it completes, or left clean when it is discarded.
        #[tokio::test]
        async fn fsync_multipart_goes_through_the_cache() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);

            let mut upload = store.put_multipart(&Path::from("e/f/g")).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            let f = cached(&store, "e/f");
            assert!(f.is_dirty(), "the staging entry is not yet fsynced");
            assert_eq!(fsyncs(&f), 0);
            upload.complete().await.unwrap();
            assert_eq!(fsyncs(&f), 1);
            assert!(!f.is_dirty() && !cached(&store, "e").is_dirty());
            assert!(!cached(&store, "").is_dirty());
            assert_eq!(read(&store, "e/f/g").await, Bytes::from("data"));
            assert_eq!(fs::read_dir(f.path()).unwrap().count(), 1);

            let mut upload = store.put_multipart(&Path::from("e/f/h")).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            assert_eq!(fs::read_dir(f.path()).unwrap().count(), 2);
            upload.abort().await.unwrap();
            assert_eq!(fs::read_dir(f.path()).unwrap().count(), 1);
            assert!(!f.is_dirty());

            let mut upload = store.put_multipart(&Path::from("e/f/i")).await.unwrap();
            upload.put_part("data".into()).await.unwrap();
            drop(upload);
            for _ in 0..100 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                if fs::read_dir(f.path()).unwrap().count() == 1 && !f.is_dirty() {
                    return;
                }
            }
            panic!("the dropped upload's staging file was not removed");
        }

        /// A delete through a handle whose directory was replaced retries from the root; a
        /// missing entry in a live directory is reported without evicting the handle.
        #[tokio::test]
        async fn fsync_delete_retries_a_stale_handle() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            let location = Path::from("a/b/f");
            store.put(&location, "1".into()).await.unwrap();
            let old_b = cached(&store, "a/b");

            let err = store.delete(&Path::from("a/b/missing")).await.unwrap_err();
            assert!(matches!(err, crate::Error::NotFound { .. }), "{err}");
            assert!(Arc::ptr_eq(&old_b, &cached(&store, "a/b")));

            let b = tmp.path().join("a/b");
            fs::remove_dir_all(&b).unwrap();
            fs::create_dir(&b).unwrap();
            fs::write(b.join("f"), "2").unwrap();
            store.delete(&location).await.unwrap();
            assert!(!b.join("f").exists());
            // Evicted, and not reopened: the retry went through `a`.
            assert!(cache.cached(std::path::Path::new("a/b")).is_none());
        }

        #[tokio::test]
        async fn fsync_delete_dirs_automatically() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp).with_automatic_cleanup(true);
            let cache = &store.config.dirs;
            let location = Path::from("nested/file/test_file");
            store.put(&location, "data".into()).await.unwrap();
            assert!(cache.cached(std::path::Path::new("nested/file")).is_some());

            store.delete(&location).await.unwrap();
            assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
            assert!(cache.cached(std::path::Path::new("nested")).is_none());
            assert!(cache.cached(std::path::Path::new("nested/file")).is_none());
            assert!(matches!(
                store.delete(&location).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));

            // Recreated from scratch; the root's changed entry is synced by the put.
            store.put(&location, "again".into()).await.unwrap();
            assert!(!cached(&store, "").is_dirty());
        }

        /// A delete without a sync opens no directory below the cached chain: the object and the
        /// directories it leaves empty go through the deepest cached ancestor.
        #[tokio::test]
        async fn fsync_delete_opens_no_uncached_directory() {
            let tmp = TempDir::new().unwrap();
            fsync_store(&tmp)
                .put(&Path::from("a/b/c/f"), "1".into())
                .await
                .unwrap();
            fsync_store(&tmp)
                .put(&Path::from("a/keep"), "1".into())
                .await
                .unwrap();

            let store = fsync_store(&tmp).with_automatic_cleanup(true);
            // `a/b/c` is cached while `a/b`, the level above it, is not.
            store.put(&Path::from("a/b/c/g"), "1".into()).await.unwrap();
            let c = cached(&store, "a/b/c");
            store.config.dirs.evict(&cached(&store, "a/b"));
            store.delete(&Path::from("a/b/c/f")).await.unwrap();
            store.delete(&Path::from("a/b/c/g")).await.unwrap();
            assert_eq!(store.config.dirs.len(), 2, "the root and `a` stay cached");
            assert!(store
                .config
                .dirs
                .cached(std::path::Path::new("a/b/c"))
                .is_none());
            drop(c);
            assert!(!tmp.path().join("a/b").exists());
            assert!(tmp.path().join("a/keep").exists());
            assert!(matches!(
                store.delete(&Path::from("a/b/c/f")).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
            assert_eq!(store.config.dirs.len(), 2);
        }

        #[tokio::test]
        async fn fsync_delete_leaves_handles_clean() {
            let tmp = TempDir::new().unwrap();

            let store = fsync_store(&tmp);
            store.put(&Path::from("a/b/f"), "x".into()).await.unwrap();
            store.delete(&Path::from("a/b/f")).await.unwrap();
            assert!(!cached(&store, "a/b").is_dirty());

            let store = fsync_store(&tmp).with_automatic_cleanup(true);
            store.put(&Path::from("a/keep"), "x".into()).await.unwrap();
            store.put(&Path::from("a/b/f"), "x".into()).await.unwrap();
            store.delete(&Path::from("a/b/f")).await.unwrap();
            assert!(!tmp.path().join("a/b").exists());
            assert!(!cached(&store, "a").is_dirty());
        }

        #[tokio::test]
        async fn fsync_failed_put_leaves_handles_clean() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let location = Path::from("a/b/f");
            store.put(&location, "x".into()).await.unwrap();

            // Creates and removes a staging file in `a/b` before failing.
            let err = store
                .put_opts(&location, "y".into(), PutMode::Create.into())
                .await
                .unwrap_err();
            assert!(matches!(err, crate::Error::AlreadyExists { .. }));
            assert!(!cached(&store, "a/b").is_dirty());
        }

        /// A handle an operation holds is never evicted, so a write after it still sees the
        /// changes it records.
        #[tokio::test]
        async fn fsync_cache_keeps_handles_an_operation_holds() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            cache.set_capacity(4);

            let held = cache
                .resolve(std::path::Path::new("a"), true, false)
                .unwrap();
            for i in 0..10 {
                let location = Path::from(format!("d{i}/f"));
                store.put(&location, "x".into()).await.unwrap();
            }
            assert!(Arc::ptr_eq(&held[1], &cached(&store, "a")));

            drop(held);
            for i in 10..20 {
                let location = Path::from(format!("d{i}/f"));
                store.put(&location, "x".into()).await.unwrap();
            }
            assert!(cache.cached(std::path::Path::new("a")).is_none());
        }

        /// Evicting a stale handle leaves the live handle that replaced it alone.
        #[tokio::test]
        async fn fsync_evicting_a_stale_handle_spares_its_replacement() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            store.put(&Path::from("a/b/f"), "1".into()).await.unwrap();
            let old = cached(&store, "a/b");
            fs::remove_dir_all(tmp.path().join("a/b")).unwrap();
            store.put(&Path::from("a/b/g"), "2".into()).await.unwrap();
            let new = cached(&store, "a/b");
            assert!(!Arc::ptr_eq(&old, &new));

            store.config.dirs.evict(&old);
            assert!(Arc::ptr_eq(&new, &cached(&store, "a/b")));
        }

        /// A cached directory is not served under the handle of a directory it was not opened
        /// in: that handle is stale, and the chain restarts from the root.
        #[tokio::test]
        async fn fsync_cached_child_is_not_served_under_a_stale_parent() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            store.put(&Path::from("a/b/c/f"), "1".into()).await.unwrap();
            let b1 = cached(&store, "a/b");
            fs::remove_dir_all(tmp.path().join("a/b")).unwrap();
            store.put(&Path::from("a/b/c/g"), "2".into()).await.unwrap();
            let (b2, c2) = (cached(&store, "a/b"), cached(&store, "a/b/c"));

            let (key, name) = (std::path::Path::new("a/b/c"), std::ffi::OsStr::new("c"));
            let err = cache.child(&b1, key, name, false).unwrap_err();
            assert!(matches!(err, DirError::Stale { dir } if Arc::ptr_eq(&dir, &b1)));
            assert!(Arc::ptr_eq(
                &c2,
                &cache.child(&b2, key, name, false).unwrap()
            ));
        }

        /// The eviction order does not grow with directories that left the cache.
        #[tokio::test]
        async fn fsync_cache_clock_stays_bounded() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp).with_automatic_cleanup(true);
            let cache = &store.config.dirs;
            let location = Path::from("a/b/f");
            for _ in 0..200 {
                store.put(&location, "x".into()).await.unwrap();
                store.delete(&location).await.unwrap();
            }
            // At most twice the handles cached at once (root, `a`, `a/b`), plus the newest.
            let clock = cache.clock_len();
            assert!(clock <= 2 * 3 + 1, "{clock} clock entries after 200 cycles");
        }

        /// Renaming a directory moves its cached handle out from under its key, and the handles
        /// under it are replaced when next looked up; the next write at the old path makes a
        /// new directory.
        #[tokio::test]
        async fn fsync_rename_of_a_directory_evicts_its_handles() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            store.put(&Path::from("a/b/x"), "1".into()).await.unwrap();
            let (a1, b1) = (cached(&store, "a"), cached(&store, "a/b"));
            store
                .rename(&Path::from("a"), &Path::from("c"))
                .await
                .unwrap();
            assert!(cache.cached(std::path::Path::new("a")).is_none());
            assert_eq!(read(&store, "c/b/x").await, Bytes::from("1"));

            store.put(&Path::from("a/b/y"), "2".into()).await.unwrap();
            assert_eq!(read(&store, "a/b/y").await, Bytes::from("2"));
            assert!(matches!(
                store.get(&Path::from("c/b/y")).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
            assert!(!Arc::ptr_eq(&a1, &cached(&store, "a")));
            assert!(!Arc::ptr_eq(&b1, &cached(&store, "a/b")));
        }

        /// Renaming a directory fsyncs the changes its handles still owe before they are
        /// evicted: a later write into the moved directory opens new handles that know nothing
        /// of them.
        #[tokio::test]
        async fn fsync_rename_of_a_directory_syncs_its_dirty_handles() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let mut upload = store
                .put_multipart(&Path::from("a/b/pending"))
                .await
                .unwrap();
            upload.put_part("data".into()).await.unwrap();
            let (old_a, old_b) = (cached(&store, "a"), cached(&store, "a/b"));
            assert!(old_a.is_dirty() && old_b.is_dirty());

            store
                .rename(&Path::from("a"), &Path::from("c"))
                .await
                .unwrap();
            assert!(!old_a.is_dirty() && !old_b.is_dirty());
            assert_eq!(fsyncs(&old_b), 1);
            assert!(store
                .config
                .dirs
                .cached(std::path::Path::new("a"))
                .is_none());

            store
                .put(&Path::from("c/b/file"), "1".into())
                .await
                .unwrap();
            assert_eq!(read(&store, "c/b/file").await, Bytes::from("1"));
            upload.complete().await.unwrap();
            assert_eq!(read(&store, "c/b/pending").await, Bytes::from("data"));
        }

        /// The handle of a renamed directory is evicted even when the fsync after the rename
        /// fails: the rename itself happened.
        #[tokio::test]
        async fn fsync_rename_of_a_directory_evicts_its_handles_when_the_fsync_fails() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            store.put(&Path::from("a/b/x"), "1".into()).await.unwrap();
            let root = cached(&store, "");

            fail_syncs(&root, true);
            let err = store
                .rename(&Path::from("a"), &Path::from("c"))
                .await
                .unwrap_err();
            fail_syncs(&root, false);
            assert!(matches!(local_error(err), Error::UnableToSyncFile { .. }));
            assert!(cache.cached(std::path::Path::new("a")).is_none());
            assert_eq!(read(&store, "c/b/x").await, Bytes::from("1"));

            store.put(&Path::from("a/y"), "2".into()).await.unwrap();
            assert_eq!(read(&store, "a/y").await, Bytes::from("2"));
            assert!(matches!(
                store.get(&Path::from("c/y")).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
        }

        /// A directory that looks removed on every attempt (a filesystem reporting no links)
        /// does not keep a missing object's operation retrying.
        #[tokio::test]
        async fn fsync_forced_stale_handles_do_not_retry_forever() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            store.put(&Path::from("a/f"), "1".into()).await.unwrap();

            cache.force_stale(true);
            let err = store.delete(&Path::from("a/missing")).await.unwrap_err();
            cache.force_stale(false);
            assert!(matches!(err, crate::Error::NotFound { .. }), "{err}");

            store.delete(&Path::from("a/f")).await.unwrap();
            assert_eq!(fs::read_dir(tmp.path().join("a")).unwrap().count(), 0);
        }

        /// A store whose root sits under a directory it may traverse but not open fails every
        /// write, naming that directory, unless it opts out of fsyncing it; creating the root
        /// under it still has to.
        #[tokio::test]
        async fn fsync_root_under_an_unreadable_parent() {
            // SAFETY: `geteuid` has no preconditions.
            if unsafe { nix::libc::geteuid() } == 0 {
                return; // permissions do not apply
            }
            let tmp = TempDir::new().unwrap();
            let parent = tmp.path().join("p");
            let root = parent.join("store");
            fs::create_dir_all(&root).unwrap();
            let original_permissions = fs::metadata(&parent).unwrap().permissions();
            fs::set_permissions(&parent, fs::Permissions::from_mode(0o311)).unwrap();
            let strict = LocalFileSystem::new_with_prefix(&root)
                .unwrap()
                .with_fsync(true);
            // Set on a store sharing `strict`'s directory handles (`LocalFileSystem` is not
            // `Clone` in object_store 0.12.5): `strict` keeps its setting.
            let lenient = LocalFileSystem {
                config: Arc::clone(&strict.config),
                automatic_cleanup: strict.automatic_cleanup,
                fsync: strict.fsync,
                best_effort_root_parent_sync: strict.best_effort_root_parent_sync,
            }
            .with_best_effort_root_parent_sync(true);

            let mut refused = vec![
                strict.put(&Path::from("x"), "1".into()).await,
                strict.put(&Path::from("x"), "1".into()).await,
            ];
            let written = lenient.put(&Path::from("x"), "1".into()).await;
            let read = lenient.get(&Path::from("x")).await;
            // The lenient clone cached the root without the parent fsync; `strict` still owes it.
            refused.push(strict.put(&Path::from("z"), "3".into()).await);
            fs::remove_dir_all(&root).unwrap();
            let recreated = lenient.put(&Path::from("y"), "2".into()).await;
            fs::set_permissions(&parent, original_permissions).unwrap();

            for result in refused {
                let err = local_error(result.unwrap_err());
                let message = err.to_string();
                assert!(
                    message.contains("with_best_effort_root_parent_sync"),
                    "{message}"
                );
                match err {
                    Error::UnableToSyncRootParent { path, source } => {
                        assert_eq!(path, parent);
                        assert_eq!(source.kind(), ErrorKind::PermissionDenied);
                        assert_eq!(source.raw_os_error(), Some(nix::libc::EACCES));
                    }
                    e => panic!("expected the parent sync to fail, got {e}"),
                }
            }
            written.unwrap();
            assert_eq!(read.unwrap().bytes().await.unwrap(), Bytes::from("1"));
            match local_error(recreated.unwrap_err()) {
                Error::UnableToCreateDir { path, .. } => assert_eq!(path, root),
                e => panic!("expected the root creation to fail, got {e}"),
            }
            strict.put(&Path::from("y"), "2".into()).await.unwrap();
        }

        /// A root removed behind the cache is recreated by the next write, as the path-based
        /// code does.
        #[tokio::test]
        async fn fsync_write_recreates_a_removed_root() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            store.put(&Path::from("x"), "1".into()).await.unwrap();
            let old_root = cached(&store, "");
            fs::remove_dir_all(tmp.path()).unwrap();

            store.put(&Path::from("d/y"), "2".into()).await.unwrap();
            assert_eq!(read(&store, "d/y").await, Bytes::from("2"));
            assert!(!Arc::ptr_eq(&old_root, &cached(&store, "")));
            assert!(!cached(&store, "").is_dirty());
            assert!(matches!(
                store.get(&Path::from("x")).await.unwrap_err(),
                crate::Error::NotFound { .. }
            ));
        }

        /// Beyond its capacity the cache evicts clean handles and keeps dirty ones; writes into
        /// evicted directories reopen them.
        #[tokio::test]
        async fn fsync_cache_evicts_clean_handles_beyond_capacity() {
            let tmp = TempDir::new().unwrap();
            let store = fsync_store(&tmp);
            let cache = &store.config.dirs;
            let capacity = 4;
            cache.set_capacity(capacity);

            for i in 0..10 {
                let location = Path::from(format!("d{i}/f"));
                store.put(&location, "x".into()).await.unwrap();
            }
            assert!(cache.len() <= capacity, "{} handles cached", cache.len());
            assert!(cache.cached(std::path::Path::new("d0")).is_none());

            let dirty: Vec<_> = (7..10).map(|i| cached(&store, &format!("d{i}"))).collect();
            for dir in &dirty {
                dir.change(|_| Ok(())).unwrap();
            }
            for i in 10..20 {
                let location = Path::from(format!("d{i}/f"));
                store.put(&location, "x".into()).await.unwrap();
            }
            let len = cache.len();
            assert!(len <= capacity + dirty.len(), "{len} handles cached");
            for (i, dir) in (7..10).zip(&dirty) {
                assert!(Arc::ptr_eq(dir, &cached(&store, &format!("d{i}"))));
            }

            store.put(&Path::from("d0/g"), "y".into()).await.unwrap();
            assert_eq!(read(&store, "d0/g").await, Bytes::from("y"));
            assert_eq!(read(&store, "d0/f").await, Bytes::from("x"));
        }
    }

    /// The path-based code fsyncs the source directory of a create-mode rename after removing
    /// the source; a directory that cannot be opened fails that fsync.
    #[tokio::test]
    #[cfg(all(
        target_family = "unix",
        not(any(target_os = "linux", target_os = "android"))
    ))]
    async fn fsync_rename_if_not_exists_propagates_source_delete_sync_error() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path())
            .unwrap()
            .with_fsync(true);

        let source = Path::from("source_dir/source_file");
        let dest = Path::from("dest_dir/dest_file");
        integration.put(&source, "data".into()).await.unwrap();

        let source_dir = root.path().join("source_dir");
        let original_permissions = fs::metadata(&source_dir).unwrap().permissions();
        // Allow unlinking the file from the directory, but prevent opening the directory for
        // fsync.
        fs::set_permissions(&source_dir, fs::Permissions::from_mode(0o300)).unwrap();

        let result = integration.rename_if_not_exists(&source, &dest).await;
        fs::set_permissions(&source_dir, original_permissions).unwrap();

        match result {
            Err(crate::Error::Generic { source, .. }) => {
                assert!(source.to_string().contains("Unable to sync data to disk"));
            }
            _ => panic!("expected source parent fsync to fail"),
        }

        let read = integration.get(&dest).await.unwrap().bytes().await.unwrap();
        assert_eq!(read, Bytes::from("data"));
        assert!(matches!(
            integration.get(&source).await.unwrap_err(),
            crate::Error::NotFound { .. }
        ));
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn test_non_tokio() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        futures::executor::block_on(async move {
            put_get_delete_list(&integration).await;
            list_uses_directories_correctly(&integration).await;
            list_with_delimiter(&integration).await;

            // Can't use stream_get test as WriteMultipart uses a tokio JoinSet
            let p = Path::from("manual_upload");
            let mut upload = integration.put_multipart(&p).await.unwrap();
            upload.put_part("123".into()).await.unwrap();
            upload.put_part("45678".into()).await.unwrap();
            let r = upload.complete().await.unwrap();

            let get = integration.get(&p).await.unwrap();
            assert_eq!(get.meta.e_tag.as_ref().unwrap(), r.e_tag.as_ref().unwrap());
            let actual = get.bytes().await.unwrap();
            assert_eq!(actual.as_ref(), b"12345678");
        });
    }

    #[tokio::test]
    async fn creates_dir_if_not_present() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("nested/file/test_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, data);
    }

    #[tokio::test]
    async fn unknown_length() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&*read_data, data);
    }

    #[tokio::test]
    async fn range_request_start_beyond_end_of_file() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        integration
            .get_range(&location, 100..200)
            .await
            .expect_err("Should error with start range beyond end of file");
    }

    #[tokio::test]
    async fn range_request_beyond_end_of_file() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from("some_file");

        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration.get_range(&location, 0..100).await.unwrap();
        assert_eq!(&*read_data, data);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    // Fails on github actions runner (which runs the tests as root)
    #[ignore]
    async fn bubble_up_io_errors() {
        use std::{fs::set_permissions, os::unix::prelude::PermissionsExt};

        let root = TempDir::new().unwrap();

        // make non-readable
        let metadata = root.path().metadata().unwrap();
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o000);
        set_permissions(root.path(), permissions).unwrap();

        let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let mut stream = store.list(None);
        let mut any_err = false;
        while let Some(res) = stream.next().await {
            if res.is_err() {
                any_err = true;
            }
        }
        assert!(any_err);

        // `list_with_delimiter
        assert!(store.list_with_delimiter(None).await.is_err());
    }

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn get_nonexistent_location() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, source } = err {
            let source_variant = source.downcast_ref::<std::io::Error>();
            assert!(
                matches!(source_variant, Some(std::io::Error { .. }),),
                "got: {source_variant:?}"
            );
            assert!(path.ends_with(NON_EXISTENT_NAME), "{}", path);
        } else {
            panic!("unexpected error type: {err:?}");
        }
    }

    #[tokio::test]
    async fn root() {
        let integration = LocalFileSystem::new();

        let canonical = std::path::Path::new("Cargo.toml").canonicalize().unwrap();
        let url = Url::from_directory_path(&canonical).unwrap();
        let path = Path::parse(url.path()).unwrap();

        let roundtrip = integration.path_to_filesystem(&path).unwrap();

        // Needed as on Windows canonicalize returns extended length path syntax
        // C:\Users\circleci -> \\?\C:\Users\circleci
        let roundtrip = roundtrip.canonicalize().unwrap();

        assert_eq!(roundtrip, canonical);

        integration.head(&path).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn test_list_root() {
        let fs = LocalFileSystem::new();
        let r = fs.list_with_delimiter(None).await.unwrap_err().to_string();

        assert!(
            r.contains("Unable to convert URL \"file:///\" to filesystem path"),
            "{}",
            r
        );
    }

    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn test_list_root() {
        let fs = LocalFileSystem::new();
        fs.list_with_delimiter(None).await.unwrap();
    }

    #[cfg(target_family = "unix")]
    async fn check_list(integration: &LocalFileSystem, prefix: Option<&Path>, expected: &[&str]) {
        let result: Vec<_> = integration.list(prefix).try_collect().await.unwrap();

        let mut strings: Vec<_> = result.iter().map(|x| x.location.as_ref()).collect();
        strings.sort_unstable();
        assert_eq!(&strings, expected)
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn test_symlink() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let subdir = root.path().join("a");
        std::fs::create_dir(&subdir).unwrap();
        let file = subdir.join("file.parquet");
        std::fs::write(file, "test").unwrap();

        check_list(&integration, None, &["a/file.parquet"]).await;
        integration
            .head(&Path::from("a/file.parquet"))
            .await
            .unwrap();

        // Follow out of tree symlink
        let other = NamedTempFile::new().unwrap();
        std::os::unix::fs::symlink(other.path(), root.path().join("test.parquet")).unwrap();

        // Should return test.parquet even though out of tree
        check_list(&integration, None, &["a/file.parquet", "test.parquet"]).await;

        // Can fetch test.parquet
        integration.head(&Path::from("test.parquet")).await.unwrap();

        // Follow in tree symlink
        std::os::unix::fs::symlink(&subdir, root.path().join("b")).unwrap();
        check_list(
            &integration,
            None,
            &["a/file.parquet", "b/file.parquet", "test.parquet"],
        )
        .await;
        check_list(&integration, Some(&Path::from("b")), &["b/file.parquet"]).await;

        // Can fetch through symlink
        integration
            .head(&Path::from("b/file.parquet"))
            .await
            .unwrap();

        // Ignore broken symlink
        std::os::unix::fs::symlink(root.path().join("foo.parquet"), root.path().join("c")).unwrap();

        check_list(
            &integration,
            None,
            &["a/file.parquet", "b/file.parquet", "test.parquet"],
        )
        .await;

        let mut r = integration.list_with_delimiter(None).await.unwrap();
        r.common_prefixes.sort_unstable();
        assert_eq!(r.common_prefixes.len(), 2);
        assert_eq!(r.common_prefixes[0].as_ref(), "a");
        assert_eq!(r.common_prefixes[1].as_ref(), "b");
        assert_eq!(r.objects.len(), 1);
        assert_eq!(r.objects[0].location.as_ref(), "test.parquet");

        let r = integration
            .list_with_delimiter(Some(&Path::from("a")))
            .await
            .unwrap();
        assert_eq!(r.common_prefixes.len(), 0);
        assert_eq!(r.objects.len(), 1);
        assert_eq!(r.objects[0].location.as_ref(), "a/file.parquet");

        // Deleting a symlink doesn't delete the source file
        integration
            .delete(&Path::from("test.parquet"))
            .await
            .unwrap();
        assert!(other.path().exists());

        check_list(&integration, None, &["a/file.parquet", "b/file.parquet"]).await;

        // Deleting through a symlink deletes both files
        integration
            .delete(&Path::from("b/file.parquet"))
            .await
            .unwrap();

        check_list(&integration, None, &[]).await;

        // Adding a file through a symlink creates in both paths
        integration
            .put(&Path::from("b/file.parquet"), vec![0, 1, 2].into())
            .await
            .unwrap();

        check_list(&integration, None, &["a/file.parquet", "b/file.parquet"]).await;
    }

    #[tokio::test]
    async fn invalid_path() {
        let root = TempDir::new().unwrap();
        let root = root.path().join("🙀");
        std::fs::create_dir(root.clone()).unwrap();

        // Invalid paths supported above root of store
        let integration = LocalFileSystem::new_with_prefix(root.clone()).unwrap();

        let directory = Path::from("directory");
        let object = directory.child("child.txt");
        let data = Bytes::from("arbitrary");
        integration.put(&object, data.clone().into()).await.unwrap();
        integration.head(&object).await.unwrap();
        let result = integration.get(&object).await.unwrap();
        assert_eq!(result.bytes().await.unwrap(), data);

        flatten_list_stream(&integration, None).await.unwrap();
        flatten_list_stream(&integration, Some(&directory))
            .await
            .unwrap();

        let result = integration
            .list_with_delimiter(Some(&directory))
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert!(result.common_prefixes.is_empty());
        assert_eq!(result.objects[0].location, object);

        let emoji = root.join("💀");
        std::fs::write(emoji, "foo").unwrap();

        // Can list illegal file
        let mut paths = flatten_list_stream(&integration, None).await.unwrap();
        paths.sort_unstable();

        assert_eq!(
            paths,
            vec![
                Path::parse("directory/child.txt").unwrap(),
                Path::parse("💀").unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn list_hides_incomplete_uploads() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let location = Path::from("some_file");

        let data = PutPayload::from("arbitrary data");
        let mut u1 = integration.put_multipart(&location).await.unwrap();
        u1.put_part(data.clone()).await.unwrap();

        let mut u2 = integration.put_multipart(&location).await.unwrap();
        u2.put_part(data).await.unwrap();

        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list.len(), 0);

        assert_eq!(
            integration
                .list_with_delimiter(None)
                .await
                .unwrap()
                .objects
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn multipart_complete_failure_removes_staging_file() {
        for fsync in [false, true] {
            let root = TempDir::new().unwrap();
            let integration = LocalFileSystem::new_with_prefix(root.path())
                .unwrap()
                .with_fsync(fsync);
            let location = Path::from("some_file");
            let dest = integration.path_to_filesystem(&location).unwrap();
            let staging = staged_upload_path(&dest, "1");

            let mut upload = integration.put_multipart(&location).await.unwrap();
            upload
                .put_part(PutPayload::from("arbitrary data"))
                .await
                .unwrap();
            assert!(staging.exists());

            // The rename fails: the destination is now a non-empty directory.
            fs::create_dir_all(dest.join("child")).unwrap();
            upload.complete().await.unwrap_err();
            assert!(!staging.exists(), "fsync: {fsync}");
        }
    }

    #[tokio::test]
    async fn put_failure_removes_staging_file() {
        for fsync in [false, true] {
            let root = TempDir::new().unwrap();
            let integration = LocalFileSystem::new_with_prefix(root.path())
                .unwrap()
                .with_fsync(fsync);
            let payload = PutPayload::from("arbitrary data");

            // Overwrite: the rename fails on a non-empty directory; that is not "already exists".
            let location = Path::from("dir");
            let dest = integration.path_to_filesystem(&location).unwrap();
            fs::create_dir_all(dest.join("child")).unwrap();
            let err = integration
                .put(&location, payload.clone())
                .await
                .unwrap_err();
            assert!(!matches!(err, crate::Error::AlreadyExists { .. }), "{err}");
            assert_eq!(
                fs::read_dir(root.path()).unwrap().count(),
                1,
                "fsync: {fsync}"
            );

            // Create: the hard link fails on an existing file.
            let location = Path::from("file");
            integration.put(&location, payload.clone()).await.unwrap();
            let err = integration
                .put_opts(&location, payload, PutMode::Create.into())
                .await
                .unwrap_err();
            assert!(matches!(err, crate::Error::AlreadyExists { .. }), "{err}");
            assert_eq!(
                fs::read_dir(root.path()).unwrap().count(),
                2,
                "fsync: {fsync}"
            );
        }
    }

    #[tokio::test]
    async fn test_path_with_offset() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let root_path = root.path();
        for i in 0..5 {
            let filename = format!("test{i}.parquet");
            let file = root_path.join(filename);
            std::fs::write(file, "test").unwrap();
        }
        let filter_str = "test";
        let filter = String::from(filter_str);
        let offset_str = filter + "1";
        let offset = Path::from(offset_str.clone());

        // Use list_with_offset to retrieve files
        let res = integration.list_with_offset(None, &offset);
        let offset_paths: Vec<_> = res.map_ok(|x| x.location).try_collect().await.unwrap();
        let mut offset_files: Vec<_> = offset_paths
            .iter()
            .map(|x| String::from(x.filename().unwrap()))
            .collect();

        // Check result with direct filesystem read
        let files = fs::read_dir(root_path).unwrap();
        let filtered_files = files
            .filter_map(Result::ok)
            .filter_map(|d| {
                d.file_name().to_str().and_then(|f| {
                    if f.contains(filter_str) {
                        Some(String::from(f))
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut expected_offset_files: Vec<_> = filtered_files
            .iter()
            .filter(|s| **s > offset_str)
            .cloned()
            .collect();

        fn do_vecs_match<T: PartialEq>(a: &[T], b: &[T]) -> bool {
            let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
            matching == a.len() && matching == b.len()
        }

        offset_files.sort();
        expected_offset_files.sort();

        // println!("Expected Offset Files: {:?}", expected_offset_files);
        // println!("Actual Offset Files: {:?}", offset_files);

        assert_eq!(offset_files.len(), expected_offset_files.len());
        assert!(do_vecs_match(&expected_offset_files, &offset_files));
    }

    #[tokio::test]
    async fn filesystem_filename_with_percent() {
        let temp_dir = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let filename = "L%3ABC.parquet";

        std::fs::write(temp_dir.path().join(filename), "foo").unwrap();

        let res: Vec<_> = integration.list(None).try_collect().await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].location.as_ref(), filename);

        let res = integration.list_with_delimiter(None).await.unwrap();
        assert_eq!(res.objects.len(), 1);
        assert_eq!(res.objects[0].location.as_ref(), filename);
    }

    #[tokio::test]
    async fn relative_paths() {
        LocalFileSystem::new_with_prefix(".").unwrap();
        LocalFileSystem::new_with_prefix("..").unwrap();
        LocalFileSystem::new_with_prefix("../..").unwrap();

        let integration = LocalFileSystem::new();
        let path = Path::from_filesystem_path(".").unwrap();
        integration.list_with_delimiter(Some(&path)).await.unwrap();
    }

    #[test]
    fn test_valid_path() {
        let cases = [
            ("foo#123/test.txt", true),
            ("foo#123/test#23.txt", true),
            ("foo#123/test#34", false),
            ("foo😁/test#34", false),
            ("foo/test#😁34", true),
        ];

        for (case, expected) in cases {
            let path = Path::parse(case).unwrap();
            assert_eq!(is_valid_file_path(&path), expected);
        }
    }

    #[tokio::test]
    async fn test_intermediate_files() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();

        let a = Path::parse("foo#123/test.txt").unwrap();
        integration.put(&a, "test".into()).await.unwrap();

        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list, vec![a.clone()]);

        std::fs::write(root.path().join("bar#123"), "test").unwrap();

        // Should ignore file
        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list, vec![a.clone()]);

        let b = Path::parse("bar#123").unwrap();
        let err = integration.get(&b).await.unwrap_err().to_string();
        assert_eq!(err, "Generic LocalFileSystem error: Filenames containing trailing '/#\\d+/' are not supported: bar#123");

        let c = Path::parse("foo#123.txt").unwrap();
        integration.put(&c, "test".into()).await.unwrap();

        let mut list = flatten_list_stream(&integration, None).await.unwrap();
        list.sort_unstable();
        assert_eq!(list, vec![c, a]);
    }

    #[tokio::test]
    #[cfg(target_os = "windows")]
    async fn filesystem_filename_with_colon() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let path = Path::parse("file%3Aname.parquet").unwrap();
        let location = Path::parse("file:name.parquet").unwrap();

        integration.put(&location, "test".into()).await.unwrap();
        let list = flatten_list_stream(&integration, None).await.unwrap();
        assert_eq!(list, vec![path.clone()]);

        let result = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(result, Bytes::from("test"));
    }

    #[tokio::test]
    async fn delete_dirs_automatically() {
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path())
            .unwrap()
            .with_automatic_cleanup(true);
        let location = Path::from("nested/file/test_file");
        let data = Bytes::from("arbitrary data");

        integration
            .put(&location, data.clone().into())
            .await
            .unwrap();

        let read_data = integration
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(&*read_data, data);
        assert!(fs::read_dir(root.path()).unwrap().count() > 0);
        integration.delete(&location).await.unwrap();
        assert!(fs::read_dir(root.path()).unwrap().count() == 0);
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn test_close_file_detects_error_unix() {
        // A descriptor at or above the open-file limit can never be open, so closing
        // it fails without any risk of closing a descriptor another test is using.
        let mut limit = nix::libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid, writable rlimit.
        assert_eq!(
            unsafe { nix::libc::getrlimit(nix::libc::RLIMIT_NOFILE, &mut limit) },
            0
        );
        let never_open = std::os::unix::io::RawFd::try_from(limit.rlim_cur)
            .unwrap_or(std::os::unix::io::RawFd::MAX);

        let err = super::close_raw_fd(never_open).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(nix::libc::EBADF), "got: {err:?}");

        // A real file still closes cleanly.
        super::close_file(tempfile::tempfile().unwrap()).unwrap();
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn test_close_file_detects_error_windows() {
        // A null handle is never valid, so closing it fails without any risk of
        // closing a handle another test is using.
        let err = super::close_raw_handle(std::ptr::null_mut()).unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE as i32),
            "got: {err:?}"
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod not_wasm_tests {
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path, PutPayload};

    #[tokio::test]
    async fn test_cleanup_intermediate_files() {
        'stores: for fsync in [false, true] {
            let root = TempDir::new().unwrap();
            let integration = LocalFileSystem::new_with_prefix(root.path())
                .unwrap()
                .with_fsync(fsync);

            let location = Path::from("some_file");
            let data = PutPayload::from_static(b"hello");
            let mut upload = integration.put_multipart(&location).await.unwrap();
            upload.put_part(data).await.unwrap();

            let file_count = std::fs::read_dir(root.path()).unwrap().count();
            assert_eq!(file_count, 1);
            drop(upload);

            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let file_count = std::fs::read_dir(root.path()).unwrap().count();
                if file_count == 0 {
                    continue 'stores;
                }
            }
            panic!("Failed to cleanup file in 100ms (fsync: {fsync})")
        }
    }
}

#[cfg(target_family = "unix")]
#[cfg(test)]
mod unix_test {
    use std::fs::OpenOptions;

    use nix::sys::stat;
    use nix::unistd;
    use tempfile::TempDir;

    use crate::local::LocalFileSystem;
    use crate::{ObjectStore, Path};

    #[tokio::test]
    async fn test_fifo() {
        let filename = "some_file";
        let root = TempDir::new().unwrap();
        let integration = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let path = root.path().join(filename);
        unistd::mkfifo(&path, stat::Mode::S_IRWXU).unwrap();

        // Need to open read and write side in parallel
        let spawned =
            tokio::task::spawn_blocking(|| OpenOptions::new().write(true).open(path).unwrap());

        let location = Path::from(filename);
        integration.head(&location).await.unwrap();
        integration.get(&location).await.unwrap();

        spawned.await.unwrap();
    }
}
