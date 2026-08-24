use std::{
    collections::HashSet,
    ffi::{CStr, CString, OsStr, OsString},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
        unix::ffi::{OsStrExt, OsStringExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use uuid::Uuid;

use super::{StoreError, StorePaths};

#[derive(Clone)]
pub(crate) struct SecureDir {
    descriptor: Arc<OwnedFd>,
    display_path: PathBuf,
}

impl std::fmt::Debug for SecureDir {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecureDir")
            .field("display_path", &self.display_path)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StoreHandles {
    pub config_root: SecureDir,
    pub settings: SecureDir,
    pub presets: SecureDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationBoundary {
    PartialWritten,
    FileSyncStarted,
    FileSynced,
    Renamed,
    DirectorySyncStarted,
    DirectorySynced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SecureFileSnapshot {
    bytes: Vec<u8>,
    device: u64,
    inode: u64,
    size: i64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

pub(crate) enum NoReplacePublication {
    NotPublished(StoreError),
    Published(SecureFileSnapshot),
    PublishedWithError {
        snapshot: Option<SecureFileSnapshot>,
        error: StoreError,
    },
}

pub(crate) enum SecureEntry {
    Regular(Vec<u8>),
    Directory(SecureDir),
    Symlink(PathBuf),
}

impl StoreHandles {
    pub fn open(paths: &StorePaths, create: bool) -> Result<Self, StoreError> {
        let config_root = SecureDir::open_writable(paths.config_root(), create)?;
        let state_root = SecureDir::open_writable(paths.state_root(), create)?;
        Ok(Self {
            config_root: config_root.clone(),
            settings: config_root.child_writable(OsStr::new("sleepy"), create)?,
            presets: state_root.child_writable(OsStr::new("sleepy"), create)?,
        })
    }
}

impl SecureDir {
    pub fn open_writable(path: &Path, create: bool) -> Result<Self, StoreError> {
        if !path.is_absolute() {
            return Err(StoreError::unsafe_path(path.display()));
        }
        let root = CString::new("/").expect("static root has no NUL");
        let mut descriptor = openat_owned(
            libc::AT_FDCWD,
            &root,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
        .map_err(StoreError::io)?;
        let mut opened = PathBuf::from("/");
        let components = path.components().collect::<Vec<_>>();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(component) = component else {
                if matches!(component, Component::RootDir) {
                    continue;
                }
                return Err(StoreError::unsafe_path(path.display()));
            };
            opened.push(component);
            let name = c_string(component, &opened)?;
            let final_component = index + 1 == components.len();
            descriptor = match openat_owned(
                descriptor.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
            ) {
                Ok(descriptor) => descriptor,
                Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
                    mkdirat(descriptor.as_raw_fd(), &name, 0o700)
                        .map_err(|error| map_open_error(&opened, error))?;
                    openat_owned(
                        descriptor.as_raw_fd(),
                        &name,
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                        0,
                    )
                    .map_err(|error| map_open_error(&opened, error))?
                }
                Err(error) => return Err(map_open_error(&opened, error)),
            };
            if final_component {
                validate_writable_directory(descriptor.as_raw_fd(), &opened)?;
            }
        }
        validate_writable_directory(descriptor.as_raw_fd(), path)?;
        Ok(Self {
            descriptor: Arc::new(descriptor),
            display_path: path.to_owned(),
        })
    }

    pub fn child_writable(&self, name: &OsStr, create: bool) -> Result<Self, StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let descriptor = match openat_owned(
            self.as_raw_fd(),
            &name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
                mkdirat(self.as_raw_fd(), &name, 0o700)
                    .map_err(|error| map_open_error(&path, error))?;
                openat_owned(
                    self.as_raw_fd(),
                    &name,
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    0,
                )
                .map_err(|error| map_open_error(&path, error))?
            }
            Err(error) => return Err(map_open_error(&path, error)),
        };
        validate_writable_directory(descriptor.as_raw_fd(), &path)?;
        Ok(Self {
            descriptor: Arc::new(descriptor),
            display_path: path,
        })
    }

    pub fn read_optional(&self, name: &OsStr) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(file) = self.open_regular_optional(name, libc::O_RDONLY | libc::O_NONBLOCK)?
        else {
            return Ok(None);
        };
        let mut file = File::from(file);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(StoreError::io)?;
        Ok(Some(bytes))
    }

    pub fn read(&self, name: &OsStr) -> Result<Vec<u8>, StoreError> {
        self.read_optional(name)?.ok_or_else(|| {
            StoreError::io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} does not exist", self.display_path.join(name).display()),
            ))
        })
    }

    pub fn snapshot_regular(&self, name: &OsStr) -> Result<Option<SecureFileSnapshot>, StoreError> {
        let Some(file) = self.open_regular_optional(name, libc::O_RDONLY | libc::O_NONBLOCK)?
        else {
            return Ok(None);
        };
        let before = fstat(file.as_raw_fd()).map_err(StoreError::io)?;
        let mut file = File::from(file);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(StoreError::io)?;
        let after = fstat(file.as_raw_fd()).map_err(StoreError::io)?;
        let before = snapshot_metadata(before);
        let after = snapshot_metadata(after);
        if before != after {
            return Err(StoreError::conflict(format!(
                "{} changed while it was being snapshotted",
                self.display_path.join(name).display()
            )));
        }
        Ok(Some(SecureFileSnapshot { bytes, ..before }))
    }

    pub fn snapshot_matches(
        &self,
        name: &OsStr,
        expected: &SecureFileSnapshot,
    ) -> Result<bool, StoreError> {
        Ok(self.snapshot_regular(name)?.as_ref() == Some(expected))
    }

    pub fn exists(&self, name: &OsStr) -> Result<bool, StoreError> {
        match self.open_regular_optional(name, libc::O_RDONLY | libc::O_NONBLOCK)? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn open_lock(&self, name: &OsStr) -> Result<File, StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let descriptor = openat_owned(
            self.as_raw_fd(),
            &name,
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
        .map_err(|error| map_open_error(&path, error))?;
        validate_regular_file(descriptor.as_raw_fd(), &path)?;
        Ok(File::from(descriptor))
    }

    pub fn atomic_replace(
        &self,
        name: &OsStr,
        bytes: &[u8],
        mut after_file_sync: impl FnMut() -> Result<(), StoreError>,
        mut after_rename: impl FnMut() -> Result<(), StoreError>,
        mut after_directory_sync: impl FnMut() -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.validate_file_if_present(name)?;
        let temporary_name = format!(
            ".{}.{}.tmp",
            name.to_string_lossy(),
            Uuid::new_v4().hyphenated()
        );
        let temporary = OsStr::new(&temporary_name);
        if let Err(error) = self.write_new(temporary, bytes) {
            let _ = self.remove_file(temporary);
            return Err(error);
        }
        if let Err(error) = after_file_sync() {
            let _ = self.remove_file(temporary);
            return Err(error);
        }
        self.rename(temporary, name)?;
        after_rename()?;
        self.sync().map_err(StoreError::commit_state_unknown)?;
        after_directory_sync()
    }

    pub fn write_new(&self, name: &OsStr, bytes: &[u8]) -> Result<(), StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let descriptor = openat_owned(
            self.as_raw_fd(),
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
        .map_err(|error| map_open_error(&path, error))?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(StoreError::io)
    }

    pub fn publish_new(
        &self,
        temporary: &OsStr,
        destination: &OsStr,
        bytes: &[u8],
        observe: impl FnMut(PublicationBoundary) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.publish_new_inner(temporary, destination, bytes, false, observe)
    }

    pub fn publish_new_no_replace(
        &self,
        temporary: &OsStr,
        destination: &OsStr,
        bytes: &[u8],
        mut observe: impl FnMut(PublicationBoundary) -> Result<(), StoreError>,
    ) -> NoReplacePublication {
        if let Err(error) = self.exists(destination).and_then(|exists| {
            if exists {
                Err(StoreError::conflict(format!(
                    "{} already exists",
                    self.display_path.join(destination).display()
                )))
            } else {
                Ok(())
            }
        }) {
            return NoReplacePublication::NotPublished(error);
        }
        let path = self.display_path.join(temporary);
        let temporary_c = match c_string(temporary, &path) {
            Ok(temporary) => temporary,
            Err(error) => return NoReplacePublication::NotPublished(error),
        };
        let descriptor = match openat_owned(
            self.as_raw_fd(),
            &temporary_c,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        ) {
            Ok(descriptor) => descriptor,
            Err(error) => return NoReplacePublication::NotPublished(map_open_error(&path, error)),
        };
        let mut file = File::from(descriptor);
        let split = if bytes.is_empty() {
            0
        } else {
            (bytes.len() / 2).max(1)
        };
        let before_rename = (|| {
            file.write_all(&bytes[..split]).map_err(StoreError::io)?;
            observe(PublicationBoundary::PartialWritten)?;
            file.write_all(&bytes[split..]).map_err(StoreError::io)?;
            observe(PublicationBoundary::FileSyncStarted)?;
            file.sync_all().map_err(StoreError::io)?;
            observe(PublicationBoundary::FileSynced)?;
            self.rename_no_replace(temporary, destination)
        })();
        if let Err(error) = before_rename {
            drop(file);
            let _ = self.remove_file(temporary);
            return NoReplacePublication::NotPublished(error);
        }
        let snapshot = snapshot_from_descriptor(file.as_raw_fd(), bytes.to_vec()).ok();
        let after_rename = (|| {
            observe(PublicationBoundary::Renamed)?;
            observe(PublicationBoundary::DirectorySyncStarted)?;
            self.sync().map_err(StoreError::commit_state_unknown)?;
            observe(PublicationBoundary::DirectorySynced)
        })();
        match after_rename {
            Ok(()) => match snapshot_from_descriptor(file.as_raw_fd(), bytes.to_vec()) {
                Ok(snapshot) => NoReplacePublication::Published(snapshot),
                Err(error) => NoReplacePublication::PublishedWithError {
                    snapshot,
                    error: StoreError::io(error),
                },
            },
            Err(error) => NoReplacePublication::PublishedWithError { snapshot, error },
        }
    }

    fn publish_new_inner(
        &self,
        temporary: &OsStr,
        destination: &OsStr,
        bytes: &[u8],
        no_replace: bool,
        mut observe: impl FnMut(PublicationBoundary) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        if self.exists(destination)? {
            return Err(StoreError::conflict(format!(
                "{} already exists",
                self.display_path.join(destination).display()
            )));
        }
        let path = self.display_path.join(temporary);
        let temporary_c = c_string(temporary, &path)?;
        let descriptor = openat_owned(
            self.as_raw_fd(),
            &temporary_c,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
        .map_err(|error| map_open_error(&path, error))?;
        let mut published = false;
        let result = (|| {
            let mut file = File::from(descriptor);
            let split = if bytes.is_empty() {
                0
            } else {
                (bytes.len() / 2).max(1)
            };
            file.write_all(&bytes[..split]).map_err(StoreError::io)?;
            observe(PublicationBoundary::PartialWritten)?;
            file.write_all(&bytes[split..]).map_err(StoreError::io)?;
            observe(PublicationBoundary::FileSyncStarted)?;
            file.sync_all().map_err(StoreError::io)?;
            observe(PublicationBoundary::FileSynced)?;
            drop(file);
            if no_replace {
                self.rename_no_replace(temporary, destination)?;
            } else {
                self.rename(temporary, destination)?;
            }
            published = true;
            observe(PublicationBoundary::Renamed)?;
            observe(PublicationBoundary::DirectorySyncStarted)?;
            self.sync().map_err(StoreError::commit_state_unknown)?;
            observe(PublicationBoundary::DirectorySynced)
        })();
        if result.is_err() && !published {
            let _ = self.remove_file(temporary);
        }
        result
    }

    pub fn rename(&self, source: &OsStr, destination: &OsStr) -> Result<(), StoreError> {
        let source_path = self.display_path.join(source);
        let destination_path = self.display_path.join(destination);
        let source = c_string(source, &source_path)?;
        let destination = c_string(destination, &destination_path)?;
        let result = unsafe {
            libc::renameat(
                self.as_raw_fd(),
                source.as_ptr(),
                self.as_raw_fd(),
                destination.as_ptr(),
            )
        };
        cvt(result).map_err(StoreError::io)
    }

    fn rename_no_replace(&self, source: &OsStr, destination: &OsStr) -> Result<(), StoreError> {
        let source_path = self.display_path.join(source);
        let destination_path = self.display_path.join(destination);
        let source = c_string(source, &source_path)?;
        let destination = c_string(destination, &destination_path)?;
        #[cfg(target_os = "linux")]
        {
            let result = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    self.as_raw_fd(),
                    source.as_ptr(),
                    self.as_raw_fd(),
                    destination.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            if result == 0 {
                Ok(())
            } else {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::AlreadyExists {
                    Err(StoreError::conflict(format!(
                        "{} already exists",
                        destination_path.display()
                    )))
                } else {
                    Err(StoreError::io(error))
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (source, destination);
            Err(StoreError::unsupported(
                "atomic no-replace publication requires renameat2",
            ))
        }
    }

    pub fn remove_file(&self, name: &OsStr) -> Result<(), StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let result = unsafe { libc::unlinkat(self.as_raw_fd(), name.as_ptr(), 0) };
        match cvt(result) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StoreError::io(error)),
        }
    }

    pub fn remove_dir(&self, name: &OsStr) -> Result<(), StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let result = unsafe { libc::unlinkat(self.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
        match cvt(result) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StoreError::io(error)),
        }
    }

    pub fn entries(&self) -> Result<Vec<OsString>, StoreError> {
        let current = CString::new(".").expect("static current directory has no NUL");
        let descriptor = openat_owned(
            self.as_raw_fd(),
            &current,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
        .map_err(StoreError::io)?;
        let descriptor = descriptor.into_raw_fd();
        let stream = unsafe { libc::fdopendir(descriptor) };
        if stream.is_null() {
            unsafe { libc::close(descriptor) };
            return Err(StoreError::io(io::Error::last_os_error()));
        }
        let mut entries = Vec::new();
        loop {
            unsafe { *libc::__errno_location() = 0 };
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let error = io::Error::last_os_error();
                unsafe { libc::closedir(stream) };
                if error.raw_os_error() == Some(0) {
                    break;
                }
                return Err(StoreError::io(error));
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name.to_bytes() != b"." && name.to_bytes() != b".." {
                entries.push(OsStr::from_bytes(name.to_bytes()).to_owned());
            }
        }
        entries.sort();
        Ok(entries)
    }

    pub fn open_entry(&self, name: &OsStr) -> Result<SecureEntry, StoreError> {
        let path = self.display_path.join(name);
        let name_c = c_string(name, &path)?;
        let descriptor = match openat_owned(
            self.as_raw_fd(),
            &name_c,
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                let descriptor = openat_owned(
                    self.as_raw_fd(),
                    &name_c,
                    libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    0,
                )
                .map_err(|error| map_open_error(&path, error))?;
                let metadata = fstat(descriptor.as_raw_fd()).map_err(StoreError::io)?;
                if metadata.st_mode & libc::S_IFMT != libc::S_IFLNK {
                    return Err(StoreError::unsafe_path(path.display()));
                }
                return readlink_fd(descriptor.as_raw_fd()).map(SecureEntry::Symlink);
            }
            Err(error) => return Err(map_open_error(&path, error)),
        };
        let metadata = fstat(descriptor.as_raw_fd()).map_err(StoreError::io)?;
        match metadata.st_mode & libc::S_IFMT {
            libc::S_IFREG => {
                let mut file = File::from(descriptor);
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).map_err(StoreError::io)?;
                Ok(SecureEntry::Regular(bytes))
            }
            libc::S_IFDIR => {
                validate_writable_directory(descriptor.as_raw_fd(), &path)?;
                Ok(SecureEntry::Directory(Self {
                    descriptor: Arc::new(descriptor),
                    display_path: path,
                }))
            }
            _ => Err(StoreError::unsafe_path(path.display())),
        }
    }

    pub fn read_root_store_regular(path: &Path) -> Result<Vec<u8>, StoreError> {
        read_store_regular_for_policy(path, Path::new("/nix/store"), 0, 40)
    }

    pub fn sync(&self) -> Result<(), StoreError> {
        cvt(unsafe { libc::fsync(self.as_raw_fd()) }).map_err(StoreError::io)
    }

    pub fn validate_file_if_present(&self, name: &OsStr) -> Result<(), StoreError> {
        self.open_regular_optional(name, libc::O_RDONLY | libc::O_NONBLOCK)
            .map(|_| ())
    }

    pub fn proc_path(&self) -> PathBuf {
        PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            self.as_raw_fd()
        ))
    }

    pub fn as_raw_fd(&self) -> i32 {
        self.descriptor.as_raw_fd()
    }

    fn open_regular_optional(
        &self,
        name: &OsStr,
        access_flags: i32,
    ) -> Result<Option<OwnedFd>, StoreError> {
        let path = self.display_path.join(name);
        let name = c_string(name, &path)?;
        let descriptor = match openat_owned(
            self.as_raw_fd(),
            &name,
            access_flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(map_open_error(&path, error)),
        };
        validate_regular_file(descriptor.as_raw_fd(), &path)?;
        Ok(Some(descriptor))
    }
}

fn read_store_regular_for_policy(
    initial_target: &Path,
    store_root: &Path,
    required_owner: libc::uid_t,
    max_symlink_hops: usize,
) -> Result<Vec<u8>, StoreError> {
    if !initial_target.is_absolute() {
        return Err(StoreError::unsafe_path(initial_target.display()));
    }
    let mut pending = normalize_store_target(initial_target, store_root, &[])?;
    let store_descriptor = open_directory_path_no_follow(store_root)?;
    let mut seen = HashSet::new();
    let mut followed = 0_usize;

    'resolve: loop {
        if pending.is_empty() || !seen.insert(pending.clone()) {
            return Err(StoreError::unsafe_path(initial_target.display()));
        }
        let mut directory: Option<OwnedFd> = None;
        for (index, component) in pending.iter().enumerate() {
            let parent_descriptor = directory
                .as_ref()
                .map(AsRawFd::as_raw_fd)
                .unwrap_or_else(|| store_descriptor.as_raw_fd());
            let name = c_string(component, initial_target)?;
            let final_component = index + 1 == pending.len();
            let descriptor = if final_component {
                match openat_owned(
                    parent_descriptor,
                    &name,
                    libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    0,
                ) {
                    Ok(descriptor) => descriptor,
                    Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                        open_path_entry(parent_descriptor, &name, initial_target)?
                    }
                    Err(error) => return Err(map_open_error(initial_target, error)),
                }
            } else {
                open_path_entry(parent_descriptor, &name, initial_target)?
            };
            let metadata = fstat(descriptor.as_raw_fd()).map_err(StoreError::io)?;
            match metadata.st_mode & libc::S_IFMT {
                libc::S_IFLNK => {
                    followed += 1;
                    if followed > max_symlink_hops {
                        return Err(StoreError::unsafe_path(initial_target.display()));
                    }
                    let target = readlink_fd(descriptor.as_raw_fd())?;
                    let mut resolved =
                        normalize_store_target(&target, store_root, &pending[..index])?;
                    resolved.extend_from_slice(&pending[index + 1..]);
                    pending = resolved;
                    continue 'resolve;
                }
                libc::S_IFDIR if !final_component => directory = Some(descriptor),
                libc::S_IFREG if final_component && metadata.st_uid == required_owner => {
                    let mut file = File::from(descriptor);
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes).map_err(StoreError::io)?;
                    return Ok(bytes);
                }
                _ => return Err(StoreError::unsafe_path(initial_target.display())),
            }
        }
    }
}

fn open_path_entry(
    directory: i32,
    name: &CString,
    display_path: &Path,
) -> Result<OwnedFd, StoreError> {
    openat_owned(
        directory,
        name,
        libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )
    .map_err(|error| map_open_error(display_path, error))
}

fn open_directory_path_no_follow(path: &Path) -> Result<OwnedFd, StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::unsafe_path(path.display()));
    }
    let root = CString::new("/").expect("static root has no NUL");
    let mut descriptor = openat_owned(
        libc::AT_FDCWD,
        &root,
        libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )
    .map_err(StoreError::io)?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(component) => {
                let name = c_string(component, path)?;
                descriptor = openat_owned(
                    descriptor.as_raw_fd(),
                    &name,
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    0,
                )
                .map_err(|error| map_open_error(path, error))?;
            }
            _ => return Err(StoreError::unsafe_path(path.display())),
        }
    }
    Ok(descriptor)
}

fn normalize_store_target(
    target: &Path,
    store_root: &Path,
    relative_parent: &[OsString],
) -> Result<Vec<OsString>, StoreError> {
    let (mut resolved, components) = if target.is_absolute() {
        let relative = target
            .strip_prefix(store_root)
            .map_err(|_| StoreError::unsafe_path(target.display()))?;
        (Vec::new(), relative.components())
    } else {
        (relative_parent.to_vec(), target.components())
    };
    for component in components {
        match component {
            Component::CurDir => {}
            Component::Normal(component) => resolved.push(component.to_owned()),
            Component::ParentDir => {
                if resolved.pop().is_none() {
                    return Err(StoreError::unsafe_path(target.display()));
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(StoreError::unsafe_path(target.display()));
            }
        }
    }
    Ok(resolved)
}

fn validate_writable_directory(descriptor: i32, path: &Path) -> Result<(), StoreError> {
    let metadata = fstat(descriptor).map_err(StoreError::io)?;
    if metadata.st_mode & libc::S_IFMT != libc::S_IFDIR
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o022 != 0
    {
        return Err(StoreError::unsafe_path(path.display()));
    }
    Ok(())
}

fn validate_regular_file(descriptor: i32, path: &Path) -> Result<(), StoreError> {
    let metadata = fstat(descriptor).map_err(StoreError::io)?;
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o133 != 0
    {
        return Err(StoreError::unsafe_path(path.display()));
    }
    Ok(())
}

fn snapshot_metadata(metadata: libc::stat) -> SecureFileSnapshot {
    SecureFileSnapshot {
        bytes: Vec::new(),
        device: metadata.st_dev,
        inode: metadata.st_ino,
        size: metadata.st_size,
        modified_seconds: metadata.st_mtime,
        modified_nanoseconds: metadata.st_mtime_nsec,
        changed_seconds: metadata.st_ctime,
        changed_nanoseconds: metadata.st_ctime_nsec,
    }
}

fn snapshot_from_descriptor(descriptor: i32, bytes: Vec<u8>) -> io::Result<SecureFileSnapshot> {
    Ok(SecureFileSnapshot {
        bytes,
        ..snapshot_metadata(fstat(descriptor)?)
    })
}

fn fstat(descriptor: i32) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    cvt(unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) })?;
    Ok(unsafe { metadata.assume_init() })
}

fn readlink_fd(descriptor: i32) -> Result<PathBuf, StoreError> {
    let empty = CString::new("").expect("static empty string has no NUL");
    let mut capacity = 256;
    loop {
        let mut buffer = vec![0_u8; capacity];
        let length = unsafe {
            libc::readlinkat(
                descriptor,
                empty.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if length < 0 {
            return Err(StoreError::io(io::Error::last_os_error()));
        }
        let length = length as usize;
        if length < buffer.len() {
            buffer.truncate(length);
            return Ok(PathBuf::from(OsString::from_vec(buffer)));
        }
        capacity *= 2;
        if capacity > 1024 * 1024 {
            return Err(StoreError::unsafe_path("oversized symlink target"));
        }
    }
}

fn openat_owned(directory: i32, name: &CString, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    let descriptor = unsafe { libc::openat(directory, name.as_ptr(), flags, mode) };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
    }
}

fn mkdirat(directory: i32, name: &CString, mode: u32) -> io::Result<()> {
    cvt(unsafe { libc::mkdirat(directory, name.as_ptr(), mode) })
}

fn cvt(result: i32) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn c_string(component: &OsStr, path: &Path) -> Result<CString, StoreError> {
    let bytes = component.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(StoreError::unsafe_path(path.display()));
    }
    CString::new(bytes).map_err(|_| StoreError::unsafe_path(path.display()))
}

fn map_open_error(path: &Path, error: io::Error) -> StoreError {
    if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
        StoreError::unsafe_path(path.display())
    } else {
        StoreError::io(error)
    }
}

#[cfg(test)]
mod static_store_resolution_tests {
    use std::{
        fs,
        os::unix::fs::symlink,
        time::{Duration, Instant},
    };

    use tempfile::TempDir;

    use super::*;

    fn read_test_store(target: &Path, store_root: &Path) -> Result<Vec<u8>, StoreError> {
        read_store_regular_for_policy(target, store_root, unsafe { libc::geteuid() }, 40)
    }

    #[test]
    fn directory_entries_can_be_enumerated_repeatedly() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("first"), b"first").unwrap();
        fs::write(temp.path().join("second"), b"second").unwrap();
        let directory = SecureDir::open_writable(temp.path(), false).unwrap();
        let expected = vec![OsString::from("first"), OsString::from("second")];

        assert_eq!(directory.entries().unwrap(), expected);
        assert_eq!(directory.entries().unwrap(), expected);
    }

    #[test]
    fn home_absolute_link_resolves_store_symlink_to_owned_regular() {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("store");
        let package = store.join("package");
        fs::create_dir_all(&package).unwrap();
        fs::write(package.join("config.kdl"), b"approved\n").unwrap();
        symlink(package.join("config.kdl"), store.join("profile.kdl")).unwrap();
        let home_link = temp.path().join("home-config.kdl");
        symlink(store.join("profile.kdl"), &home_link).unwrap();

        let bytes = read_test_store(&fs::read_link(home_link).unwrap(), &store).unwrap();

        assert_eq!(bytes, b"approved\n");
    }

    #[test]
    fn relative_store_symlink_chain_is_resolved_inside_store() {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("store");
        fs::create_dir_all(store.join("links/nested")).unwrap();
        fs::create_dir_all(store.join("package")).unwrap();
        fs::write(store.join("package/config.kdl"), b"relative-chain\n").unwrap();
        symlink("nested/second.kdl", store.join("links/first.kdl")).unwrap();
        symlink(
            "../../package/config.kdl",
            store.join("links/nested/second.kdl"),
        )
        .unwrap();

        let bytes = read_test_store(&store.join("links/first.kdl"), &store).unwrap();

        assert_eq!(bytes, b"relative-chain\n");
    }

    #[test]
    fn static_store_symlink_escape_is_rejected() {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("store");
        fs::create_dir_all(store.join("links")).unwrap();
        let outside = temp.path().join("outside.kdl");
        let outside_directory = temp.path().join("outside-directory");
        fs::create_dir_all(&outside_directory).unwrap();
        fs::write(
            outside_directory.join("config.kdl"),
            b"component-attacker\n",
        )
        .unwrap();
        fs::write(&outside, b"attacker\n").unwrap();
        symlink(&outside, store.join("absolute.kdl")).unwrap();
        symlink("../../outside.kdl", store.join("links/relative.kdl")).unwrap();
        symlink(&outside_directory, store.join("links/component")).unwrap();

        for target in [
            store.join("absolute.kdl"),
            store.join("links/relative.kdl"),
            store.join("links/component/config.kdl"),
        ] {
            let error = read_test_store(&target, &store).unwrap_err();
            assert_eq!(error.code(), "unsafe_path");
        }
    }

    #[test]
    fn static_store_symlink_loop_is_rejected_within_hop_bound() {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("store");
        fs::create_dir_all(&store).unwrap();
        symlink("second.kdl", store.join("first.kdl")).unwrap();
        symlink("first.kdl", store.join("second.kdl")).unwrap();
        let started = Instant::now();

        let error = read_test_store(&store.join("first.kdl"), &store).unwrap_err();

        assert_eq!(error.code(), "unsafe_path");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn static_store_final_must_be_owned_regular() {
        let temp = TempDir::new().unwrap();
        let store = temp.path().join("store");
        fs::create_dir_all(store.join("directory")).unwrap();
        symlink("directory", store.join("not-regular.kdl")).unwrap();

        let error = read_test_store(&store.join("not-regular.kdl"), &store).unwrap_err();
        assert_eq!(error.code(), "unsafe_path");

        fs::write(store.join("regular.kdl"), b"owned-by-current-user\n").unwrap();
        let wrong_owner = unsafe { libc::geteuid() }.wrapping_add(1);
        let error =
            read_store_regular_for_policy(&store.join("regular.kdl"), &store, wrong_owner, 40)
                .unwrap_err();
        assert_eq!(error.code(), "unsafe_path");
    }
}
