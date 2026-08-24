use std::{
    ffi::{CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
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
        self.rename(temporary, destination)?;
        observe(PublicationBoundary::Renamed)?;
        observe(PublicationBoundary::DirectorySyncStarted)?;
        self.sync().map_err(StoreError::commit_state_unknown)?;
        observe(PublicationBoundary::DirectorySynced)
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

    pub fn entries(&self) -> Result<Vec<std::ffi::OsString>, StoreError> {
        std::fs::read_dir(self.proc_path())
            .map_err(StoreError::io)?
            .map(|entry| entry.map(|entry| entry.file_name()).map_err(StoreError::io))
            .collect()
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

fn fstat(descriptor: i32) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    cvt(unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) })?;
    Ok(unsafe { metadata.assume_init() })
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
