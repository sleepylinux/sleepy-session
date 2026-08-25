use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io::{self},
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::MetadataExt,
    },
    path::Path,
};

use fs2::FileExt;

use crate::store::{SecureDir, StoreError};

pub struct GenerationAllocator {
    directory: SecureDir,
    state_name: OsString,
    lock_name: OsString,
    block_size: u64,
    next: u64,
    end: u64,
}

impl GenerationAllocator {
    pub fn open(path: impl AsRef<Path>, block_size: u64) -> io::Result<Self> {
        if block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "generation block size must be positive",
            ));
        }
        let state_path = path.as_ref();
        if !state_path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "generation state path must be absolute",
            ));
        }
        let parent = state_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "generation state has no parent",
            )
        })?;
        let state_name = state_path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "generation state has no name")
            })?
            .to_owned();
        let mut lock_bytes = state_name.as_bytes().to_vec();
        lock_bytes.extend_from_slice(b".lock");
        let lock_name = OsString::from_vec(lock_bytes);
        let directory = SecureDir::open_writable(parent, true).map_err(store_error)?;
        directory.enforce_private_directory().map_err(store_error)?;
        directory
            .validate_private_file_if_present(&state_name)
            .map_err(store_error)?;
        let mut allocator = Self {
            directory,
            state_name,
            lock_name,
            block_size,
            next: 0,
            end: 0,
        };
        allocator.reserve_block()?;
        Ok(allocator)
    }

    pub fn next_generation(&mut self) -> io::Result<u64> {
        if self.next == self.end {
            self.reserve_block()?;
        }
        let generation = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| io::Error::other("generation exhausted"))?;
        Ok(generation)
    }

    pub fn next_after(&mut self, floor: u64) -> io::Result<u64> {
        while self.end <= floor {
            self.reserve_block()?;
        }
        if self.next <= floor {
            self.next = floor
                .checked_add(1)
                .ok_or_else(|| io::Error::other("generation exhausted"))?;
        }
        self.next_generation()
    }

    fn reserve_block(&mut self) -> io::Result<()> {
        let lock = self
            .directory
            .open_lock(&self.lock_name)
            .map_err(store_error)?;
        validate_private_file(&lock)?;
        lock.lock_exclusive()?;

        self.directory
            .validate_private_file_if_present(&self.state_name)
            .map_err(store_error)?;
        let start = read_next(&self.directory, &self.state_name)?;
        let end = start
            .checked_add(self.block_size)
            .ok_or_else(|| io::Error::other("generation range exhausted"))?;
        atomic_write(
            &self.directory,
            &self.state_name,
            format!("{end}\n").as_bytes(),
        )?;
        FileExt::unlock(&lock)?;

        self.next = start;
        self.end = end;
        Ok(())
    }
}

fn validate_private_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "generation lock must be an owned mode-0600 regular file",
        ));
    }
    Ok(())
}

fn read_next(directory: &SecureDir, name: &OsStr) -> io::Result<u64> {
    match directory.read_optional(name).map_err(store_error)? {
        Some(bytes) => {
            if bytes.len() > 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "generation state exceeds the bounded input limit",
                ));
            }
            let value = std::str::from_utf8(&bytes)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid generation"))?;
            let parsed = value
                .trim()
                .parse::<u64>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid generation"))?;
            if parsed == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "generation must be positive",
                ));
            }
            Ok(parsed)
        }
        None => Ok(1),
    }
}

fn atomic_write(directory: &SecureDir, name: &OsStr, bytes: &[u8]) -> io::Result<()> {
    directory
        .atomic_replace(name, bytes, || Ok(()), || Ok(()), || Ok(()))
        .map_err(store_error)
}

fn store_error(error: StoreError) -> io::Error {
    let kind = if error.code() == "unsafe_path" {
        io::ErrorKind::PermissionDenied
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, error)
}
