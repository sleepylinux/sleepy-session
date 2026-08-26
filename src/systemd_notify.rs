use std::{env, ffi::OsStr, io, os::unix::ffi::OsStrExt, path::Path};

pub fn ready() -> io::Result<()> {
    let Some(socket) = env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    if socket.as_os_str().as_bytes().first() == Some(&b'@') {
        send_abstract(
            socket.as_os_str(),
            b"READY=1\nSTATUS=Sleepy session sockets ready",
        )
    } else {
        let sender = std::os::unix::net::UnixDatagram::unbound()?;
        sender.connect(Path::new(&socket))?;
        sender.send(b"READY=1\nSTATUS=Sleepy session sockets ready")?;
        Ok(())
    }
}

fn send_abstract(socket: &OsStr, message: &[u8]) -> io::Result<()> {
    let name = socket.as_bytes();
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    let path_offset = std::mem::offset_of!(libc::sockaddr_un, sun_path);
    if name.len() > address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NOTIFY_SOCKET abstract name is too long",
        ));
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (destination, source) in address.sun_path[1..].iter_mut().zip(&name[1..]) {
        *destination = *source as libc::c_char;
    }
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let sent = unsafe {
        libc::sendto(
            fd,
            message.as_ptr().cast(),
            message.len(),
            libc::MSG_NOSIGNAL,
            (&raw const address).cast(),
            (path_offset + name.len()) as libc::socklen_t,
        )
    };
    let result = if sent < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    unsafe { libc::close(fd) };
    result
}
