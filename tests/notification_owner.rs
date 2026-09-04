use std::{ffi::OsStr, io};

use sleepy_session::notifications::{notification_bus_owner, NotificationBusOwner};

#[test]
fn notification_bus_owner_defaults_to_session_and_accepts_explicit_owners() {
    assert_eq!(
        notification_bus_owner(None).unwrap(),
        NotificationBusOwner::Session
    );
    assert_eq!(
        notification_bus_owner(Some(OsStr::new("session"))).unwrap(),
        NotificationBusOwner::Session
    );
    assert_eq!(
        notification_bus_owner(Some(OsStr::new("shell"))).unwrap(),
        NotificationBusOwner::Shell
    );
}

#[test]
fn notification_bus_owner_rejects_unknown_or_non_utf8_values() {
    let unknown = notification_bus_owner(Some(OsStr::new("auto"))).unwrap_err();
    assert_eq!(unknown.kind(), io::ErrorKind::InvalidInput);

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let invalid = notification_bus_owner(Some(OsStr::from_bytes(&[0xff]))).unwrap_err();
        assert_eq!(invalid.kind(), io::ErrorKind::InvalidInput);
    }
}
