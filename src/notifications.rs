use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::{CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    validate_notification_document, EventCause, EventCauseKind, NotificationActionState,
    NotificationChange, NotificationDocument, NotificationEvent, NotificationUrgency,
    ProviderEvent, SessionEvent, DURABLE_SCHEMA_VERSION,
};

use crate::sessiond::GenerationAuthority;

mod dbus_server;
pub use dbus_server::NotificationDbusServer;

pub const DBUS_NOTIFICATIONS_NAME: &str = "org.freedesktop.Notifications";
pub const MAX_NOTIFICATION_BYTES: usize = 64 * 1024;
pub const DEFAULT_ACTIVE_NOTIFICATION_CAPACITY: usize = 500;
pub const DEFAULT_NOTIFICATION_STATE_BYTES: usize = 48 * 1024 * 1024;
const MAX_DURABLE_FILE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NotifyRequest {
    pub origin: String,
    pub notification: NotificationDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotifyOutcome {
    pub id: u64,
    pub popup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PushResult {
    id: u64,
    archived_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionInvocation {
    pub notification_id: u64,
    pub action_id: String,
    pub origin: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationCommand {
    MarkRead { id: u64 },
    Dismiss { id: u64 },
    Archive { id: u64 },
    SetDnd { enabled: bool },
    PurgeArchive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationGroup<'a> {
    pub application_id: &'a str,
    pub notifications: Vec<&'a NotificationDocument>,
}

pub struct FreedesktopNotificationProvider {
    store: NotificationStore,
    origins: HashMap<u64, String>,
    popups: HashMap<u64, Option<Instant>>,
}

impl FreedesktopNotificationProvider {
    pub fn new(mut store: NotificationStore) -> io::Result<Self> {
        store.expire_all_actions()?;
        Ok(Self {
            store,
            origins: HashMap::new(),
            popups: HashMap::new(),
        })
    }

    pub fn bus_name(&self) -> &'static str {
        DBUS_NOTIFICATIONS_NAME
    }

    pub fn capabilities(&self) -> &'static [&'static str] {
        &["actions", "body"]
    }

    pub fn store(&self) -> &NotificationStore {
        &self.store
    }

    pub fn with_commit_observer(mut self, observer: Arc<dyn NotificationCommitObserver>) -> Self {
        self.store.commit_observer = Some(observer);
        self
    }

    pub fn notify(&mut self, request: NotifyRequest) -> io::Result<NotifyOutcome> {
        self.notify_at(request, Instant::now())
    }

    pub fn notify_at(&mut self, request: NotifyRequest, now: Instant) -> io::Result<NotifyOutcome> {
        if request.origin.trim().is_empty() || request.origin.len() > 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid notification D-Bus origin",
            ));
        }
        let replacement_id = request.notification.id;
        if replacement_id != 0 {
            let active = self
                .store
                .active()
                .iter()
                .any(|notification| notification.id == replacement_id);
            if !active || self.origins.get(&replacement_id) != Some(&request.origin) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "replaces_id is not an active notification owned by this D-Bus source",
                ));
            }
        }
        let popup = self.store.popup_allowed(&request.notification);
        let timeout_ms = request.notification.timeout_ms;
        let result = self.store.push(request.notification)?;
        let id = result.id;
        for archived_id in result.archived_ids {
            self.popups.remove(&archived_id);
        }
        self.origins.insert(id, request.origin);
        if popup {
            let deadline = timeout_ms
                .filter(|timeout| *timeout > 0)
                .map(|timeout| now + Duration::from_millis(timeout));
            self.popups.insert(id, deadline);
        } else {
            self.popups.remove(&id);
        }
        Ok(NotifyOutcome { id, popup })
    }

    pub fn execute(&mut self, command: NotificationCommand) -> io::Result<()> {
        match command {
            NotificationCommand::MarkRead { id } => self.store.mark_read(id),
            NotificationCommand::Dismiss { id } | NotificationCommand::Archive { id } => {
                self.store.dismiss(id)?;
                self.popups.remove(&id);
                Ok(())
            }
            NotificationCommand::SetDnd { enabled } => {
                self.store.set_dnd(enabled)?;
                if enabled {
                    let critical = self
                        .store
                        .active()
                        .iter()
                        .filter_map(|notification| {
                            (notification.urgency == NotificationUrgency::Critical)
                                .then_some(notification.id)
                        })
                        .collect::<HashSet<_>>();
                    self.popups.retain(|id, _| critical.contains(id));
                }
                Ok(())
            }
            NotificationCommand::PurgeArchive => {
                let purged = self
                    .store
                    .archive()
                    .iter()
                    .map(|notification| notification.id)
                    .collect::<Vec<_>>();
                self.store.purge_archive()?;
                for id in purged {
                    self.origins.remove(&id);
                    self.popups.remove(&id);
                }
                Ok(())
            }
        }
    }

    pub fn popup_visible(&self, id: u64) -> bool {
        self.popups.contains_key(&id)
    }

    fn advance_popup_time(&mut self, now: Instant) -> Vec<u64> {
        let mut expired = self
            .popups
            .iter()
            .filter_map(|(id, deadline)| deadline.filter(|deadline| now >= *deadline).map(|_| *id))
            .collect::<Vec<_>>();
        expired.sort_unstable();
        for id in &expired {
            self.popups.remove(id);
        }
        expired
    }

    fn expire_popups(&mut self, now: Instant) -> Vec<u64> {
        self.advance_popup_time(now)
    }

    pub fn invoke_action(
        &self,
        notification_id: u64,
        action_id: &str,
    ) -> io::Result<ActionInvocation> {
        let notification = self
            .store
            .active()
            .iter()
            .find(|notification| notification.id == notification_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        let action = notification
            .actions
            .iter()
            .find(|action| action.id == action_id)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "notification action not found")
            })?;
        let origin = self.origins.get(&notification_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "notification origin is no longer available",
            )
        })?;
        if action.state != NotificationActionState::Available {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "notification action has expired",
            ));
        }
        Ok(ActionInvocation {
            notification_id,
            action_id: action.id.clone(),
            origin: origin.clone(),
        })
    }

    pub fn origin_lost(&mut self, origin: &str) -> io::Result<()> {
        let ids = self.ids_for_origin(origin);
        for id in ids {
            self.store.expire_actions(id)?;
            self.origins.remove(&id);
        }
        Ok(())
    }

    fn ids_for_origin(&self, origin: &str) -> Vec<u64> {
        self.origins
            .iter()
            .filter_map(|(id, current)| (current == origin).then_some(*id))
            .collect()
    }
}

pub struct NotificationEventService {
    provider: FreedesktopNotificationProvider,
    authority: GenerationAuthority,
}

impl NotificationEventService {
    pub fn new(provider: FreedesktopNotificationProvider, authority: GenerationAuthority) -> Self {
        Self {
            provider,
            authority,
        }
    }

    pub fn provider(&self) -> &FreedesktopNotificationProvider {
        &self.provider
    }

    pub async fn notify(&mut self, request: NotifyRequest) -> io::Result<NotifyOutcome> {
        let requested_id = request.notification.id;
        let change = if requested_id != 0
            && self
                .provider
                .store()
                .active()
                .iter()
                .any(|notification| notification.id == requested_id)
        {
            NotificationChange::Updated
        } else {
            NotificationChange::Added
        };
        let outcome = self.provider.notify(request)?;
        self.publish(outcome.id, change).await?;
        Ok(outcome)
    }

    pub async fn execute(&mut self, command: NotificationCommand) -> io::Result<()> {
        self.provider.execute(command)?;
        match command {
            NotificationCommand::MarkRead { id } => {
                self.publish(id, NotificationChange::Updated).await?;
            }
            NotificationCommand::Dismiss { id } | NotificationCommand::Archive { id } => {
                self.publish(id, NotificationChange::Archived).await?;
            }
            NotificationCommand::SetDnd { enabled } => {
                self.publish_provider("org.freedesktop.Notifications.dnd", !enabled)
                    .await?;
            }
            NotificationCommand::PurgeArchive => {
                self.publish_provider("org.freedesktop.Notifications.archive", true)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn advance_popup_time(&mut self, now: Instant) -> io::Result<Vec<u64>> {
        let expired = self.provider.expire_popups(now);
        for id in &expired {
            self.publish(*id, NotificationChange::Updated).await?;
        }
        Ok(expired)
    }

    pub async fn origin_lost(&mut self, origin: &str) -> io::Result<()> {
        let ids = self.provider.ids_for_origin(origin);
        self.provider.origin_lost(origin)?;
        for id in ids {
            self.publish(id, NotificationChange::ActionExpired).await?;
        }
        Ok(())
    }

    async fn publish(&self, id: u64, change: NotificationChange) -> io::Result<()> {
        self.authority
            .lock()
            .await
            .publish(
                EventCause {
                    kind: EventCauseKind::External,
                    request_id: None,
                },
                SessionEvent::Notification(NotificationEvent {
                    notification_id: id,
                    change,
                }),
            )
            .await
            .map(|_| ())
    }

    async fn publish_provider(&self, provider_id: &str, online: bool) -> io::Result<()> {
        self.authority
            .lock()
            .await
            .publish(
                EventCause {
                    kind: EventCauseKind::External,
                    request_id: None,
                },
                SessionEvent::Provider(ProviderEvent {
                    provider_id: provider_id.to_owned(),
                    online,
                }),
            )
            .await
            .map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationCommitStage {
    JournalCommitted,
    ActiveCommitted,
    ArchiveCommitted,
    PreferencesCommitted,
    JournalRemoved,
    DirectorySynced,
}

impl NotificationCommitStage {
    pub const ALL: [Self; 6] = [
        Self::JournalCommitted,
        Self::ActiveCommitted,
        Self::ArchiveCommitted,
        Self::PreferencesCommitted,
        Self::JournalRemoved,
        Self::DirectorySynced,
    ];
}

pub trait NotificationCommitObserver: Send + Sync {
    fn reached(&self, stage: NotificationCommitStage) -> io::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Transaction {
    schema_version: u32,
    active: Vec<NotificationDocument>,
    archive: Vec<NotificationDocument>,
    preferences: Preferences,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DurableNotifications {
    schema_version: u32,
    notifications: Vec<NotificationDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Preferences {
    schema_version: u32,
    dnd: bool,
    #[serde(default = "first_notification_id")]
    next_notification_id: u64,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            schema_version: DURABLE_SCHEMA_VERSION,
            dnd: false,
            next_notification_id: first_notification_id(),
        }
    }
}

const fn first_notification_id() -> u64 {
    1
}

pub struct NotificationStore {
    directory: SecureDirectory,
    capacity: usize,
    aggregate_limit: usize,
    active: Vec<NotificationDocument>,
    archive: Vec<NotificationDocument>,
    preferences: Preferences,
    commit_observer: Option<Arc<dyn NotificationCommitObserver>>,
}

impl NotificationStore {
    pub fn open_default(directory: impl AsRef<Path>) -> io::Result<Self> {
        Self::open(directory, DEFAULT_ACTIVE_NOTIFICATION_CAPACITY)
    }

    pub fn open(directory: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        Self::open_with_limits(directory, capacity, DEFAULT_NOTIFICATION_STATE_BYTES)
    }

    pub fn open_with_limits(
        directory: impl AsRef<Path>,
        capacity: usize,
        aggregate_limit: usize,
    ) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "notification capacity must be positive",
            ));
        }
        if aggregate_limit == 0 || aggregate_limit >= MAX_DURABLE_FILE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "notification aggregate limit must fit in one durable document",
            ));
        }
        let directory = SecureDirectory::open(directory.as_ref(), true)?;

        reconcile(&directory)?;
        let active = read_documents(&directory, "active.json")?;
        let archive = read_documents(&directory, "archive.json")?;
        let mut preferences = read_preferences(&directory)?;
        let greatest_id = active
            .iter()
            .chain(&archive)
            .map(|notification| notification.id)
            .max()
            .unwrap_or(0);
        preferences.next_notification_id = preferences
            .next_notification_id
            .max(greatest_id.saturating_add(1));
        validate_unique_ids(&active, &archive)?;
        validate_aggregate(&active, &archive, &preferences, aggregate_limit)?;
        Ok(Self {
            directory,
            capacity,
            aggregate_limit,
            active,
            archive,
            preferences,
            commit_observer: None,
        })
    }

    pub fn with_commit_observer(mut self, observer: Arc<dyn NotificationCommitObserver>) -> Self {
        self.commit_observer = Some(observer);
        self
    }

    pub fn active(&self) -> &[NotificationDocument] {
        &self.active
    }

    pub fn archive(&self) -> &[NotificationDocument] {
        &self.archive
    }

    pub fn dnd(&self) -> bool {
        self.preferences.dnd
    }

    pub fn unread_count(&self) -> usize {
        self.active.iter().filter(|item| !item.read).count()
    }

    pub fn grouped_active(&self) -> Vec<NotificationGroup<'_>> {
        let mut groups = BTreeMap::<&str, Vec<&NotificationDocument>>::new();
        for notification in &self.active {
            groups
                .entry(notification.application_id.as_str())
                .or_default()
                .push(notification);
        }
        groups
            .into_iter()
            .map(|(application_id, notifications)| NotificationGroup {
                application_id,
                notifications,
            })
            .collect()
    }

    pub fn popup_allowed(&self, notification: &NotificationDocument) -> bool {
        !self.preferences.dnd || notification.urgency == NotificationUrgency::Critical
    }

    fn set_dnd(&mut self, enabled: bool) -> io::Result<()> {
        let mut preferences = self.preferences.clone();
        preferences.dnd = enabled;
        self.commit_candidate(self.active.clone(), self.archive.clone(), preferences)
    }

    fn push(&mut self, mut notification: NotificationDocument) -> io::Result<PushResult> {
        let mut preferences = self.preferences.clone();
        if notification.id == 0 {
            if preferences.next_notification_id > u64::from(u32::MAX) {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "D-Bus notification identifiers exhausted",
                ));
            }
            notification.id = preferences.next_notification_id;
            preferences.next_notification_id = notification.id.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "notification identifiers exhausted",
                )
            })?;
        } else if notification.id >= preferences.next_notification_id {
            preferences.next_notification_id = notification.id.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "notification identifiers exhausted",
                )
            })?;
        }
        validate_document(&notification)?;
        if self
            .archive
            .iter()
            .any(|archived| archived.id == notification.id)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "archived notification identifiers cannot be reused",
            ));
        }
        let id = notification.id;
        notification.archived = false;
        let mut active = self.active.clone();
        let mut archive = self.archive.clone();
        if let Some(index) = active.iter().position(|item| item.id == notification.id) {
            active.remove(index);
        }
        active.push(notification);
        let mut archived_ids = Vec::new();
        if active.len() > self.capacity {
            let overflow = active.len() - self.capacity;
            let mut archived: Vec<_> = active.drain(..overflow).collect();
            for item in &mut archived {
                item.archived = true;
            }
            archived_ids.extend(archived.iter().map(|item| item.id));
            archive.extend(archived);
        }
        self.commit_candidate(active, archive, preferences)?;
        Ok(PushResult { id, archived_ids })
    }

    fn dismiss(&mut self, id: u64) -> io::Result<()> {
        let mut active = self.active.clone();
        let mut archive = self.archive.clone();
        let index = active
            .iter()
            .position(|item| item.id == id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        let mut notification = active.remove(index);
        notification.archived = true;
        archive.push(notification);
        self.commit_candidate(active, archive, self.preferences.clone())
    }

    fn mark_read(&mut self, id: u64) -> io::Result<()> {
        let mut active = self.active.clone();
        let notification = active
            .iter_mut()
            .find(|item| item.id == id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        notification.read = true;
        self.commit_candidate(active, self.archive.clone(), self.preferences.clone())
    }

    fn expire_actions(&mut self, id: u64) -> io::Result<()> {
        let mut active = self.active.clone();
        let mut archive = self.archive.clone();
        let notification = active
            .iter_mut()
            .find(|item| item.id == id)
            .or_else(|| archive.iter_mut().find(|item| item.id == id))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        for action in &mut notification.actions {
            action.state = NotificationActionState::Expired;
        }
        self.commit_candidate(active, archive, self.preferences.clone())
    }

    fn purge_archive(&mut self) -> io::Result<()> {
        self.commit_candidate(self.active.clone(), Vec::new(), self.preferences.clone())
    }

    fn expire_all_actions(&mut self) -> io::Result<()> {
        let mut active = self.active.clone();
        let mut archive = self.archive.clone();
        let mut changed = false;
        for notification in active.iter_mut().chain(archive.iter_mut()) {
            for action in &mut notification.actions {
                if action.state == NotificationActionState::Available {
                    action.state = NotificationActionState::Expired;
                    changed = true;
                }
            }
        }
        if changed {
            self.commit_candidate(active, archive, self.preferences.clone())?;
        }
        Ok(())
    }

    fn commit_candidate(
        &mut self,
        active: Vec<NotificationDocument>,
        archive: Vec<NotificationDocument>,
        preferences: Preferences,
    ) -> io::Result<()> {
        validate_unique_ids(&active, &archive)?;
        validate_aggregate(&active, &archive, &preferences, self.aggregate_limit)?;
        let transaction = Transaction {
            schema_version: DURABLE_SCHEMA_VERSION,
            active: active.clone(),
            archive: archive.clone(),
            preferences: preferences.clone(),
        };
        atomic_json(&self.directory, "transaction.json", &transaction)?;
        self.observe(NotificationCommitStage::JournalCommitted)?;
        write_documents(&self.directory, "active.json", &active)?;
        self.observe(NotificationCommitStage::ActiveCommitted)?;
        write_documents(&self.directory, "archive.json", &archive)?;
        self.observe(NotificationCommitStage::ArchiveCommitted)?;
        atomic_json(&self.directory, "preferences.json", &preferences)?;
        self.observe(NotificationCommitStage::PreferencesCommitted)?;
        self.directory.remove("transaction.json")?;
        self.observe(NotificationCommitStage::JournalRemoved)?;
        self.directory.sync()?;
        self.observe(NotificationCommitStage::DirectorySynced)?;
        self.active = active;
        self.archive = archive;
        self.preferences = preferences;
        Ok(())
    }

    fn observe(&self, stage: NotificationCommitStage) -> io::Result<()> {
        match &self.commit_observer {
            Some(observer) => observer.reached(stage),
            None => Ok(()),
        }
    }
}

fn reconcile(directory: &SecureDirectory) -> io::Result<()> {
    let Some(transaction): Option<Transaction> = read_json(directory, "transaction.json")? else {
        return Ok(());
    };
    require_durable_version(transaction.schema_version)?;
    require_durable_version(transaction.preferences.schema_version)?;
    validate_documents(&transaction.active)?;
    validate_documents(&transaction.archive)?;
    write_documents(directory, "active.json", &transaction.active)?;
    write_documents(directory, "archive.json", &transaction.archive)?;
    atomic_json(directory, "preferences.json", &transaction.preferences)?;
    directory.remove("transaction.json")?;
    directory.sync()
}

fn read_documents(
    directory: &SecureDirectory,
    name: &str,
) -> io::Result<Vec<NotificationDocument>> {
    let Some(document): Option<DurableNotifications> = read_json(directory, name)? else {
        return Ok(Vec::new());
    };
    require_durable_version(document.schema_version)?;
    let documents = document.notifications;
    validate_documents(&documents)?;
    Ok(documents)
}

fn write_documents(
    directory: &SecureDirectory,
    name: &str,
    notifications: &[NotificationDocument],
) -> io::Result<()> {
    atomic_json(
        directory,
        name,
        &DurableNotifications {
            schema_version: DURABLE_SCHEMA_VERSION,
            notifications: notifications.to_vec(),
        },
    )
}

fn read_preferences(directory: &SecureDirectory) -> io::Result<Preferences> {
    let preferences: Preferences = read_json(directory, "preferences.json")?.unwrap_or_default();
    require_durable_version(preferences.schema_version)?;
    Ok(preferences)
}

fn require_durable_version(version: u32) -> io::Result<()> {
    if version != DURABLE_SCHEMA_VERSION {
        return Err(invalid_data(format!(
            "unsupported notification durable schema version {version}"
        )));
    }
    Ok(())
}

fn validate_documents(documents: &[NotificationDocument]) -> io::Result<()> {
    for document in documents {
        validate_document(document)?;
    }
    Ok(())
}

fn validate_unique_ids(
    active: &[NotificationDocument],
    archive: &[NotificationDocument],
) -> io::Result<()> {
    let mut ids = HashSet::with_capacity(active.len().saturating_add(archive.len()));
    if active.iter().chain(archive).any(|notification| {
        notification.id == 0
            || notification.id > u64::from(u32::MAX)
            || !ids.insert(notification.id)
    }) {
        return Err(invalid_data(
            "notification identifiers must be nonzero and unique across active and archive",
        ));
    }
    Ok(())
}

fn validate_aggregate(
    active: &[NotificationDocument],
    archive: &[NotificationDocument],
    preferences: &Preferences,
    limit: usize,
) -> io::Result<()> {
    let transaction = Transaction {
        schema_version: DURABLE_SCHEMA_VERSION,
        active: active.to_vec(),
        archive: archive.to_vec(),
        preferences: preferences.clone(),
    };
    let bytes = serde_json::to_vec(&transaction).map_err(invalid_data)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "notification state exceeds the configured aggregate limit",
        ));
    }
    Ok(())
}

fn validate_document(document: &NotificationDocument) -> io::Result<()> {
    let json = serde_json::to_string(document).map_err(invalid_data)?;
    if json.len() > MAX_NOTIFICATION_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "notification exceeds the 64 KiB input limit",
        ));
    }
    validate_notification_document(&json)
        .map(|_| ())
        .map_err(invalid_data)
}

fn read_json<T: for<'de> Deserialize<'de>>(
    directory: &SecureDirectory,
    name: &str,
) -> io::Result<Option<T>> {
    directory
        .read_optional(name)?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(invalid_data))
        .transpose()
}

fn atomic_json(directory: &SecureDirectory, name: &str, value: &impl Serialize) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(invalid_data)?;
    bytes.push(b'\n');
    directory.atomic_replace(name, &bytes)
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[derive(Clone)]
struct SecureDirectory {
    descriptor: Arc<OwnedFd>,
}

impl SecureDirectory {
    fn open(path: &Path, create: bool) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "notification state path must be absolute",
            ));
        }
        let root = CString::new("/").expect("static path has no NUL");
        let mut descriptor = openat_owned(
            libc::AT_FDCWD,
            &root,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )?;
        for component in path.components() {
            let Component::Normal(component) = component else {
                if matches!(component, Component::RootDir) {
                    continue;
                }
                return Err(unsafe_path());
            };
            let name = component_name(component)?;
            descriptor = match openat_owned(
                descriptor.as_raw_fd(),
                &name,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
            ) {
                Ok(descriptor) => descriptor,
                Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                    cvt(unsafe { libc::mkdirat(descriptor.as_raw_fd(), name.as_ptr(), 0o700) })?;
                    openat_owned(
                        descriptor.as_raw_fd(),
                        &name,
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                        0,
                    )?
                }
                Err(error) if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) => {
                    return Err(unsafe_path())
                }
                Err(error) => return Err(error),
            };
        }
        let metadata = fstat(descriptor.as_raw_fd())?;
        if metadata.st_mode & libc::S_IFMT != libc::S_IFDIR
            || metadata.st_uid != unsafe { libc::geteuid() }
            || metadata.st_mode & 0o022 != 0
        {
            return Err(unsafe_path());
        }
        Ok(Self {
            descriptor: Arc::new(descriptor),
        })
    }

    fn read_optional(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        let name = component_name(OsStr::new(name))?;
        let descriptor = match openat_owned(
            self.descriptor.as_raw_fd(),
            &name,
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => return Err(unsafe_path()),
            Err(error) => return Err(error),
        };
        validate_private_file(descriptor.as_raw_fd())?;
        let mut file = File::from(descriptor);
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(MAX_DURABLE_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_DURABLE_FILE_BYTES {
            return Err(invalid_data("notification state exceeds 64 MiB"));
        }
        Ok(Some(bytes))
    }

    fn atomic_replace(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        if let Some(existing) = self.open_optional(name)? {
            validate_private_file(existing.as_raw_fd())?;
        }
        let temporary_name = format!(".{name}.{}.tmp", uuid::Uuid::new_v4());
        let temporary = component_name(OsStr::new(&temporary_name))?;
        let destination = component_name(OsStr::new(name))?;
        let descriptor = openat_owned(
            self.descriptor.as_raw_fd(),
            &temporary,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )?;
        let result = (|| {
            let mut file = File::from(descriptor);
            file.write_all(bytes)?;
            file.sync_all()?;
            cvt(unsafe {
                libc::renameat(
                    self.descriptor.as_raw_fd(),
                    temporary.as_ptr(),
                    self.descriptor.as_raw_fd(),
                    destination.as_ptr(),
                )
            })?;
            self.sync()
        })();
        if result.is_err() {
            let _ = unsafe { libc::unlinkat(self.descriptor.as_raw_fd(), temporary.as_ptr(), 0) };
        }
        result
    }

    fn open_optional(&self, name: &str) -> io::Result<Option<OwnedFd>> {
        let name = component_name(OsStr::new(name))?;
        match openat_owned(
            self.descriptor.as_raw_fd(),
            &name,
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(descriptor) => Ok(Some(descriptor)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => Err(unsafe_path()),
            Err(error) => Err(error),
        }
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        let name = component_name(OsStr::new(name))?;
        match cvt(unsafe { libc::unlinkat(self.descriptor.as_raw_fd(), name.as_ptr(), 0) }) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn sync(&self) -> io::Result<()> {
        cvt(unsafe { libc::fsync(self.descriptor.as_raw_fd()) })
    }
}

fn validate_private_file(descriptor: i32) -> io::Result<()> {
    let metadata = fstat(descriptor)?;
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o777 != 0o600
    {
        return Err(unsafe_path());
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

fn component_name(name: &OsStr) -> io::Result<CString> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes == b"." || bytes == b".." || bytes.contains(&b'/') {
        return Err(unsafe_path());
    }
    CString::new(bytes).map_err(|_| unsafe_path())
}

fn cvt(result: i32) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn unsafe_path() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "unsafe notification state path",
    )
}
