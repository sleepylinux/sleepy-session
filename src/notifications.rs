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
mod socket;
pub use dbus_server::{NotificationActionDispatcher, NotificationDbusServer};
pub use socket::NotificationSocket;

pub const DBUS_NOTIFICATIONS_NAME: &str = "org.freedesktop.Notifications";
pub const MAX_NOTIFICATION_BYTES: usize = 64 * 1024;
pub const DEFAULT_ACTIVE_NOTIFICATION_CAPACITY: usize = 500;
pub const DEFAULT_NOTIFICATION_STATE_BYTES: usize = 48 * 1024 * 1024;
const MAX_DURABLE_FILE_BYTES: usize = 64 * 1024 * 1024;
const SEGMENT_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const CHECKPOINT_LOG_BYTES: usize = 16 * 1024 * 1024;
const CHECKPOINT_OPERATION_INTERVAL: u64 = 1_000;
const V2_CHECKPOINT: &str = "checkpoint-v2.json";
const V2_SEGMENT: &str = "segment-v2.ndjson";

fn segment_name(index: usize) -> String {
    if index == 0 {
        V2_SEGMENT.to_owned()
    } else {
        format!("segment-v2-{index}.ndjson")
    }
}

#[derive(Debug, Clone)]
pub struct NotifyRequest {
    pub origin: String,
    pub notification: NotificationDocument,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyOutcome {
    pub id: u64,
    pub popup: bool,
    archived_ids: Vec<u64>,
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

    pub(crate) fn notify(&mut self, request: NotifyRequest) -> io::Result<NotifyOutcome> {
        self.notify_at(request, Instant::now())
    }

    pub(crate) fn notify_at(
        &mut self,
        request: NotifyRequest,
        now: Instant,
    ) -> io::Result<NotifyOutcome> {
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
        for archived_id in &result.archived_ids {
            self.popups.remove(archived_id);
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
        Ok(NotifyOutcome {
            id,
            popup,
            archived_ids: result.archived_ids,
        })
    }

    pub(crate) fn execute(&mut self, command: NotificationCommand) -> io::Result<()> {
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

    pub(crate) fn origin_lost(&mut self, origin: &str) -> io::Result<()> {
        let mut ids = self.ids_for_origin(origin);
        ids.sort_unstable();
        self.store.expire_actions(&ids)?;
        for id in ids {
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
    provider: Arc<std::sync::Mutex<FreedesktopNotificationProvider>>,
    authority: GenerationAuthority,
}

impl NotificationEventService {
    pub fn new(provider: FreedesktopNotificationProvider, authority: GenerationAuthority) -> Self {
        Self {
            provider: Arc::new(std::sync::Mutex::new(provider)),
            authority,
        }
    }

    pub fn provider(&self) -> std::sync::MutexGuard<'_, FreedesktopNotificationProvider> {
        self.provider
            .lock()
            .expect("notification provider mutex was poisoned")
    }

    pub fn with_commit_observer(self, observer: Arc<dyn NotificationCommitObserver>) -> Self {
        self.provider
            .lock()
            .expect("notification provider mutex was poisoned")
            .store
            .commit_observer = Some(observer);
        self
    }

    pub async fn notify(&mut self, request: NotifyRequest) -> io::Result<NotifyOutcome> {
        let requested_id = request.notification.id;
        let change = if requested_id != 0
            && self
                .provider()
                .store()
                .active()
                .iter()
                .any(|notification| notification.id == requested_id)
        {
            NotificationChange::Updated
        } else {
            NotificationChange::Added
        };
        let provider = Arc::clone(&self.provider);
        let outcome = tokio::task::spawn_blocking(move || {
            provider
                .lock()
                .map_err(|_| io::Error::other("notification provider mutex was poisoned"))?
                .notify(request)
        })
        .await
        .map_err(|error| {
            io::Error::other(format!("notification store worker failed: {error}"))
        })??;
        for id in &outcome.archived_ids {
            self.publish(*id, NotificationChange::Archived).await?;
        }
        self.publish(outcome.id, change).await?;
        Ok(outcome)
    }

    pub async fn execute(&mut self, command: NotificationCommand) -> io::Result<()> {
        let provider = Arc::clone(&self.provider);
        tokio::task::spawn_blocking(move || {
            provider
                .lock()
                .map_err(|_| io::Error::other("notification provider mutex was poisoned"))?
                .execute(command)
        })
        .await
        .map_err(|error| {
            io::Error::other(format!("notification store worker failed: {error}"))
        })??;
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
        let expired = self.provider().expire_popups(now);
        for id in &expired {
            self.publish(*id, NotificationChange::Updated).await?;
        }
        Ok(expired)
    }

    pub async fn origin_lost(&mut self, origin: &str) -> io::Result<()> {
        let mut ids = self.provider().ids_for_origin(origin);
        ids.sort_unstable();
        let provider = Arc::clone(&self.provider);
        let origin = origin.to_owned();
        tokio::task::spawn_blocking(move || {
            provider
                .lock()
                .map_err(|_| io::Error::other("notification provider mutex was poisoned"))?
                .origin_lost(&origin)
        })
        .await
        .map_err(|error| {
            io::Error::other(format!("notification store worker failed: {error}"))
        })??;
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
    LegacyMigration,
    CheckpointPublished,
    SegmentRotated,
    AppendWritten,
    AppendFileSynced,
    AppendDirectorySynced,
    RecordAppended,
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

    pub const V2_ALL: [Self; 7] = [
        Self::LegacyMigration,
        Self::CheckpointPublished,
        Self::SegmentRotated,
        Self::AppendWritten,
        Self::AppendFileSynced,
        Self::AppendDirectorySynced,
        Self::RecordAppended,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    deny_unknown_fields
)]
enum StoreOperation {
    SetDnd {
        enabled: bool,
    },
    Push {
        notification: NotificationDocument,
        archived: Vec<NotificationDocument>,
    },
    Dismiss {
        notification: NotificationDocument,
    },
    MarkRead {
        id: u64,
    },
    ExpireActions {
        ids: Vec<u64>,
    },
    PurgeArchive,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SegmentRecord {
    schema_version: u32,
    sequence: u64,
    operation: StoreOperation,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoreCheckpoint {
    schema_version: u32,
    storage_version: u32,
    sequence: u64,
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
    v2_initialized: bool,
    next_sequence: u64,
    operations_since_checkpoint: u64,
    segment_bytes: usize,
    log_bytes_since_checkpoint: usize,
    segment_index: usize,
    tail_repair: Option<(usize, u64)>,
    active_json_bytes: usize,
    archive_json_bytes: usize,
    preferences_json_bytes: usize,
    notification_ids: HashSet<u64>,
    poisoned: bool,
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
        let v2 = read_v2_state(&directory, capacity)?;
        let (
            active,
            archive,
            mut preferences,
            next_sequence,
            operations,
            segment_bytes,
            log_bytes_since_checkpoint,
            segment_index,
            tail_repair,
        ) = if let Some(state) = v2 {
            state
        } else {
            (
                read_documents(&directory, "active.json")?,
                read_documents(&directory, "archive.json")?,
                read_preferences(&directory)?,
                1,
                0,
                0,
                0,
                0,
                None,
            )
        };
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
        validate_documents(&active)?;
        validate_documents(&archive)?;
        if active.len() > capacity {
            return Err(invalid_data(
                "notification active state exceeds configured capacity",
            ));
        }
        validate_aggregate(&active, &archive, &preferences, aggregate_limit)?;
        let v2_initialized = next_sequence > 1 || directory.open_optional(V2_CHECKPOINT)?.is_some();
        let active_json_bytes = serialized_documents_size(&active)?;
        let archive_json_bytes = serialized_documents_size(&archive)?;
        let preferences_json_bytes = serde_json::to_vec(&preferences)
            .map_err(invalid_data)?
            .len();
        let notification_ids = active
            .iter()
            .chain(&archive)
            .map(|notification| notification.id)
            .collect();
        Ok(Self {
            directory,
            capacity,
            aggregate_limit,
            active,
            archive,
            preferences,
            commit_observer: None,
            v2_initialized,
            next_sequence,
            operations_since_checkpoint: operations,
            segment_bytes,
            log_bytes_since_checkpoint,
            segment_index,
            tail_repair,
            active_json_bytes,
            archive_json_bytes,
            preferences_json_bytes,
            notification_ids,
            poisoned: false,
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
        let preferences_bytes = serde_json::to_vec(&preferences)
            .map_err(invalid_data)?
            .len();
        ensure_aggregate_parts(
            self.active.len(),
            self.archive.len(),
            self.active_json_bytes,
            self.archive_json_bytes,
            preferences_bytes,
            self.aggregate_limit,
        )?;
        self.append_operation(StoreOperation::SetDnd { enabled })?;
        self.preferences = preferences;
        self.preferences_json_bytes = preferences_bytes;
        self.finish_operation()
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
        let existing_index = self
            .active
            .iter()
            .position(|item| item.id == notification.id);
        if existing_index.is_none() && self.notification_ids.contains(&notification.id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "archived notification identifiers cannot be reused",
            ));
        }
        let id = notification.id;
        notification.archived = false;
        let notification_bytes = serde_json::to_vec(&notification)
            .map_err(invalid_data)?
            .len();
        let mut active_bytes = self.active_json_bytes;
        if let Some(index) = existing_index {
            active_bytes = active_bytes
                .checked_sub(
                    serde_json::to_vec(&self.active[index])
                        .map_err(invalid_data)?
                        .len(),
                )
                .ok_or_else(|| invalid_data("notification active size underflow"))?;
        }
        active_bytes = active_bytes
            .checked_add(notification_bytes)
            .ok_or_else(|| invalid_data("notification active size overflow"))?;
        let active_count = self.active.len() - usize::from(existing_index.is_some()) + 1;
        let mut archived = Vec::new();
        let mut archive_bytes = self.archive_json_bytes;
        if active_count > self.capacity {
            let mut item = self.active[0].clone();
            active_bytes = active_bytes
                .checked_sub(serde_json::to_vec(&item).map_err(invalid_data)?.len())
                .ok_or_else(|| invalid_data("notification active size underflow"))?;
            item.archived = true;
            archive_bytes = archive_bytes
                .checked_add(serde_json::to_vec(&item).map_err(invalid_data)?.len())
                .ok_or_else(|| invalid_data("notification archive size overflow"))?;
            archived.push(item);
        }
        let final_active_count = active_count.min(self.capacity);
        let final_archive_count = self.archive.len() + archived.len();
        let preferences_bytes = serde_json::to_vec(&preferences)
            .map_err(invalid_data)?
            .len();
        ensure_aggregate_parts(
            final_active_count,
            final_archive_count,
            active_bytes,
            archive_bytes,
            preferences_bytes,
            self.aggregate_limit,
        )?;
        self.append_operation(StoreOperation::Push {
            notification: notification.clone(),
            archived: archived.clone(),
        })?;
        if let Some(index) = existing_index {
            self.active.remove(index);
        }
        self.active.push(notification);
        self.notification_ids.insert(id);
        if !archived.is_empty() {
            self.active.remove(0);
            self.archive.extend(archived.iter().cloned());
        }
        let archived_ids = archived.iter().map(|item| item.id).collect();
        self.preferences = preferences;
        self.active_json_bytes = active_bytes;
        self.archive_json_bytes = archive_bytes;
        self.preferences_json_bytes = preferences_bytes;
        self.finish_operation()?;
        Ok(PushResult { id, archived_ids })
    }

    fn dismiss(&mut self, id: u64) -> io::Result<()> {
        let index = self
            .active
            .iter()
            .position(|item| item.id == id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        let active_length = serde_json::to_vec(&self.active[index])
            .map_err(invalid_data)?
            .len();
        let mut notification = self.active[index].clone();
        notification.archived = true;
        let archive_length = serde_json::to_vec(&notification)
            .map_err(invalid_data)?
            .len();
        let active_bytes = self
            .active_json_bytes
            .checked_sub(active_length)
            .ok_or_else(|| invalid_data("notification active size underflow"))?;
        let archive_bytes = self
            .archive_json_bytes
            .checked_add(archive_length)
            .ok_or_else(|| invalid_data("notification archive size overflow"))?;
        ensure_aggregate_parts(
            self.active.len() - 1,
            self.archive.len() + 1,
            active_bytes,
            archive_bytes,
            self.preferences_json_bytes,
            self.aggregate_limit,
        )?;
        self.append_operation(StoreOperation::Dismiss {
            notification: notification.clone(),
        })?;
        self.active.remove(index);
        self.archive.push(notification);
        self.active_json_bytes = active_bytes;
        self.archive_json_bytes = archive_bytes;
        self.finish_operation()
    }

    fn mark_read(&mut self, id: u64) -> io::Result<()> {
        let index = self
            .active
            .iter()
            .position(|item| item.id == id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "notification not found"))?;
        let old_length = serde_json::to_vec(&self.active[index])
            .map_err(invalid_data)?
            .len();
        let mut notification = self.active[index].clone();
        notification.read = true;
        let new_length = serde_json::to_vec(&notification)
            .map_err(invalid_data)?
            .len();
        let active_bytes = self
            .active_json_bytes
            .checked_sub(old_length)
            .and_then(|value| value.checked_add(new_length))
            .ok_or_else(|| invalid_data("notification active size overflow"))?;
        ensure_aggregate_parts(
            self.active.len(),
            self.archive.len(),
            active_bytes,
            self.archive_json_bytes,
            self.preferences_json_bytes,
            self.aggregate_limit,
        )?;
        self.append_operation(StoreOperation::MarkRead { id })?;
        self.active[index] = notification;
        self.active_json_bytes = active_bytes;
        self.finish_operation()
    }

    fn expire_actions(&mut self, ids: &[u64]) -> io::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut active_bytes = self.active_json_bytes;
        let mut archive_bytes = self.archive_json_bytes;
        if ids.iter().any(|id| !self.notification_ids.contains(id)) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "notification not found",
            ));
        }
        let ids_set = ids.iter().copied().collect::<HashSet<_>>();
        for (documents, bytes) in [
            (&self.active, &mut active_bytes),
            (&self.archive, &mut archive_bytes),
        ] {
            for notification in documents {
                if !ids_set.contains(&notification.id) {
                    continue;
                }
                let old_length = serde_json::to_vec(notification)
                    .map_err(invalid_data)?
                    .len();
                let mut updated = notification.clone();
                for action in &mut updated.actions {
                    action.state = NotificationActionState::Expired;
                }
                let new_length = serde_json::to_vec(&updated).map_err(invalid_data)?.len();
                *bytes = bytes
                    .checked_sub(old_length)
                    .and_then(|value| value.checked_add(new_length))
                    .ok_or_else(|| invalid_data("notification action size overflow"))?;
            }
        }
        ensure_aggregate_parts(
            self.active.len(),
            self.archive.len(),
            active_bytes,
            archive_bytes,
            self.preferences_json_bytes,
            self.aggregate_limit,
        )?;
        self.append_operation(StoreOperation::ExpireActions { ids: ids.clone() })?;
        for notification in self.active.iter_mut().chain(self.archive.iter_mut()) {
            if ids.binary_search(&notification.id).is_ok() {
                for action in &mut notification.actions {
                    action.state = NotificationActionState::Expired;
                }
            }
        }
        self.active_json_bytes = active_bytes;
        self.archive_json_bytes = archive_bytes;
        self.finish_operation()
    }

    fn purge_archive(&mut self) -> io::Result<()> {
        self.append_operation(StoreOperation::PurgeArchive)?;
        for notification in &self.archive {
            self.notification_ids.remove(&notification.id);
        }
        self.archive.clear();
        self.archive_json_bytes = 0;
        self.finish_operation()
    }

    fn expire_all_actions(&mut self) -> io::Result<()> {
        let changed_ids = self
            .active
            .iter()
            .chain(&self.archive)
            .filter(|notification| {
                notification
                    .actions
                    .iter()
                    .any(|action| action.state == NotificationActionState::Available)
            })
            .map(|notification| notification.id)
            .collect::<Vec<_>>();
        self.expire_actions(&changed_ids)
    }

    fn append_operation(&mut self, operation: StoreOperation) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "notification store requires restart after an ambiguous durable write",
            ));
        }
        let record = SegmentRecord {
            schema_version: DURABLE_SCHEMA_VERSION,
            sequence: self.next_sequence,
            operation,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(invalid_data)?;
        bytes.push(b'\n');
        if bytes.len() > SEGMENT_LIMIT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "notification operation exceeds the segment limit",
            ));
        }
        // From the first durable mutation onward an I/O error can mean either
        // "not written" or "written but acknowledgement failed". Refuse all
        // later mutations until reopen has replayed and validated the log.
        self.poisoned = true;
        if !self.v2_initialized {
            self.import_legacy()?;
        }
        if let Some((segment_index, valid_length)) = self.tail_repair.take() {
            let name = segment_name(segment_index);
            let old_length = if segment_index == self.segment_index {
                self.segment_bytes
            } else {
                0
            };
            self.directory.truncate(&name, valid_length)?;
            if segment_index == self.segment_index {
                self.segment_bytes = valid_length as usize;
            }
            self.log_bytes_since_checkpoint = self
                .log_bytes_since_checkpoint
                .saturating_sub(old_length.saturating_sub(valid_length as usize));
        }
        if self.log_bytes_since_checkpoint.saturating_add(bytes.len()) > CHECKPOINT_LOG_BYTES {
            self.publish_checkpoint(
                &self.active,
                &self.archive,
                &self.preferences,
                self.next_sequence.saturating_sub(1),
            )?;
            self.operations_since_checkpoint = 0;
            self.segment_bytes = 0;
            self.log_bytes_since_checkpoint = 0;
            self.segment_index = 0;
        } else if self.segment_bytes.saturating_add(bytes.len()) > SEGMENT_LIMIT_BYTES {
            self.segment_index = self.segment_index.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "notification segment index exhausted",
                )
            })?;
            self.segment_bytes = 0;
            self.observe(NotificationCommitStage::SegmentRotated)?;
        }
        let observer = self.commit_observer.clone();
        self.directory
            .append(
                &segment_name(self.segment_index),
                &bytes,
                move |stage| match &observer {
                    Some(observer) => observer.reached(stage),
                    None => Ok(()),
                },
            )?;
        self.observe(NotificationCommitStage::RecordAppended)?;
        self.segment_bytes = self.segment_bytes.saturating_add(bytes.len());
        self.log_bytes_since_checkpoint =
            self.log_bytes_since_checkpoint.saturating_add(bytes.len());
        self.operations_since_checkpoint = self.operations_since_checkpoint.saturating_add(1);
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "notification sequence exhausted",
            )
        })?;
        Ok(())
    }

    fn finish_operation(&mut self) -> io::Result<()> {
        if self.operations_since_checkpoint >= CHECKPOINT_OPERATION_INTERVAL {
            self.publish_checkpoint(
                &self.active,
                &self.archive,
                &self.preferences,
                self.next_sequence - 1,
            )?;
            self.operations_since_checkpoint = 0;
            self.segment_bytes = 0;
            self.log_bytes_since_checkpoint = 0;
            self.segment_index = 0;
        }
        self.poisoned = false;
        Ok(())
    }

    fn import_legacy(&mut self) -> io::Result<()> {
        for (source, backup) in [
            ("active.json", "legacy-active.json"),
            ("archive.json", "legacy-archive.json"),
            ("preferences.json", "legacy-preferences.json"),
        ] {
            if let Some(bytes) = self.directory.read_optional(source)? {
                self.directory.atomic_replace(backup, &bytes)?;
            }
        }
        self.observe(NotificationCommitStage::LegacyMigration)?;
        self.publish_checkpoint(&self.active, &self.archive, &self.preferences, 0)?;
        self.v2_initialized = true;
        Ok(())
    }

    fn publish_checkpoint(
        &self,
        active: &[NotificationDocument],
        archive: &[NotificationDocument],
        preferences: &Preferences,
        sequence: u64,
    ) -> io::Result<()> {
        let started = Instant::now();
        atomic_json(
            &self.directory,
            V2_CHECKPOINT,
            &StoreCheckpoint {
                schema_version: DURABLE_SCHEMA_VERSION,
                storage_version: 2,
                sequence,
                active: active.to_vec(),
                archive: archive.to_vec(),
                preferences: preferences.clone(),
            },
        )?;
        self.observe(NotificationCommitStage::CheckpointPublished)?;
        self.directory.atomic_replace(V2_SEGMENT, b"")?;
        for index in 1..=self.segment_index {
            self.directory.remove(&segment_name(index))?;
        }
        self.directory.sync()?;
        self.observe(NotificationCommitStage::SegmentRotated)?;
        eprintln!(
            "event=notification_compaction active={} archive={} elapsed_ms={}",
            active.len(),
            archive.len(),
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn observe(&self, stage: NotificationCommitStage) -> io::Result<()> {
        match &self.commit_observer {
            Some(observer) => observer.reached(stage),
            None => Ok(()),
        }
    }
}

type RecoveredV2State = (
    Vec<NotificationDocument>,
    Vec<NotificationDocument>,
    Preferences,
    u64,
    u64,
    usize,
    usize,
    usize,
    Option<(usize, u64)>,
);

fn read_v2_state(
    directory: &SecureDirectory,
    capacity: usize,
) -> io::Result<Option<RecoveredV2State>> {
    let Some(checkpoint): Option<StoreCheckpoint> = read_json(directory, V2_CHECKPOINT)? else {
        return Ok(None);
    };
    require_durable_version(checkpoint.schema_version)?;
    require_durable_version(checkpoint.preferences.schema_version)?;
    if checkpoint.storage_version != 2 {
        return Err(invalid_data("unsupported notification storage version"));
    }
    let mut active = checkpoint.active;
    let mut archive = checkpoint.archive;
    let mut preferences = checkpoint.preferences;
    let mut latest_sequence = checkpoint.sequence;
    let mut applied = 0_u64;
    let mut segment_index = 0usize;
    let mut segment_bytes = 0usize;
    let mut log_bytes = 0usize;
    let mut tail_repair = None;
    for index in 0..=CHECKPOINT_OPERATION_INTERVAL as usize {
        let name = segment_name(index);
        let Some(segment) = directory.read_optional(&name)? else {
            break;
        };
        if segment.len() > SEGMENT_LIMIT_BYTES {
            return Err(invalid_data("notification segment exceeds 4 MiB"));
        }
        segment_index = index;
        segment_bytes = segment.len();
        log_bytes = log_bytes
            .checked_add(segment.len())
            .ok_or_else(|| invalid_data("notification segment size overflow"))?;
        if log_bytes > CHECKPOINT_LOG_BYTES {
            return Err(invalid_data("notification log exceeds checkpoint interval"));
        }
        let valid_length = segment
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        if valid_length != segment.len() {
            tail_repair = Some((index, valid_length as u64));
        }
        for line in segment[..valid_length].split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let record: SegmentRecord = serde_json::from_slice(line).map_err(invalid_data)?;
            require_durable_version(record.schema_version)?;
            if record.sequence <= checkpoint.sequence {
                continue;
            }
            if record.sequence != latest_sequence.saturating_add(1) {
                return Err(invalid_data(
                    "notification segment sequence is not contiguous",
                ));
            }
            apply_store_operation(
                &mut active,
                &mut archive,
                &mut preferences,
                record.operation,
                capacity,
            )?;
            latest_sequence = record.sequence;
            applied = applied.saturating_add(1);
        }
        if tail_repair.is_some() {
            break;
        }
    }
    validate_unique_ids(&active, &archive)?;
    Ok(Some((
        active,
        archive,
        preferences,
        latest_sequence.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "notification sequence exhausted",
            )
        })?,
        applied,
        segment_bytes,
        log_bytes,
        segment_index,
        tail_repair,
    )))
}

fn apply_store_operation(
    active: &mut Vec<NotificationDocument>,
    archive: &mut Vec<NotificationDocument>,
    preferences: &mut Preferences,
    operation: StoreOperation,
    capacity: usize,
) -> io::Result<()> {
    match operation {
        StoreOperation::SetDnd { enabled } => preferences.dnd = enabled,
        StoreOperation::Push {
            notification,
            archived,
        } => {
            active.retain(|item| item.id != notification.id);
            for archived_notification in archived {
                active.retain(|item| item.id != archived_notification.id);
                archive.retain(|item| item.id != archived_notification.id);
                archive.push(archived_notification);
            }
            active.push(notification.clone());
            preferences.next_notification_id = preferences
                .next_notification_id
                .max(notification.id.saturating_add(1));
            if active.len() > capacity {
                return Err(invalid_data("notification segment exceeds active capacity"));
            }
        }
        StoreOperation::Dismiss { notification } => {
            active.retain(|item| item.id != notification.id);
            archive.retain(|item| item.id != notification.id);
            archive.push(notification);
        }
        StoreOperation::MarkRead { id } => {
            let notification = active
                .iter_mut()
                .find(|notification| notification.id == id)
                .ok_or_else(|| invalid_data("notification segment references a missing id"))?;
            notification.read = true;
        }
        StoreOperation::ExpireActions { ids } => {
            for id in ids {
                let notification = active
                    .iter_mut()
                    .chain(archive.iter_mut())
                    .find(|notification| notification.id == id)
                    .ok_or_else(|| invalid_data("notification segment references a missing id"))?;
                for action in &mut notification.actions {
                    action.state = NotificationActionState::Expired;
                }
            }
        }
        StoreOperation::PurgeArchive => archive.clear(),
    }
    Ok(())
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

fn serialized_documents_size(documents: &[NotificationDocument]) -> io::Result<usize> {
    documents.iter().try_fold(0usize, |total, document| {
        let length = serde_json::to_vec(document).map_err(invalid_data)?.len();
        total
            .checked_add(length)
            .ok_or_else(|| invalid_data("notification aggregate size overflow"))
    })
}

fn aggregate_size_from_parts(
    active_count: usize,
    archive_count: usize,
    active_bytes: usize,
    archive_bytes: usize,
    preferences_bytes: usize,
) -> io::Result<usize> {
    let empty_preferences = Preferences::default();
    let empty_preferences_bytes = serde_json::to_vec(&empty_preferences)
        .map_err(invalid_data)?
        .len();
    let structural = serde_json::to_vec(&Transaction {
        schema_version: DURABLE_SCHEMA_VERSION,
        active: Vec::new(),
        archive: Vec::new(),
        preferences: empty_preferences,
    })
    .map_err(invalid_data)?
    .len()
    .checked_sub(empty_preferences_bytes)
    .ok_or_else(|| invalid_data("notification aggregate size underflow"))?;
    [
        structural,
        preferences_bytes,
        active_bytes,
        archive_bytes,
        active_count.saturating_sub(1),
        archive_count.saturating_sub(1),
    ]
    .into_iter()
    .try_fold(0usize, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| invalid_data("notification aggregate size overflow"))
    })
}

fn ensure_aggregate_parts(
    active_count: usize,
    archive_count: usize,
    active_bytes: usize,
    archive_bytes: usize,
    preferences_bytes: usize,
    limit: usize,
) -> io::Result<()> {
    if aggregate_size_from_parts(
        active_count,
        archive_count,
        active_bytes,
        archive_bytes,
        preferences_bytes,
    )? > limit
    {
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
                    if let Err(error) =
                        cvt(unsafe { libc::mkdirat(descriptor.as_raw_fd(), name.as_ptr(), 0o700) })
                    {
                        if error.kind() != io::ErrorKind::AlreadyExists {
                            return Err(error);
                        }
                    }
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

    fn append(
        &self,
        name: &str,
        bytes: &[u8],
        mut observe: impl FnMut(NotificationCommitStage) -> io::Result<()>,
    ) -> io::Result<()> {
        let name = component_name(OsStr::new(name))?;
        let descriptor = openat_owned(
            self.descriptor.as_raw_fd(),
            &name,
            libc::O_WRONLY
                | libc::O_APPEND
                | libc::O_CREAT
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )?;
        validate_private_file(descriptor.as_raw_fd())?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        observe(NotificationCommitStage::AppendWritten)?;
        observe(NotificationCommitStage::JournalCommitted)?;
        observe(NotificationCommitStage::ActiveCommitted)?;
        file.sync_all()?;
        observe(NotificationCommitStage::AppendFileSynced)?;
        observe(NotificationCommitStage::ArchiveCommitted)?;
        observe(NotificationCommitStage::PreferencesCommitted)?;
        self.sync()?;
        observe(NotificationCommitStage::AppendDirectorySynced)?;
        observe(NotificationCommitStage::JournalRemoved)?;
        observe(NotificationCommitStage::DirectorySynced)
    }

    fn truncate(&self, name: &str, length: u64) -> io::Result<()> {
        let name = component_name(OsStr::new(name))?;
        let descriptor = openat_owned(
            self.descriptor.as_raw_fd(),
            &name,
            libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )?;
        validate_private_file(descriptor.as_raw_fd())?;
        let file = File::from(descriptor);
        file.set_len(length)?;
        file.sync_all()?;
        self.sync()
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
