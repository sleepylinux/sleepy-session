// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fmt,
    future::Future,
    io,
    path::Path,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    validate_theme_document, EventCause, EventCauseKind, SemanticColors, SessionEvent,
    ThemeAppearance, ThemeDocument, ThemeEffects, ThemeEvent, ThemeOrigin,
};

use crate::{
    sessiond::{GenerationAuthority, GenerationGuard},
    store::{SecureDir, StoreError},
    system::RunControl,
};

const CURRENT: &str = "current.json";
const JOURNAL: &str = "apply-journal.json";

#[derive(Debug)]
pub struct ThemeError {
    kind: ThemeErrorKind,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeErrorKind {
    Busy,
    Timeout,
    Cancelled,
    Other,
}

impl ThemeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            kind: ThemeErrorKind::Other,
            message: message.into(),
        }
    }

    fn controlled(kind: ThemeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> ThemeErrorKind {
        self.kind
    }
}

impl fmt::Display for ThemeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ThemeError {}

impl From<StoreError> for ThemeError {
    fn from(error: StoreError) -> Self {
        Self::new(error.to_string())
    }
}

impl From<serde_json::Error> for ThemeError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

impl From<io::Error> for ThemeError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

pub trait DesktopThemeSink: Send + Sync {
    fn acknowledge<'a>(
        &'a self,
        theme: &'a ThemeDocument,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeApplyStage {
    JournalWritten,
    DesktopAcknowledged,
    GenerationCommitted,
    CurrentWritten,
    RollbackAcknowledged,
    RollbackWritten,
    JournalRemoveStarted,
    JournalRemoved,
    JournalDirectorySynced,
    CleanupRecoveryWritten,
}

pub trait ThemeTransactionObserver: Send {
    fn observe(&mut self, _stage: ThemeApplyStage) -> Result<(), String> {
        Ok(())
    }
}

struct NoopObserver;
impl ThemeTransactionObserver for NoopObserver {}

#[derive(Debug, Clone)]
pub struct AppliedTheme {
    pub theme: ThemeDocument,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThemeReconciliationOutcome {
    pub reconciled: bool,
    pub generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApplyJournal {
    schema_version: u32,
    request_id: String,
    previous: ThemeDocument,
    candidate: ThemeDocument,
}

#[derive(Clone, Copy)]
struct ApplyRequest<'a> {
    theme_id: &'a str,
    request_id: &'a str,
    expected_generation: u64,
}

pub struct ThemeManager {
    documents: SecureDir,
    state: SecureDir,
    preview: Option<ThemeDocument>,
    acknowledgement_timeout: Duration,
}

impl ThemeManager {
    pub fn open(
        config_root: impl AsRef<Path>,
        state_root: impl AsRef<Path>,
    ) -> Result<Self, ThemeError> {
        Self::open_with_acknowledgement_timeout(config_root, state_root, Duration::from_secs(2))
    }

    pub fn open_with_acknowledgement_timeout(
        config_root: impl AsRef<Path>,
        state_root: impl AsRef<Path>,
        acknowledgement_timeout: Duration,
    ) -> Result<Self, ThemeError> {
        if acknowledgement_timeout.is_zero() {
            return Err(ThemeError::new(
                "theme acknowledgement timeout must be positive",
            ));
        }
        let config = SecureDir::open_writable(config_root.as_ref(), true)?;
        let config = config.child_writable(OsStr::new("sleepy"), true)?;
        let documents = config.child_writable(OsStr::new("themes"), true)?;
        let state = SecureDir::open_writable(state_root.as_ref(), true)?;
        let state = state.child_writable(OsStr::new("sleepy"), true)?;
        let state = state.child_writable(OsStr::new("themes"), true)?;
        documents.enforce_private_directory()?;
        state.enforce_private_directory()?;
        documents.validate_private_file_if_present(OsStr::new(CURRENT))?;
        state.validate_private_file_if_present(OsStr::new(CURRENT))?;
        state.validate_private_file_if_present(OsStr::new(JOURNAL))?;
        Ok(Self {
            documents,
            state,
            preview: None,
            acknowledgement_timeout,
        })
    }

    pub fn builtin(id: &str) -> Option<ThemeDocument> {
        match id {
            "builtin.sleepy-dark" => Some(builtin(
                id,
                "Sleepy Dark",
                ThemeAppearance::Dark,
                ["#10131A", "#181D27", "#F7F9FC", "#D8DEE9", "#8EC5FF"],
            )),
            "builtin.sleepy-light" => Some(builtin(
                id,
                "Sleepy Light",
                ThemeAppearance::Light,
                ["#F7F9FC", "#FFFFFF", "#151A22", "#394252", "#005FB8"],
            )),
            "builtin.sleepy-system" => Some(builtin(
                id,
                "Sleepy System",
                ThemeAppearance::System,
                ["#10131A", "#181D27", "#F7F9FC", "#D8DEE9", "#8EC5FF"],
            )),
            _ => None,
        }
    }

    pub fn theme(&self, id: &str) -> Result<ThemeDocument, ThemeError> {
        if let Some(theme) = Self::builtin(id) {
            return Ok(theme);
        }
        let name = theme_file_name(id)?;
        let bytes = self.documents.read(OsStr::new(&name))?;
        parse_document(&bytes)
    }

    pub fn current(&self) -> Result<ThemeDocument, ThemeError> {
        match self.state.read_optional(OsStr::new(CURRENT))? {
            Some(bytes) => parse_document(&bytes),
            None => Ok(Self::builtin("builtin.sleepy-dark").expect("static builtin")),
        }
    }

    pub fn import(&self, input: &str) -> Result<ThemeDocument, ThemeError> {
        self.import_controlled(input, &default_control())
    }

    pub fn import_controlled(
        &self,
        input: &str,
        control: &RunControl,
    ) -> Result<ThemeDocument, ThemeError> {
        let lock = self.mutation_lock_blocking("theme import", control)?;
        let result = self.import_unlocked(input);
        drop(lock);
        result
    }

    pub async fn import_async_controlled(
        &self,
        input: &str,
        control: &RunControl,
    ) -> Result<ThemeDocument, ThemeError> {
        let _lock = self.mutation_lock_async("theme import", control).await?;
        self.import_unlocked(input)
    }

    fn import_unlocked(&self, input: &str) -> Result<ThemeDocument, ThemeError> {
        let mut document =
            validate_theme_document(input).map_err(|error| ThemeError::new(error.to_string()))?;
        document.id = uuid::Uuid::new_v4().to_string();
        document.origin = ThemeOrigin::User;
        validate_document(&document)?;
        self.write_user(&document)?;
        Ok(document)
    }

    pub fn copy_for_edit(&self, id: &str, name: &str) -> Result<ThemeDocument, ThemeError> {
        self.copy_for_edit_controlled(id, name, &default_control())
    }

    pub fn copy_for_edit_controlled(
        &self,
        id: &str,
        name: &str,
        control: &RunControl,
    ) -> Result<ThemeDocument, ThemeError> {
        let lock = self.mutation_lock_blocking("theme copy", control)?;
        let result = self.copy_for_edit_unlocked(id, name);
        drop(lock);
        result
    }

    pub async fn copy_for_edit_async_controlled(
        &self,
        id: &str,
        name: &str,
        control: &RunControl,
    ) -> Result<ThemeDocument, ThemeError> {
        let _lock = self.mutation_lock_async("theme copy", control).await?;
        self.copy_for_edit_unlocked(id, name)
    }

    fn copy_for_edit_unlocked(&self, id: &str, name: &str) -> Result<ThemeDocument, ThemeError> {
        if name.trim().is_empty() {
            return Err(ThemeError::new("theme name must not be empty"));
        }
        let mut copy = self.theme(id)?;
        copy.id = uuid::Uuid::new_v4().to_string();
        copy.name = name.trim().to_owned();
        copy.origin = ThemeOrigin::User;
        validate_document(&copy)?;
        self.write_user(&copy)?;
        Ok(copy)
    }

    pub fn delete(&self, id: &str) -> Result<(), ThemeError> {
        self.delete_controlled(id, &default_control())
    }

    pub fn delete_controlled(&self, id: &str, control: &RunControl) -> Result<(), ThemeError> {
        let lock = self.mutation_lock_blocking("theme delete", control)?;
        let result = self.delete_unlocked(id);
        drop(lock);
        result
    }

    pub async fn delete_async_controlled(
        &self,
        id: &str,
        control: &RunControl,
    ) -> Result<(), ThemeError> {
        let _lock = self.mutation_lock_async("theme delete", control).await?;
        self.delete_unlocked(id)
    }

    fn delete_unlocked(&self, id: &str) -> Result<(), ThemeError> {
        if Self::builtin(id).is_some() {
            return Err(ThemeError::new("built-in themes are immutable"));
        }
        if self.current()?.id == id {
            return Err(ThemeError::new("the current theme cannot be deleted"));
        }
        self.documents
            .remove_file(OsStr::new(&theme_file_name(id)?))?;
        self.documents.sync()?;
        Ok(())
    }

    pub fn preview(&mut self, id: &str) -> Result<&ThemeDocument, ThemeError> {
        self.preview = Some(self.theme(id)?);
        Ok(self.preview.as_ref().expect("preview was set"))
    }

    pub fn previewed(&self) -> Option<&ThemeDocument> {
        self.preview.as_ref()
    }

    pub fn clear_preview(&mut self) {
        self.preview = None;
    }

    pub async fn apply(
        &mut self,
        theme_id: &str,
        request_id: &str,
        expected_generation: u64,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
    ) -> Result<AppliedTheme, ThemeError> {
        let control = default_control();
        self.apply_controlled(
            theme_id,
            request_id,
            expected_generation,
            sink,
            authority,
            &control,
        )
        .await
    }

    pub async fn apply_controlled(
        &mut self,
        theme_id: &str,
        request_id: &str,
        expected_generation: u64,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
        control: &RunControl,
    ) -> Result<AppliedTheme, ThemeError> {
        self.apply_transaction(
            ApplyRequest {
                theme_id,
                request_id,
                expected_generation,
            },
            sink,
            authority,
            control,
            &mut NoopObserver,
        )
        .await
    }

    pub async fn apply_observed(
        &mut self,
        theme_id: &str,
        request_id: &str,
        expected_generation: u64,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
        observer: &mut dyn ThemeTransactionObserver,
    ) -> Result<AppliedTheme, ThemeError> {
        let control = default_control();
        self.apply_transaction(
            ApplyRequest {
                theme_id,
                request_id,
                expected_generation,
            },
            sink,
            authority,
            &control,
            observer,
        )
        .await
    }

    async fn apply_transaction(
        &mut self,
        request: ApplyRequest<'_>,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
        control: &RunControl,
        observer: &mut dyn ThemeTransactionObserver,
    ) -> Result<AppliedTheme, ThemeError> {
        let _lock = self
            .mutation_lock_async("theme transaction", control)
            .await?;
        uuid::Uuid::parse_str(request.request_id)
            .map_err(|_| ThemeError::new("request id must be a UUID"))?;
        if self.has_journal()? {
            return Err(ThemeError::new(
                "a theme transaction already needs reconciliation",
            ));
        }
        let mut generation = authority.lock().await;
        if generation.current_generation() != request.expected_generation {
            return Err(ThemeError::new(format!(
                "stale theme generation: expected {}, current {}",
                request.expected_generation,
                generation.current_generation()
            )));
        }
        let candidate = self.theme(request.theme_id)?;
        let previous = self.current()?;
        let journal = ApplyJournal {
            schema_version: 1,
            request_id: request.request_id.to_owned(),
            previous: previous.clone(),
            candidate: candidate.clone(),
        };
        let journal_bytes = json_bytes(&journal)?;
        self.atomic_state_write(JOURNAL, &journal_bytes)?;

        let outcome = self
            .continue_apply(
                &candidate,
                request.request_id,
                sink,
                &mut generation,
                control,
                observer,
            )
            .await;
        match outcome {
            Ok(applied_generation) => {
                if let Err(cleanup_error) = self.cleanup_journal(observer) {
                    let recovery_error = match self.atomic_state_write(JOURNAL, &journal_bytes) {
                        Ok(()) => {
                            observe(observer, ThemeApplyStage::CleanupRecoveryWritten)?;
                            None
                        }
                        Err(error) => Some(error),
                    };
                    let rollback = self
                        .rollback(
                            &journal,
                            sink,
                            &mut generation,
                            control,
                            observer,
                            &journal_bytes,
                        )
                        .await;
                    return match rollback {
                        Ok(_) => match recovery_error {
                            Some(recovery) => Err(ThemeError::new(format!(
                                "{cleanup_error}; cleanup recovery write failed before confirmed rollback: {recovery}"
                            ))),
                            None => Err(cleanup_error),
                        },
                        Err(rollback) => Err(ThemeError::new(format!(
                            "{cleanup_error}; confirmed rollback failed: {rollback}"
                        ))),
                    };
                }
                self.preview = None;
                Ok(AppliedTheme {
                    theme: candidate,
                    generation: applied_generation,
                })
            }
            Err(error) => {
                let rollback = self
                    .rollback(
                        &journal,
                        sink,
                        &mut generation,
                        control,
                        observer,
                        &journal_bytes,
                    )
                    .await;
                match rollback {
                    Ok(_) => Err(error),
                    Err(rollback) => Err(ThemeError::controlled(
                        error.kind(),
                        format!("{error}; confirmed rollback failed: {rollback}"),
                    )),
                }
            }
        }
    }

    async fn continue_apply(
        &self,
        candidate: &ThemeDocument,
        request_id: &str,
        sink: &dyn DesktopThemeSink,
        generation: &mut GenerationGuard<'_>,
        control: &RunControl,
        observer: &mut dyn ThemeTransactionObserver,
    ) -> Result<u64, ThemeError> {
        observe(observer, ThemeApplyStage::JournalWritten)?;
        self.acknowledge(sink, candidate, "desktop acknowledgement", control)
            .await?;
        observe(observer, ThemeApplyStage::DesktopAcknowledged)?;
        ensure_active(control, "theme apply")?;
        let event = generation
            .publish(
                EventCause {
                    kind: EventCauseKind::Request,
                    request_id: Some(request_id.to_owned()),
                },
                SessionEvent::Theme(ThemeEvent {
                    theme_id: candidate.id.clone(),
                    applied: true,
                }),
            )
            .await?;
        observe(observer, ThemeApplyStage::GenerationCommitted)?;
        self.atomic_state_write(CURRENT, &document_bytes(candidate)?)?;
        observe(observer, ThemeApplyStage::CurrentWritten)?;
        Ok(event.generation)
    }

    async fn rollback(
        &self,
        journal: &ApplyJournal,
        sink: &dyn DesktopThemeSink,
        generation: &mut GenerationGuard<'_>,
        control: &RunControl,
        observer: &mut dyn ThemeTransactionObserver,
        journal_bytes: &[u8],
    ) -> Result<u64, ThemeError> {
        self.acknowledge(
            sink,
            &journal.previous,
            "desktop rollback acknowledgement",
            control,
        )
        .await?;
        observe(observer, ThemeApplyStage::RollbackAcknowledged)?;
        ensure_active(control, "theme rollback")?;
        self.atomic_state_write(CURRENT, &document_bytes(&journal.previous)?)?;
        observe(observer, ThemeApplyStage::RollbackWritten)?;
        let event = generation
            .publish(
                EventCause {
                    kind: EventCauseKind::Request,
                    request_id: Some(journal.request_id.clone()),
                },
                SessionEvent::Theme(ThemeEvent {
                    theme_id: journal.previous.id.clone(),
                    applied: true,
                }),
            )
            .await?;
        if let Err(error) = self.cleanup_journal(&mut NoopObserver) {
            self.atomic_state_write(JOURNAL, journal_bytes)?;
            return Err(ThemeError::new(format!(
                "rollback journal cleanup failed and was restored: {error}"
            )));
        }
        Ok(event.generation)
    }

    pub async fn reconcile(
        &mut self,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
    ) -> Result<ThemeReconciliationOutcome, ThemeError> {
        let control = default_control();
        self.reconcile_controlled(sink, authority, &control).await
    }

    pub async fn reconcile_controlled(
        &mut self,
        sink: &dyn DesktopThemeSink,
        authority: &GenerationAuthority,
        control: &RunControl,
    ) -> Result<ThemeReconciliationOutcome, ThemeError> {
        let _lock = self
            .mutation_lock_async("theme reconciliation", control)
            .await?;
        let Some(bytes) = self.state.read_optional(OsStr::new(JOURNAL))? else {
            return Ok(ThemeReconciliationOutcome {
                reconciled: false,
                generation: None,
            });
        };
        let journal: ApplyJournal = serde_json::from_slice(&bytes)?;
        if journal.schema_version != 1 {
            return Err(ThemeError::new("unknown theme journal schema version"));
        }
        validate_document(&journal.previous)?;
        validate_document(&journal.candidate)?;
        let mut generation = authority.lock().await;
        let generation = self
            .rollback(
                &journal,
                sink,
                &mut generation,
                control,
                &mut NoopObserver,
                &bytes,
            )
            .await?;
        self.preview = None;
        Ok(ThemeReconciliationOutcome {
            reconciled: true,
            generation: Some(generation),
        })
    }

    pub fn has_journal(&self) -> Result<bool, ThemeError> {
        Ok(self.state.exists(OsStr::new(JOURNAL))?)
    }

    pub fn durable_state_bytes(&self) -> Result<Vec<u8>, ThemeError> {
        let mut bytes = Vec::new();
        for name in self.documents.entries()? {
            bytes.extend_from_slice(name.as_encoded_bytes());
            bytes.extend_from_slice(&self.documents.read(&name)?);
        }
        for name in [CURRENT, JOURNAL] {
            if let Some(value) = self.state.read_optional(OsStr::new(name))? {
                bytes.extend_from_slice(name.as_bytes());
                bytes.extend_from_slice(&value);
            }
        }
        Ok(bytes)
    }

    #[doc(hidden)]
    pub fn seed_crash_journal_for_test(&self, candidate_id: &str) -> Result<(), ThemeError> {
        let lock = self.mutation_lock_blocking("theme test journal", &default_control())?;
        let journal = ApplyJournal {
            schema_version: 1,
            request_id: uuid::Uuid::new_v4().to_string(),
            previous: self.current()?,
            candidate: self.theme(candidate_id)?,
        };
        let result = self.atomic_state_write(JOURNAL, &json_bytes(&journal)?);
        drop(lock);
        result
    }

    fn write_user(&self, document: &ThemeDocument) -> Result<(), ThemeError> {
        let name = theme_file_name(&document.id)?;
        self.documents.atomic_replace(
            OsStr::new(&name),
            &document_bytes(document)?,
            || Ok(()),
            || Ok(()),
            || Ok(()),
        )?;
        Ok(())
    }

    fn atomic_state_write(&self, name: &str, bytes: &[u8]) -> Result<(), ThemeError> {
        self.state
            .atomic_replace(OsStr::new(name), bytes, || Ok(()), || Ok(()), || Ok(()))?;
        Ok(())
    }

    async fn acknowledge(
        &self,
        sink: &dyn DesktopThemeSink,
        theme: &ThemeDocument,
        operation: &str,
        control: &RunControl,
    ) -> Result<(), ThemeError> {
        ensure_active(control, operation)?;
        let timeout = self.acknowledgement_timeout.min(control.remaining());
        if timeout.is_zero() {
            return Err(ThemeError::controlled(
                ThemeErrorKind::Timeout,
                format!("{operation} timed out"),
            ));
        }
        let acknowledgement = sink.acknowledge(theme);
        tokio::pin!(acknowledgement);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            ensure_active(control, operation)?;
            let tick = Duration::from_millis(5).min(control.remaining());
            tokio::select! {
                result = &mut acknowledgement => return result
                    .map_err(|error| ThemeError::new(format!("{operation} failed: {error}"))),
                _ = tokio::time::sleep_until(deadline) => return Err(ThemeError::controlled(
                    ThemeErrorKind::Timeout,
                    format!("{operation} timed out"),
                )),
                _ = tokio::time::sleep(tick) => {}
            }
        }
    }

    fn mutation_lock_blocking(
        &self,
        operation: &str,
        control: &RunControl,
    ) -> Result<std::fs::File, ThemeError> {
        let lock = self.state.open_lock(OsStr::new("apply.lock"))?;
        loop {
            ensure_active(control, operation)?;
            match FileExt::try_lock_exclusive(&lock) {
                Ok(()) => return Ok(lock),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5).min(control.remaining()));
                }
                Err(error) => {
                    return Err(ThemeError::new(format!("{operation} lock failed: {error}")))
                }
            }
        }
    }

    async fn mutation_lock_async(
        &self,
        operation: &str,
        control: &RunControl,
    ) -> Result<std::fs::File, ThemeError> {
        let lock = self.state.open_lock(OsStr::new("apply.lock"))?;
        loop {
            ensure_active(control, operation)?;
            match FileExt::try_lock_exclusive(&lock) {
                Ok(()) => return Ok(lock),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(5).min(control.remaining())).await;
                }
                Err(error) => {
                    return Err(ThemeError::new(format!("{operation} lock failed: {error}")))
                }
            }
        }
    }

    fn cleanup_journal(
        &self,
        observer: &mut dyn ThemeTransactionObserver,
    ) -> Result<(), ThemeError> {
        observe(observer, ThemeApplyStage::JournalRemoveStarted)?;
        self.state.remove_file(OsStr::new(JOURNAL))?;
        observe(observer, ThemeApplyStage::JournalRemoved)?;
        self.state.sync()?;
        observe(observer, ThemeApplyStage::JournalDirectorySynced)
    }
}

fn default_control() -> RunControl {
    RunControl::for_request(
        Instant::now() + Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    )
}

fn ensure_active(control: &RunControl, operation: &str) -> Result<(), ThemeError> {
    if control.is_cancelled() {
        return Err(ThemeError::controlled(
            ThemeErrorKind::Cancelled,
            format!("{operation} was cancelled"),
        ));
    }
    if control.remaining().is_zero() {
        return Err(ThemeError::controlled(
            ThemeErrorKind::Timeout,
            format!("{operation} lock timed out"),
        ));
    }
    Ok(())
}

fn observe(
    observer: &mut dyn ThemeTransactionObserver,
    stage: ThemeApplyStage,
) -> Result<(), ThemeError> {
    observer
        .observe(stage)
        .map_err(|error| ThemeError::new(format!("theme transaction fault at {stage:?}: {error}")))
}

fn theme_file_name(id: &str) -> Result<String, ThemeError> {
    let id =
        uuid::Uuid::parse_str(id).map_err(|_| ThemeError::new("user theme id must be UUID"))?;
    Ok(format!("{}.json", id.hyphenated()))
}

fn parse_document(bytes: &[u8]) -> Result<ThemeDocument, ThemeError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ThemeError::new("theme must be UTF-8"))?;
    validate_theme_document(text).map_err(|error| ThemeError::new(error.to_string()))
}

fn validate_document(document: &ThemeDocument) -> Result<(), ThemeError> {
    parse_document(&serde_json::to_vec(document)?)?;
    for (name, foreground) in [
        ("accent", &document.colors.accent),
        ("control", &document.colors.control),
    ] {
        for (surface_name, surface) in [
            ("background", &document.colors.background),
            ("surface", &document.colors.surface),
        ] {
            if local_contrast_ratio(foreground, surface)? < 3.0 {
                return Err(ThemeError::new(format!(
                    "theme {name}/{surface_name} contrast must be at least 3:1"
                )));
            }
        }
    }
    Ok(())
}

fn local_contrast_ratio(first: &str, second: &str) -> Result<f64, ThemeError> {
    fn luminance(color: &str) -> Result<f64, ThemeError> {
        if color.len() != 7 || !color.starts_with('#') {
            return Err(ThemeError::new("theme color must be #RRGGBB"));
        }
        let component = |range: std::ops::Range<usize>| {
            u8::from_str_radix(&color[range], 16)
                .map_err(|_| ThemeError::new("theme color must be #RRGGBB"))
                .map(|value| {
                    let value = f64::from(value) / 255.0;
                    if value <= 0.04045 {
                        value / 12.92
                    } else {
                        ((value + 0.055) / 1.055).powf(2.4)
                    }
                })
        };
        Ok(0.2126 * component(1..3)? + 0.7152 * component(3..5)? + 0.0722 * component(5..7)?)
    }
    let first = luminance(first)?;
    let second = luminance(second)?;
    Ok((first.max(second) + 0.05) / (first.min(second) + 0.05))
}

fn document_bytes(document: &ThemeDocument) -> Result<Vec<u8>, ThemeError> {
    validate_document(document)?;
    json_bytes(document)
}

fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>, ThemeError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn builtin(id: &str, name: &str, appearance: ThemeAppearance, colors: [&str; 5]) -> ThemeDocument {
    let [background, surface, text_primary, text_secondary, accent] = colors;
    ThemeDocument {
        schema_version: 1,
        id: id.into(),
        name: name.into(),
        origin: ThemeOrigin::Builtin,
        appearance,
        effects: ThemeEffects::Full,
        reduced_motion: false,
        opaque_fallback: false,
        colors: SemanticColors {
            background: background.into(),
            surface: surface.into(),
            text_primary: text_primary.into(),
            text_secondary: text_secondary.into(),
            accent: accent.into(),
            control: text_primary.into(),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalColorScheme {
    Dark,
    Light,
}

pub trait ColorSchemePortal {
    fn color_scheme(&self) -> Result<PortalColorScheme, String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectsPolicy {
    pub appearance: PortalColorScheme,
    pub blur: bool,
    pub animations: bool,
    pub translucency: bool,
    pub opaque: bool,
}

impl EffectsPolicy {
    pub fn resolve(
        theme: &ThemeDocument,
        portal: &dyn ColorSchemePortal,
    ) -> Result<Self, ThemeError> {
        validate_document(theme)?;
        let appearance = match theme.appearance {
            ThemeAppearance::Dark => PortalColorScheme::Dark,
            ThemeAppearance::Light => PortalColorScheme::Light,
            ThemeAppearance::System => portal
                .color_scheme()
                .map_err(|error| ThemeError::new(format!("color-scheme portal failed: {error}")))?,
        };
        let (blur, animations, translucency) = match theme.effects {
            ThemeEffects::Full => (true, !theme.reduced_motion, true),
            ThemeEffects::Reduced => (false, false, true),
            ThemeEffects::None => (false, false, false),
        };
        Ok(Self {
            appearance,
            blur: blur && !theme.opaque_fallback,
            animations,
            translucency: translucency && !theme.opaque_fallback,
            opaque: theme.opaque_fallback || !translucency,
        })
    }
}

pub fn derive_wallpaper_palette(pixels: &[u8]) -> Result<SemanticColors, ThemeError> {
    if pixels.is_empty() || !pixels.len().is_multiple_of(3) {
        return Err(ThemeError::new(
            "wallpaper pixels must be non-empty RGB triples",
        ));
    }
    const MAX_PIXELS: usize = 1_000_000;
    if pixels.len() / 3 > MAX_PIXELS {
        return Err(ThemeError::new(
            "wallpaper palette input exceeds one million pixels",
        ));
    }
    let mut frequency = BTreeMap::<(u8, u8, u8), usize>::new();
    for pixel in pixels.chunks_exact(3) {
        *frequency.entry((pixel[0], pixel[1], pixel[2])).or_default() += 1;
    }
    let background = frequency
        .keys()
        .copied()
        .min_by_key(|&(r, g, b)| luminance_key(r, g, b))
        .expect("non-empty pixels");
    let light_background = luminance_key(background.0, background.1, background.2) > 140_000;
    let text = if light_background {
        (0, 0, 0)
    } else {
        (255, 255, 255)
    };
    let accent = frequency
        .iter()
        .max_by_key(|(&(r, g, b), &count)| {
            let saturation = u16::from(r.max(g).max(b)) - u16::from(r.min(g).min(b));
            (saturation, count, (r, g, b))
        })
        .map(|(&color, _)| color)
        .unwrap_or(text);
    let mut colors = SemanticColors {
        background: hex(background),
        surface: hex(background),
        text_primary: hex(text),
        text_secondary: hex(text),
        accent: hex(accent),
        control: hex(text),
    };
    let probe = ThemeDocument {
        schema_version: 1,
        id: uuid::Uuid::nil().to_string(),
        name: "Wallpaper palette".into(),
        origin: ThemeOrigin::User,
        appearance: if light_background {
            ThemeAppearance::Light
        } else {
            ThemeAppearance::Dark
        },
        effects: ThemeEffects::Full,
        reduced_motion: false,
        opaque_fallback: false,
        colors: colors.clone(),
    };
    if validate_document(&probe).is_err() {
        colors.accent = hex(text);
    }
    let final_probe = ThemeDocument {
        colors: colors.clone(),
        ..probe
    };
    validate_document(&final_probe)?;
    Ok(colors)
}

fn luminance_key(r: u8, g: u8, b: u8) -> u32 {
    2126 * u32::from(r) + 7152 * u32::from(g) + 722 * u32::from(b)
}

fn hex((r, g, b): (u8, u8, u8)) -> String {
    format!("#{r:02X}{g:02X}{b:02X}")
}
