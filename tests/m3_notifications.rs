use sleepy_sdk::{
    NotificationAction, NotificationActionState, NotificationDocument, NotificationUrgency,
    WIRE_SCHEMA_VERSION,
};
use sleepy_session::notifications::{
    ActionInvocation, FreedesktopNotificationProvider as RealNotificationProvider,
    NotificationCommand, NotificationCommitObserver, NotificationCommitStage,
    NotificationEventService, NotificationStore, NotifyOutcome, NotifyRequest,
    DBUS_NOTIFICATIONS_NAME, DEFAULT_ACTIVE_NOTIFICATION_CAPACITY,
};

struct FreedesktopNotificationProvider {
    runtime: tokio::runtime::Runtime,
    service: NotificationEventService,
    _generation: tempfile::TempDir,
}

impl FreedesktopNotificationProvider {
    fn new(store: NotificationStore) -> io::Result<Self> {
        let provider = RealNotificationProvider::new(store)?;
        let generation = tempfile::tempdir()?;
        let hub = EventHub::new(full_snapshot_event(0)?, 1024);
        let authority = GenerationAuthority::new(
            GenerationAllocator::open(generation.path().join("generation"), 1024)?,
            0,
            hub,
        );
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()?,
            service: NotificationEventService::new(provider, authority),
            _generation: generation,
        })
    }

    fn with_commit_observer(mut self, observer: Arc<dyn NotificationCommitObserver>) -> Self {
        self.service = self.service.with_commit_observer(observer);
        self
    }

    fn notify(&mut self, request: NotifyRequest) -> io::Result<NotifyOutcome> {
        self.runtime.block_on(self.service.notify(request))
    }

    fn execute(&mut self, command: NotificationCommand) -> io::Result<()> {
        self.runtime.block_on(self.service.execute(command))
    }

    fn origin_lost(&mut self, origin: &str) -> io::Result<()> {
        self.runtime.block_on(self.service.origin_lost(origin))
    }

    fn store(&self) -> &NotificationStore {
        self.service.provider().store()
    }

    fn bus_name(&self) -> &'static str {
        self.service.provider().bus_name()
    }

    fn capabilities(&self) -> &'static [&'static str] {
        self.service.provider().capabilities()
    }

    fn popup_visible(&self, id: u64) -> bool {
        self.service.provider().popup_visible(id)
    }

    fn invoke_action(&self, id: u64, action: &str) -> io::Result<ActionInvocation> {
        self.service.provider().invoke_action(id, action)
    }
}
use sleepy_session::sessiond::{
    full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority,
};
use std::{
    io,
    os::unix::fs::{symlink, PermissionsExt},
    sync::{Arc, Mutex},
};

fn notification(id: u64, urgency: NotificationUrgency) -> NotificationDocument {
    NotificationDocument {
        schema_version: WIRE_SCHEMA_VERSION,
        id,
        application_id: "org.example.App".into(),
        summary: format!("Message {id}"),
        body: "<b>literal, never markup</b>".into(),
        urgency,
        created_at: "2026-08-24T21:00:00Z".into(),
        timeout_ms: Some(5000),
        read: false,
        archived: false,
        actions: vec![NotificationAction {
            id: "open".into(),
            label: "Open".into(),
            state: NotificationActionState::Available,
        }],
    }
}

fn fresh_notification(urgency: NotificationUrgency) -> NotificationDocument {
    let mut document = notification(1, urgency);
    document.id = 0;
    document
}

#[test]
fn overflow_and_dismiss_move_notifications_to_durable_archive_without_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 2).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    for origin in [":1.1", ":1.2", ":1.3"] {
        provider
            .notify(NotifyRequest {
                origin: origin.into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .unwrap();
    }

    assert_eq!(
        provider
            .store()
            .active()
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(
        provider
            .store()
            .archive()
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(provider.store().archive()[0].archived);

    provider
        .execute(NotificationCommand::Dismiss { id: 2 })
        .unwrap();
    drop(provider);
    let reopened = NotificationStore::open(temp.path(), 2).unwrap();
    assert_eq!(reopened.active()[0].id, 3);
    assert_eq!(
        reopened
            .archive()
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn dnd_preserves_history_and_unread_but_allows_critical_popups() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .execute(NotificationCommand::SetDnd { enabled: true })
        .unwrap();
    let normal = provider
        .notify(NotifyRequest {
            origin: ":1.4".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    let critical = provider
        .notify(NotifyRequest {
            origin: ":1.5".into(),
            notification: fresh_notification(NotificationUrgency::Critical),
        })
        .unwrap();
    assert!(!normal.popup);
    assert!(critical.popup);

    assert_eq!(provider.store().unread_count(), 2);
    assert_eq!(provider.store().active().len(), 2);
    assert!(NotificationStore::open(temp.path(), 500).unwrap().dnd());
}

#[test]
fn plain_text_is_preserved_and_actions_expire_without_the_source_client() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.6".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider.origin_lost(":1.6").unwrap();

    assert_eq!(
        provider.store().active()[0].body,
        "<b>literal, never markup</b>"
    );
    assert_eq!(
        provider.store().active()[0].actions[0].state,
        NotificationActionState::Expired
    );
}

#[test]
fn typed_provider_owns_the_standard_name_without_advertising_markup() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();

    assert_eq!(provider.bus_name(), DBUS_NOTIFICATIONS_NAME);
    assert!(provider.capabilities().contains(&"actions"));
    assert!(!provider.capabilities().contains(&"body-markup"));

    let delivered = provider
        .notify(NotifyRequest {
            origin: ":1.42".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    assert!(delivered.popup);
    assert_eq!(
        provider.store().active()[0].body,
        "<b>literal, never markup</b>"
    );
}

#[test]
fn typed_commands_group_mark_read_and_expire_only_the_lost_origins_actions() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut second_application = fresh_notification(NotificationUrgency::Normal);
    second_application.application_id = "org.example.Other".into();

    provider
        .notify(NotifyRequest {
            origin: ":1.10".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.20".into(),
            notification: second_application,
        })
        .unwrap();

    provider
        .execute(NotificationCommand::MarkRead { id: 1 })
        .unwrap();
    assert_eq!(provider.store().unread_count(), 1);
    assert_eq!(provider.store().grouped_active().len(), 2);

    provider.origin_lost(":1.10").unwrap();
    assert_eq!(
        provider.store().active()[0].actions[0].state,
        NotificationActionState::Expired
    );
    assert_eq!(
        provider.store().active()[1].actions[0].state,
        NotificationActionState::Available
    );
}

#[test]
fn notification_state_rejects_a_symlinked_writable_directory() {
    let temp = tempfile::tempdir().unwrap();
    let outside = temp.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    let state = temp.path().join("notifications");
    symlink(&outside, &state).unwrap();

    assert!(NotificationStore::open(&state, 500).is_err());
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
}

#[test]
fn notification_writes_stay_on_the_retained_directory_after_path_swap() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("notifications");
    std::fs::create_dir(&state).unwrap();
    let store = NotificationStore::open(&state, 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let retained = temp.path().join("retained");
    std::fs::rename(&state, &retained).unwrap();
    std::fs::create_dir(&state).unwrap();

    provider
        .notify(NotifyRequest {
            origin: ":1.7".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();

    assert!(retained.join("active.json").is_file());
    assert_eq!(std::fs::read_dir(&state).unwrap().count(), 0);
}

struct FailOnceAt {
    target: NotificationCommitStage,
    failed: Mutex<bool>,
}

impl NotificationCommitObserver for FailOnceAt {
    fn reached(&self, stage: NotificationCommitStage) -> io::Result<()> {
        let mut failed = self.failed.lock().unwrap();
        if stage == self.target && !*failed {
            *failed = true;
            Err(io::Error::other("injected commit fault"))
        } else {
            Ok(())
        }
    }
}

struct FailAtOccurrence {
    target: NotificationCommitStage,
    occurrence: usize,
    seen: Mutex<usize>,
}

impl NotificationCommitObserver for FailAtOccurrence {
    fn reached(&self, stage: NotificationCommitStage) -> io::Result<()> {
        if stage != self.target {
            return Ok(());
        }
        let mut seen = self.seen.lock().unwrap();
        *seen += 1;
        if *seen == self.occurrence {
            Err(io::Error::other("injected commit fault"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn every_notification_commit_fault_reconciles_to_one_complete_transaction() {
    for stage in NotificationCommitStage::ALL {
        let temp = tempfile::tempdir().unwrap();
        let store = NotificationStore::open(temp.path(), 500).unwrap();
        let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
        provider
            .notify(NotifyRequest {
                origin: ":1.8".into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .unwrap();
        let mut provider = provider.with_commit_observer(Arc::new(FailOnceAt {
            target: stage,
            failed: Mutex::new(false),
        }));

        assert!(provider
            .notify(NotifyRequest {
                origin: ":1.9".into(),
                notification: fresh_notification(NotificationUrgency::Critical),
            })
            .is_err());
        drop(provider);

        let recovered = NotificationStore::open(temp.path(), 500).unwrap();
        assert_eq!(
            recovered
                .active()
                .iter()
                .map(|item| item.id)
                .collect::<Vec<_>>(),
            [1, 2],
            "failed to reconcile {stage:?}"
        );
        assert!(!temp.path().join("transaction.json").exists());
    }
}

#[test]
fn actions_fail_closed_after_origin_loss_and_only_purge_deletes_archive() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.55".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();

    assert_eq!(provider.invoke_action(1, "open").unwrap().origin, ":1.55");
    provider.origin_lost(":1.55").unwrap();
    assert_eq!(
        provider.invoke_action(1, "open").unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    provider
        .execute(NotificationCommand::Archive { id: 1 })
        .unwrap();
    assert_eq!(provider.store().archive().len(), 1);
    provider
        .execute(NotificationCommand::SetDnd { enabled: true })
        .unwrap();
    assert_eq!(provider.store().archive().len(), 1);
    provider.execute(NotificationCommand::PurgeArchive).unwrap();
    assert!(provider.store().archive().is_empty());
}

#[test]
fn archived_actions_expire_when_their_dbus_origin_disappears() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.88".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .execute(NotificationCommand::Dismiss { id: 1 })
        .unwrap();

    provider.origin_lost(":1.88").unwrap();

    assert_eq!(
        provider.store().archive()[0].actions[0].state,
        NotificationActionState::Expired
    );
}

#[test]
fn oversized_untrusted_notification_text_is_rejected_without_changing_history() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut oversized = fresh_notification(NotificationUrgency::Normal);
    oversized.body = "x".repeat(64 * 1024 + 1);

    let error = provider
        .notify(NotifyRequest {
            origin: ":1.99".into(),
            notification: oversized,
        })
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(provider.store().active().is_empty());
}

#[tokio::test]
async fn notification_mutations_publish_through_the_generation_authority() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 8);
    let mut subscriber = hub.subscribe().await;
    let _replay = subscriber.recv().await.unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 8).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let mut service = NotificationEventService::new(provider, authority);

    service
        .notify(NotifyRequest {
            origin: ":1.101".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .await
        .unwrap();

    let added = subscriber.recv().await.unwrap();
    assert_eq!(added.generation, 1);
    assert!(matches!(
        added.payload,
        sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
            notification_id: 1,
            change: sleepy_sdk::NotificationChange::Added,
        })
    ));

    service
        .execute(NotificationCommand::Dismiss { id: 1 })
        .await
        .unwrap();
    let archived = subscriber.recv().await.unwrap();
    assert_eq!(archived.generation, 2);
    assert!(matches!(
        archived.payload,
        sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
            notification_id: 1,
            change: sleepy_sdk::NotificationChange::Archived,
        })
    ));
}

#[tokio::test]
async fn popup_timeout_expires_presentation_without_deleting_history_or_unread_state() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 8);
    let allocator = GenerationAllocator::open(temp.path().join("popup-generation"), 8).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let mut service = NotificationEventService::new(provider, authority);
    let mut timed = fresh_notification(NotificationUrgency::Normal);
    timed.timeout_ms = Some(20);
    service
        .notify(NotifyRequest {
            origin: ":1.111".into(),
            notification: timed,
        })
        .await
        .unwrap();

    assert!(service.provider().popup_visible(1));
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert_eq!(
        service
            .advance_popup_time(std::time::Instant::now())
            .await
            .unwrap(),
        [1]
    );
    assert!(!service.provider().popup_visible(1));
    assert_eq!(service.provider().store().active().len(), 1);
    assert_eq!(service.provider().store().unread_count(), 1);
}

#[test]
fn default_store_keeps_exactly_the_last_five_hundred_active_notifications() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open_default(temp.path()).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    for id in 1..=501 {
        provider
            .notify(NotifyRequest {
                origin: format!(":1.{id}"),
                notification: fresh_notification(NotificationUrgency::Low),
            })
            .unwrap();
    }

    assert_eq!(DEFAULT_ACTIVE_NOTIFICATION_CAPACITY, 500);
    assert_eq!(provider.store().active().len(), 500);
    assert_eq!(provider.store().active().first().unwrap().id, 2);
    assert_eq!(provider.store().archive().len(), 1);
    assert_eq!(provider.store().archive()[0].id, 1);
}

#[test]
fn unversioned_durable_notification_documents_fail_closed_without_changing_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("active.json");
    let bytes = serde_json::to_vec(&vec![notification(1, NotificationUrgency::Normal)]).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let error = match NotificationStore::open(temp.path(), 500) {
        Ok(_) => panic!("unversioned notification state was accepted"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn enabling_dnd_immediately_suppresses_existing_normal_popups_but_keeps_critical() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.121".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.122".into(),
            notification: fresh_notification(NotificationUrgency::Critical),
        })
        .unwrap();

    provider
        .execute(NotificationCommand::SetDnd { enabled: true })
        .unwrap();

    assert!(!provider.popup_visible(1));
    assert!(provider.popup_visible(2));
    assert_eq!(provider.store().unread_count(), 2);
}

#[test]
fn purging_archive_also_forgets_obsolete_origin_tracking() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.131".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .execute(NotificationCommand::Dismiss { id: 1 })
        .unwrap();
    provider.execute(NotificationCommand::PurgeArchive).unwrap();

    provider.origin_lost(":1.131").unwrap();
}

#[test]
fn provider_allocates_server_owned_ids_for_new_dbus_notifications() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut first = notification(1, NotificationUrgency::Normal);
    first.id = 0;

    let outcome = provider
        .notify(NotifyRequest {
            origin: ":1.141".into(),
            notification: first,
        })
        .unwrap();

    assert_eq!(outcome.id, 1);
    assert_eq!(provider.store().active()[0].id, 1);
}

#[test]
fn replacement_ids_are_same_origin_active_only_and_allocator_survives_purge_restart() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let first = provider
        .notify(NotifyRequest {
            origin: ":1.201".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();

    let mut hostile = fresh_notification(NotificationUrgency::Normal);
    hostile.id = first.id;
    assert_eq!(
        provider
            .notify(NotifyRequest {
                origin: ":1.202".into(),
                notification: hostile,
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );

    let mut replacement = fresh_notification(NotificationUrgency::Normal);
    replacement.id = first.id;
    replacement.summary = "same owner replacement".into();
    provider
        .notify(NotifyRequest {
            origin: ":1.201".into(),
            notification: replacement,
        })
        .unwrap();
    provider
        .execute(NotificationCommand::Dismiss { id: first.id })
        .unwrap();

    let mut archived_reuse = fresh_notification(NotificationUrgency::Normal);
    archived_reuse.id = first.id;
    assert!(provider
        .notify(NotifyRequest {
            origin: ":1.201".into(),
            notification: archived_reuse,
        })
        .is_err());
    provider.execute(NotificationCommand::PurgeArchive).unwrap();
    assert_eq!(
        provider
            .notify(NotifyRequest {
                origin: ":1.201".into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .unwrap()
            .id,
        2
    );
    drop(provider);

    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    assert_eq!(
        provider
            .notify(NotifyRequest {
                origin: ":1.203".into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .unwrap()
            .id,
        3
    );
}

#[test]
fn restart_transactionally_expires_active_and_archived_actions() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.211".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.212".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .execute(NotificationCommand::Dismiss { id: 1 })
        .unwrap();
    drop(provider);

    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    assert_eq!(
        provider.store().active()[0].actions[0].state,
        NotificationActionState::Expired
    );
    assert_eq!(
        provider.store().archive()[0].actions[0].state,
        NotificationActionState::Expired
    );
    drop(provider);
    let reopened = NotificationStore::open(temp.path(), 500).unwrap();
    assert_eq!(
        reopened.active()[0].actions[0].state,
        NotificationActionState::Expired
    );
}

#[test]
fn interrupted_restart_expiry_reconciles_before_actions_can_be_used() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.215".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.216".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .execute(NotificationCommand::Dismiss { id: 1 })
        .unwrap();
    drop(provider);

    let store = NotificationStore::open(temp.path(), 500)
        .unwrap()
        .with_commit_observer(Arc::new(FailOnceAt {
            target: NotificationCommitStage::ActiveCommitted,
            failed: Mutex::new(false),
        }));
    assert!(FreedesktopNotificationProvider::new(store).is_err());

    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    for notification in provider
        .store()
        .active()
        .iter()
        .chain(provider.store().archive())
    {
        assert!(notification
            .actions
            .iter()
            .all(|action| action.state == NotificationActionState::Expired));
    }
    assert!(!temp.path().join("transaction.json").exists());
}

#[test]
fn overflow_atomically_removes_the_archived_notification_popup() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 1).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.221".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.222".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .unwrap();

    assert!(!provider.popup_visible(1));
    assert!(provider.popup_visible(2));
    assert_eq!(provider.store().archive()[0].id, 1);
}

#[test]
fn aggregate_admission_never_commits_state_that_cannot_be_reopened() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open_with_limits(temp.path(), 1, 2_400).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut accepted = 0;
    loop {
        let mut candidate = fresh_notification(NotificationUrgency::Low);
        candidate.body = "x".repeat(300);
        match provider.notify(NotifyRequest {
            origin: format!(":1.30{accepted}"),
            notification: candidate,
        }) {
            Ok(_) => accepted += 1,
            Err(error) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                break;
            }
        }
    }
    assert!(accepted >= 2);
    let active = provider.store().active().to_vec();
    let archive = provider.store().archive().to_vec();
    drop(provider);

    let reopened = NotificationStore::open_with_limits(temp.path(), 1, 2_400).unwrap();
    assert_eq!(reopened.active(), active);
    assert_eq!(reopened.archive(), archive);
}

#[tokio::test]
async fn dnd_purge_and_popup_expiry_each_publish_one_ordered_generation_event() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let mut service = NotificationEventService::new(provider, authority);

    let mut timed = fresh_notification(NotificationUrgency::Critical);
    timed.timeout_ms = Some(1);
    service
        .notify(NotifyRequest {
            origin: ":1.231".into(),
            notification: timed,
        })
        .await
        .unwrap();
    assert_eq!(subscriber.recv().await.unwrap().generation, 1);

    service
        .execute(NotificationCommand::SetDnd { enabled: true })
        .await
        .unwrap();
    let dnd = subscriber.recv().await.unwrap();
    assert_eq!(dnd.generation, 2);
    assert!(matches!(
        dnd.payload,
        sleepy_sdk::SessionEvent::Provider(sleepy_sdk::ProviderEvent {
            ref provider_id,
            online: false,
        }) if provider_id == "org.freedesktop.Notifications.dnd"
    ));

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert_eq!(
        service
            .advance_popup_time(std::time::Instant::now())
            .await
            .unwrap(),
        [1]
    );
    let expired = subscriber.recv().await.unwrap();
    assert_eq!(expired.generation, 3);
    assert!(matches!(
        expired.payload,
        sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
            notification_id: 1,
            change: sleepy_sdk::NotificationChange::Updated,
        })
    ));

    service
        .execute(NotificationCommand::Dismiss { id: 1 })
        .await
        .unwrap();
    assert_eq!(subscriber.recv().await.unwrap().generation, 4);
    service
        .execute(NotificationCommand::PurgeArchive)
        .await
        .unwrap();
    let purged = subscriber.recv().await.unwrap();
    assert_eq!(purged.generation, 5);
    assert!(matches!(
        purged.payload,
        sleepy_sdk::SessionEvent::Provider(sleepy_sdk::ProviderEvent {
            ref provider_id,
            online: true,
        }) if provider_id == "org.freedesktop.Notifications.archive"
    ));
}

#[tokio::test]
async fn overflow_publishes_archived_ids_before_the_incoming_notification() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 2).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        0,
        hub,
    );
    let mut service = NotificationEventService::new(provider, authority);

    for origin in [":1.301", ":1.302"] {
        service
            .notify(NotifyRequest {
                origin: origin.into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .await
            .unwrap();
        subscriber.recv().await.unwrap();
    }
    service
        .notify(NotifyRequest {
            origin: ":1.303".into(),
            notification: fresh_notification(NotificationUrgency::Normal),
        })
        .await
        .unwrap();

    let archived = subscriber.recv().await.unwrap();
    assert_eq!(archived.generation, 3);
    assert!(matches!(
        archived.payload,
        sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
            notification_id: 1,
            change: sleepy_sdk::NotificationChange::Archived,
        })
    ));
    let added = subscriber.recv().await.unwrap();
    assert_eq!(added.generation, 4);
    assert!(matches!(
        added.payload,
        sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
            notification_id: 3,
            change: sleepy_sdk::NotificationChange::Added,
        })
    ));
}

#[tokio::test]
async fn origin_loss_fault_reconciles_all_matching_actions_without_partial_events() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("notifications");
    let store = NotificationStore::open(&state, 500).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        0,
        hub,
    );
    let mut service = NotificationEventService::new(provider, authority).with_commit_observer(
        Arc::new(FailAtOccurrence {
            target: NotificationCommitStage::ActiveCommitted,
            occurrence: 4,
            seen: Mutex::new(0),
        }),
    );
    for _ in 0..2 {
        service
            .notify(NotifyRequest {
                origin: ":1.311".into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .await
            .unwrap();
        subscriber.recv().await.unwrap();
    }
    service
        .execute(NotificationCommand::Dismiss { id: 1 })
        .await
        .unwrap();
    subscriber.recv().await.unwrap();

    assert!(service.origin_lost(":1.311").await.is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), subscriber.recv())
            .await
            .is_err()
    );
    drop(service);

    let recovered = NotificationStore::open(&state, 500).unwrap();
    assert_eq!(recovered.active().len(), 1);
    assert_eq!(recovered.archive().len(), 1);
    for notification in recovered.active().iter().chain(recovered.archive()) {
        assert!(notification
            .actions
            .iter()
            .all(|action| action.state == NotificationActionState::Expired));
    }
}

#[tokio::test]
async fn successful_origin_loss_publishes_every_expiry_in_id_order() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = RealNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        0,
        hub,
    );
    let mut service = NotificationEventService::new(provider, authority);
    for _ in 0..3 {
        service
            .notify(NotifyRequest {
                origin: ":1.321".into(),
                notification: fresh_notification(NotificationUrgency::Normal),
            })
            .await
            .unwrap();
        subscriber.recv().await.unwrap();
    }

    service.origin_lost(":1.321").await.unwrap();
    for (generation, id) in [(4, 1), (5, 2), (6, 3)] {
        let event = subscriber.recv().await.unwrap();
        assert_eq!(event.generation, generation);
        assert!(matches!(
            event.payload,
            sleepy_sdk::SessionEvent::Notification(sleepy_sdk::NotificationEvent {
                notification_id,
                change: sleepy_sdk::NotificationChange::ActionExpired,
            }) if notification_id == id
        ));
    }
}
