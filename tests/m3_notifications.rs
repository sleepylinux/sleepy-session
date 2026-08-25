use sleepy_sdk::{
    NotificationAction, NotificationActionState, NotificationDocument, NotificationUrgency,
    WIRE_SCHEMA_VERSION,
};
use sleepy_session::notifications::{
    FreedesktopNotificationProvider, NotificationCommand, NotificationCommitObserver,
    NotificationCommitStage, NotificationEventService, NotificationStore, NotifyRequest,
    DBUS_NOTIFICATIONS_NAME, DEFAULT_ACTIVE_NOTIFICATION_CAPACITY,
};
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

#[test]
fn overflow_and_dismiss_move_notifications_to_durable_archive_without_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = NotificationStore::open(temp.path(), 2).unwrap();
    store
        .push(notification(1, NotificationUrgency::Normal))
        .unwrap();
    store
        .push(notification(2, NotificationUrgency::Normal))
        .unwrap();
    store
        .push(notification(3, NotificationUrgency::Normal))
        .unwrap();

    assert_eq!(
        store
            .active()
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(
        store
            .archive()
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(store.archive()[0].archived);

    store.dismiss(2).unwrap();
    drop(store);
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
    let mut store = NotificationStore::open(temp.path(), 500).unwrap();
    store.set_dnd(true).unwrap();
    let normal = notification(1, NotificationUrgency::Normal);
    let critical = notification(2, NotificationUrgency::Critical);
    assert!(!store.popup_allowed(&normal));
    assert!(store.popup_allowed(&critical));
    store.push(normal).unwrap();
    store.push(critical).unwrap();

    assert_eq!(store.unread_count(), 2);
    assert_eq!(store.active().len(), 2);
    assert!(NotificationStore::open(temp.path(), 500).unwrap().dnd());
}

#[test]
fn plain_text_is_preserved_and_actions_expire_without_the_source_client() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = NotificationStore::open(temp.path(), 500).unwrap();
    store
        .push(notification(1, NotificationUrgency::Normal))
        .unwrap();
    store.expire_actions(1).unwrap();

    assert_eq!(store.active()[0].body, "<b>literal, never markup</b>");
    assert_eq!(
        store.active()[0].actions[0].state,
        NotificationActionState::Expired
    );
}

#[test]
fn typed_provider_owns_the_standard_name_without_advertising_markup() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store);

    assert_eq!(provider.bus_name(), DBUS_NOTIFICATIONS_NAME);
    assert!(provider.capabilities().contains(&"actions"));
    assert!(!provider.capabilities().contains(&"body-markup"));

    let delivered = provider
        .notify(NotifyRequest {
            origin: ":1.42".into(),
            notification: notification(7, NotificationUrgency::Normal),
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
    let mut provider = FreedesktopNotificationProvider::new(store);
    let mut second_application = notification(2, NotificationUrgency::Normal);
    second_application.application_id = "org.example.Other".into();

    provider
        .notify(NotifyRequest {
            origin: ":1.10".into(),
            notification: notification(1, NotificationUrgency::Normal),
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
    let mut store = NotificationStore::open(&state, 500).unwrap();
    let retained = temp.path().join("retained");
    std::fs::rename(&state, &retained).unwrap();
    std::fs::create_dir(&state).unwrap();

    store
        .push(notification(1, NotificationUrgency::Normal))
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

#[test]
fn every_notification_commit_fault_reconciles_to_one_complete_transaction() {
    for stage in NotificationCommitStage::ALL {
        let temp = tempfile::tempdir().unwrap();
        let mut store = NotificationStore::open(temp.path(), 500).unwrap();
        store
            .push(notification(1, NotificationUrgency::Normal))
            .unwrap();
        let mut store = store.with_commit_observer(Arc::new(FailOnceAt {
            target: stage,
            failed: Mutex::new(false),
        }));

        assert!(store
            .push(notification(2, NotificationUrgency::Critical))
            .is_err());
        drop(store);

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
    let mut provider = FreedesktopNotificationProvider::new(store);
    provider
        .notify(NotifyRequest {
            origin: ":1.55".into(),
            notification: notification(1, NotificationUrgency::Normal),
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
    let mut provider = FreedesktopNotificationProvider::new(store);
    provider
        .notify(NotifyRequest {
            origin: ":1.88".into(),
            notification: notification(1, NotificationUrgency::Normal),
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
    let mut provider = FreedesktopNotificationProvider::new(store);
    let mut oversized = notification(1, NotificationUrgency::Normal);
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
    let provider = FreedesktopNotificationProvider::new(store);
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 8);
    let mut subscriber = hub.subscribe().await;
    let _replay = subscriber.recv().await.unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 8).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let mut service = NotificationEventService::new(provider, authority);

    service
        .notify(NotifyRequest {
            origin: ":1.101".into(),
            notification: notification(1, NotificationUrgency::Normal),
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

#[test]
fn popup_timeout_expires_presentation_without_deleting_history_or_unread_state() {
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open(temp.path(), 500).unwrap();
    let mut provider = FreedesktopNotificationProvider::new(store);
    let start = std::time::Instant::now();
    let mut timed = notification(1, NotificationUrgency::Normal);
    timed.timeout_ms = Some(100);
    provider
        .notify_at(
            NotifyRequest {
                origin: ":1.111".into(),
                notification: timed,
            },
            start,
        )
        .unwrap();

    assert!(provider.popup_visible(1));
    assert!(provider
        .advance_popup_time(start + std::time::Duration::from_millis(99))
        .is_empty());
    assert_eq!(
        provider.advance_popup_time(start + std::time::Duration::from_millis(100)),
        [1]
    );
    assert!(!provider.popup_visible(1));
    assert_eq!(provider.store().active().len(), 1);
    assert_eq!(provider.store().unread_count(), 1);
}

#[test]
fn default_store_keeps_exactly_the_last_five_hundred_active_notifications() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = NotificationStore::open_default(temp.path()).unwrap();
    for id in 1..=501 {
        store
            .push(notification(id, NotificationUrgency::Low))
            .unwrap();
    }

    assert_eq!(DEFAULT_ACTIVE_NOTIFICATION_CAPACITY, 500);
    assert_eq!(store.active().len(), 500);
    assert_eq!(store.active().first().unwrap().id, 2);
    assert_eq!(store.archive().len(), 1);
    assert_eq!(store.archive()[0].id, 1);
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
    let mut provider = FreedesktopNotificationProvider::new(store);
    provider
        .notify(NotifyRequest {
            origin: ":1.121".into(),
            notification: notification(1, NotificationUrgency::Normal),
        })
        .unwrap();
    provider
        .notify(NotifyRequest {
            origin: ":1.122".into(),
            notification: notification(2, NotificationUrgency::Critical),
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
    let mut provider = FreedesktopNotificationProvider::new(store);
    provider
        .notify(NotifyRequest {
            origin: ":1.131".into(),
            notification: notification(1, NotificationUrgency::Normal),
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
    let mut provider = FreedesktopNotificationProvider::new(store);
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
