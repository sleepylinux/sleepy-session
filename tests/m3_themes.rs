// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fs::{self, OpenOptions},
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::{Duration, Instant},
};

use fs2::FileExt;
use sleepy_sdk::{SessionEvent, ThemeAppearance, ThemeDocument, ThemeEffects, ThemeOrigin};
use sleepy_session::{
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
    system::RunControl,
    theme::{
        derive_wallpaper_palette, ColorSchemePortal, DesktopThemeSink, EffectsPolicy,
        PortalColorScheme, ThemeApplyStage, ThemeErrorKind, ThemeManager, ThemeTransactionObserver,
    },
};
use tempfile::TempDir;

fn manager(temp: &TempDir) -> ThemeManager {
    ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap()
}

fn authority(temp: &TempDir) -> GenerationAuthority {
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        0,
        hub,
    )
}

fn user_json(id: &str) -> String {
    format!(
        r##"{{"schemaVersion":1,"id":"{id}","name":"Custom","origin":"user","appearance":"dark","effects":"full","reducedMotion":false,"opaqueFallback":false,"colors":{{"background":"#101010","surface":"#181818","textPrimary":"#FFFFFF","textSecondary":"#E0E0E0","accent":"#80BFFF","control":"#FFFFFF"}}}}"##
    )
}

#[test]
fn builtins_are_immutable_and_editing_creates_uuid_user_copy() {
    let temp = TempDir::new().unwrap();
    let store = manager(&temp);
    let builtin = store.theme("builtin.sleepy-dark").unwrap();
    assert_eq!(builtin.origin, ThemeOrigin::Builtin);
    assert!(store.delete("builtin.sleepy-dark").is_err());

    let copy = store
        .copy_for_edit("builtin.sleepy-dark", "My Sleepy")
        .unwrap();
    assert_eq!(copy.origin, ThemeOrigin::User);
    assert!(uuid::Uuid::parse_str(&copy.id).is_ok());
    assert_eq!(copy.name, "My Sleepy");
    assert_eq!(store.theme("builtin.sleepy-dark").unwrap(), builtin);
}

#[test]
fn import_reissues_identity_and_malformed_input_preserves_files() {
    let temp = TempDir::new().unwrap();
    let store = manager(&temp);
    let settings = temp.path().join("config/sleepy/settings.json");
    fs::create_dir_all(settings.parent().unwrap()).unwrap();
    fs::write(&settings, b"m2-bytes\n").unwrap();
    let before = fs::read(&settings).unwrap();

    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    assert_ne!(imported.id, "9ed1119d-5f95-4aa7-b31b-ecbb44a052ce");
    assert!(store.import("{\"schemaVersion\":99}").is_err());
    assert_eq!(fs::read(&settings).unwrap(), before);
    assert_eq!(store.theme(&imported.id).unwrap(), imported);
}

#[test]
fn low_contrast_import_is_rejected_without_replacing_last_valid_theme() {
    let temp = TempDir::new().unwrap();
    let store = manager(&temp);
    let valid = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let before = store.durable_state_bytes().unwrap();
    let low_contrast = user_json("67fb90b3-123b-4e3e-9489-3a8ddb66d4f2")
        .replace("#FFFFFF", "#202020")
        .replace("#E0E0E0", "#202020")
        .replace("#80BFFF", "#202020");
    assert!(store.import(&low_contrast).is_err());
    assert_eq!(store.durable_state_bytes().unwrap(), before);
    assert_eq!(store.theme(&valid.id).unwrap(), valid);
}

#[test]
fn ui_color_that_only_fails_on_surface_is_rejected_before_sdk_repin() {
    let temp = TempDir::new().unwrap();
    let store = manager(&temp);
    let document = serde_json::json!({
        "schemaVersion": 1,
        "id": "9ed1119d-5f95-4aa7-b31b-ecbb44a052ce",
        "name": "Split",
        "origin": "user",
        "appearance": "dark",
        "effects": "none",
        "reducedMotion": true,
        "opaqueFallback": true,
        "colors": {
            "background": "#000000",
            "surface": "#FFFFFF",
            "textPrimary": "#767676",
            "textSecondary": "#767676",
            "accent": "#FFFFFF",
            "control": "#767676"
        }
    });
    assert!(store.import(&document.to_string()).is_err());
    assert!(store.durable_state_bytes().unwrap().is_empty());
}

#[test]
fn preview_is_memory_only() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let before = store.durable_state_bytes().unwrap();
    store.preview(&imported.id).unwrap();
    assert_eq!(store.previewed().unwrap().id, imported.id);
    assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
    assert_eq!(store.durable_state_bytes().unwrap(), before);
    store.clear_preview();
    assert!(store.previewed().is_none());
}

#[derive(Default)]
struct RecordingSink {
    acknowledgements: Mutex<Vec<String>>,
    reject: Mutex<bool>,
}

impl DesktopThemeSink for RecordingSink {
    fn acknowledge<'a>(
        &'a self,
        theme: &'a ThemeDocument,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.acknowledgements.lock().unwrap().push(theme.id.clone());
            if *self.reject.lock().unwrap() {
                Err("desktop rejected candidate".into())
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn apply_is_journaled_acknowledged_and_generation_confirmed() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let sink = RecordingSink::default();
    let applied = store
        .apply(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &sink,
            &authority(&temp),
        )
        .await
        .unwrap();
    assert!(applied.generation > 0);
    assert_eq!(applied.theme.id, imported.id);
    assert_eq!(store.current().unwrap().id, imported.id);
    assert!(!store.has_journal().unwrap());
}

#[tokio::test]
async fn stale_generation_is_rejected_before_journal_or_desktop_acknowledgement() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let sink = RecordingSink::default();
    assert!(store
        .apply(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            99,
            &sink,
            &authority(&temp),
        )
        .await
        .is_err());
    assert!(sink.acknowledgements.lock().unwrap().is_empty());
    assert!(!store.has_journal().unwrap());
}

#[tokio::test]
async fn apply_preserves_unrelated_m2_documents_byte_for_byte() {
    let temp = TempDir::new().unwrap();
    let settings = temp.path().join("config/sleepy/settings.json");
    let presets = temp.path().join("state/sleepy/presets.json");
    fs::create_dir_all(settings.parent().unwrap()).unwrap();
    fs::create_dir_all(presets.parent().unwrap()).unwrap();
    fs::write(&settings, b"settings-exact\n").unwrap();
    fs::write(&presets, b"presets-exact\n").unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    store
        .apply(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &RecordingSink::default(),
            &authority(&temp),
        )
        .await
        .unwrap();
    assert_eq!(fs::read(settings).unwrap(), b"settings-exact\n");
    assert_eq!(fs::read(presets).unwrap(), b"presets-exact\n");
}

struct FailAt(ThemeApplyStage);
impl ThemeTransactionObserver for FailAt {
    fn observe(&mut self, stage: ThemeApplyStage) -> Result<(), String> {
        if stage == self.0 {
            Err("injected failure".into())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn every_apply_boundary_rolls_back_to_confirmed_theme() {
    for stage in [
        ThemeApplyStage::JournalWritten,
        ThemeApplyStage::DesktopAcknowledged,
        ThemeApplyStage::GenerationCommitted,
        ThemeApplyStage::CurrentWritten,
    ] {
        let temp = TempDir::new().unwrap();
        let mut store = manager(&temp);
        let imported = store
            .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
            .unwrap();
        let result = store
            .apply_observed(
                &imported.id,
                "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
                0,
                &RecordingSink::default(),
                &authority(&temp),
                &mut FailAt(stage),
            )
            .await;
        assert!(result.is_err(), "stage {stage:?}");
        assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
        assert!(!store.has_journal().unwrap());
    }
}

#[tokio::test]
async fn cleanup_remove_and_directory_sync_failures_enter_confirmed_rollback() {
    for stage in [
        ThemeApplyStage::JournalRemoved,
        ThemeApplyStage::JournalDirectorySynced,
    ] {
        let temp = TempDir::new().unwrap();
        let mut store = manager(&temp);
        let imported = store
            .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
            .unwrap();
        assert!(store
            .apply_observed(
                &imported.id,
                "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
                0,
                &RecordingSink::default(),
                &authority(&temp),
                &mut FailAt(stage),
            )
            .await
            .is_err());
        assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
        assert!(!store.has_journal().unwrap());
    }
}

#[tokio::test]
async fn crash_after_cleanup_recovery_is_reconciled_on_restart() {
    struct CrashAfterRecovery;
    impl ThemeTransactionObserver for CrashAfterRecovery {
        fn observe(&mut self, stage: ThemeApplyStage) -> Result<(), String> {
            if matches!(
                stage,
                ThemeApplyStage::JournalRemoved | ThemeApplyStage::CleanupRecoveryWritten
            ) {
                Err("injected cleanup crash".into())
            } else {
                Ok(())
            }
        }
    }
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    assert!(store
        .apply_observed(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &RecordingSink::default(),
            &authority(&temp),
            &mut CrashAfterRecovery,
        )
        .await
        .is_err());
    assert_eq!(store.current().unwrap().id, imported.id);
    assert!(store.has_journal().unwrap());

    drop(store);
    let mut restarted = manager(&temp);
    restarted
        .reconcile(&RecordingSink::default(), &authority(&temp))
        .await
        .unwrap();
    assert_eq!(restarted.current().unwrap().id, "builtin.sleepy-dark");
    assert!(!restarted.has_journal().unwrap());
}

#[tokio::test]
async fn rollback_fault_keeps_journal_for_startup_reconciliation() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    struct FailApplyAndRollback;
    impl ThemeTransactionObserver for FailApplyAndRollback {
        fn observe(&mut self, stage: ThemeApplyStage) -> Result<(), String> {
            if matches!(
                stage,
                ThemeApplyStage::DesktopAcknowledged | ThemeApplyStage::RollbackWritten
            ) {
                Err("crash boundary".into())
            } else {
                Ok(())
            }
        }
    }
    assert!(store
        .apply_observed(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &RecordingSink::default(),
            &authority(&temp),
            &mut FailApplyAndRollback,
        )
        .await
        .is_err());
    assert!(store.has_journal().unwrap());
    store
        .reconcile(&RecordingSink::default(), &authority(&temp))
        .await
        .unwrap();
    assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
    assert!(!store.has_journal().unwrap());
}

#[tokio::test]
async fn desktop_rejection_fails_closed_without_changing_current() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let sink = RecordingSink::default();
    *sink.reject.lock().unwrap() = true;
    assert!(store
        .apply(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &sink,
            &authority(&temp),
        )
        .await
        .is_err());
    assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
    assert!(
        store.has_journal().unwrap(),
        "failed rollback remains recoverable"
    );
}

struct HangingSink;
impl DesktopThemeSink for HangingSink {
    fn acknowledge<'a>(
        &'a self,
        _theme: &'a ThemeDocument,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn desktop_acknowledgement_and_rollback_are_bounded_by_daemon_timeout() {
    let temp = TempDir::new().unwrap();
    let mut store = ThemeManager::open_with_acknowledgement_timeout(
        temp.path().join("config"),
        temp.path().join("state"),
        Duration::from_millis(10),
    )
    .unwrap();
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let started = std::time::Instant::now();
    assert!(store
        .apply(
            &imported.id,
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &HangingSink,
            &authority(&temp),
        )
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
    assert!(store.has_journal().unwrap());
}

struct GatedSink {
    started: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

impl DesktopThemeSink for GatedSink {
    fn acknowledge<'a>(
        &'a self,
        _theme: &'a ThemeDocument,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_interprocess_lock_serializes_delete_and_import_against_apply() {
    let temp = TempDir::new().unwrap();
    let mut applying = manager(&temp);
    let deleting = manager(&temp);
    let importing = manager(&temp);
    let imported = applying
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let sink = GatedSink {
        started: started.clone(),
        release: release.clone(),
    };
    let auth = authority(&temp);
    let apply_id = imported.id.clone();
    let apply = tokio::spawn(async move {
        applying
            .apply(
                &apply_id,
                "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
                0,
                &sink,
                &auth,
            )
            .await
    });
    started.notified().await;

    let delete_id = imported.id.clone();
    let mut delete = tokio::task::spawn_blocking(move || deleting.delete(&delete_id));
    let mut import = tokio::task::spawn_blocking(move || {
        importing.import(&user_json("67fb90b3-123b-4e3e-9489-3a8ddb66d4f2"))
    });
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut delete)
        .await
        .is_err());
    assert!(tokio::time::timeout(Duration::from_millis(30), &mut import)
        .await
        .is_err());
    release.notify_one();
    apply.await.unwrap().unwrap();
    assert!(delete.await.unwrap().is_err());
    let concurrently_imported = import.await.unwrap().unwrap();
    assert_eq!(manager(&temp).current().unwrap().id, imported.id);
    assert_eq!(
        manager(&temp).theme(&concurrently_imported.id).unwrap(),
        concurrently_imported
    );
}

#[tokio::test]
async fn externally_held_interprocess_lock_returns_typed_timeout_without_blocking_runtime() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let lock_path = temp.path().join("state/sleepy/themes/apply.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .unwrap();
    lock.lock_exclusive().unwrap();
    let control = RunControl::for_request(
        Instant::now() + Duration::from_millis(40),
        Arc::new(AtomicBool::new(false)),
    );
    let started = Instant::now();
    let error = store
        .apply_controlled(
            "builtin.sleepy-light",
            "d78951f8-c6f5-4f7d-8599-d72ed0b34803",
            0,
            &RecordingSink::default(),
            &authority(&temp),
            &control,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ThemeErrorKind::Timeout);
    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(!store.has_journal().unwrap());
}

#[tokio::test]
async fn startup_reconciliation_rolls_back_crashed_candidate() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    let imported = store
        .import(&user_json("9ed1119d-5f95-4aa7-b31b-ecbb44a052ce"))
        .unwrap();
    store.seed_crash_journal_for_test(&imported.id).unwrap();
    let sink = RecordingSink::default();
    store.reconcile(&sink, &authority(&temp)).await.unwrap();
    assert_eq!(store.current().unwrap().id, "builtin.sleepy-dark");
    assert!(!store.has_journal().unwrap());
}

#[tokio::test]
async fn reconciliation_outcome_generation_matches_published_rollback_event_exactly() {
    let temp = TempDir::new().unwrap();
    let mut store = manager(&temp);
    store
        .seed_crash_journal_for_test("builtin.sleepy-light")
        .unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let mut events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation-exact"), 16).unwrap(),
        0,
        hub,
    );
    let outcome = store
        .reconcile(&RecordingSink::default(), &authority)
        .await
        .unwrap();
    assert!(outcome.reconciled);
    events.recv().await.unwrap();
    let rollback = events.recv().await.unwrap();
    assert_eq!(outcome.generation, Some(rollback.generation));
    assert!(matches!(
        rollback.payload,
        SessionEvent::Theme(ref event)
            if event.theme_id == "builtin.sleepy-dark" && event.applied
    ));
}

#[test]
fn palette_is_deterministic_valid_and_rejects_malformed_pixels() {
    let pixels = [8, 12, 20, 8, 12, 20, 34, 100, 180, 240, 240, 240];
    let first = derive_wallpaper_palette(&pixels).unwrap();
    assert_eq!(first, derive_wallpaper_palette(&pixels).unwrap());
    let document = ThemeDocument {
        schema_version: 1,
        id: uuid::Uuid::new_v4().to_string(),
        name: "Palette".into(),
        origin: ThemeOrigin::User,
        appearance: ThemeAppearance::Dark,
        effects: ThemeEffects::Full,
        reduced_motion: false,
        opaque_fallback: false,
        colors: first,
    };
    sleepy_sdk::validate_theme_document(&serde_json::to_string(&document).unwrap()).unwrap();
    assert!(derive_wallpaper_palette(&[]).is_err());
    assert!(derive_wallpaper_palette(&[1, 2]).is_err());
}

struct Portal(PortalColorScheme);
impl ColorSchemePortal for Portal {
    fn color_scheme(&self) -> Result<PortalColorScheme, String> {
        Ok(self.0)
    }
}

#[test]
fn one_effects_policy_handles_portal_motion_and_opaque_fallback() {
    let theme = ThemeManager::builtin("builtin.sleepy-system").unwrap();
    let policy = EffectsPolicy::resolve(&theme, &Portal(PortalColorScheme::Dark)).unwrap();
    assert_eq!(policy.appearance, PortalColorScheme::Dark);
    assert!(policy.blur);
    assert!(policy.animations);

    let mut constrained = theme;
    constrained.effects = ThemeEffects::Reduced;
    constrained.reduced_motion = true;
    constrained.opaque_fallback = true;
    let policy = EffectsPolicy::resolve(&constrained, &Portal(PortalColorScheme::Light)).unwrap();
    assert!(!policy.blur);
    assert!(!policy.animations);
    assert!(policy.opaque);
}

#[test]
fn theme_catalog_contains_all_immutable_builtins_and_imported_user_themes() {
    let temp = TempDir::new().unwrap();
    let store = manager(&temp);
    let mut imported = ThemeManager::builtin("builtin.sleepy-dark").unwrap();
    imported.id = uuid::Uuid::new_v4().to_string();
    imported.name = "User dusk".into();
    imported.origin = ThemeOrigin::User;
    let imported = store
        .import(&serde_json::to_string(&imported).unwrap())
        .unwrap();
    let catalog = store.list().unwrap();
    assert_eq!(
        &catalog[..3]
            .iter()
            .map(|theme| theme.id.as_str())
            .collect::<Vec<_>>(),
        &[
            "builtin.sleepy-dark",
            "builtin.sleepy-light",
            "builtin.sleepy-system"
        ]
    );
    assert!(catalog
        .iter()
        .any(|theme| theme.id == imported.id && theme.name == "User dusk"));
}
