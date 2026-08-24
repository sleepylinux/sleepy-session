use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::Component,
};

use serde_json::{json, Value};
use sleepy_sdk::{
    canonicalize_accelerator, packaged_reserved_keybindings, validate_keybindings,
    validate_keybindings_with_reserved, PresetDocument,
};

use crate::{
    bindings::{
        activate_and_apply, apply_active_bindings, compile_bindings,
        import_replace_active_and_apply, mutate_keybinding_and_apply, reconcile_bindings,
        repair_state, update_active_bindings_and_apply, BindingPaths, NiriReloader, NiriValidator,
        RepairBundle,
    },
    Defaults, ImportMode, StateInspector, StateStore, StoreError, StorePaths,
};

const MAX_JSON_INPUT_BYTES: u64 = 1024 * 1024;

enum CliCommand {
    SettingsShow,
    PresetsList,
    PresetsShow(String),
    PresetsCreate(String),
    PresetsUpdate {
        id: String,
        input: String,
    },
    PresetsUpdateApply {
        id: String,
        input: String,
    },
    PresetsDelete(String),
    PresetsValidate(String),
    PresetsImport {
        input: String,
        mode: ImportMode,
    },
    PresetsImportApply(String),
    PresetsExport(String),
    PresetsDuplicate {
        source: String,
        name: String,
    },
    PresetsRename {
        id: String,
        name: String,
    },
    PresetsActivate(String),
    PresetsActivateApply(String),
    KeybindingsList(String),
    KeybindingsSet {
        id: String,
        action: String,
        accelerator: String,
    },
    KeybindingsSetApply {
        id: String,
        action: String,
        accelerator: String,
    },
    KeybindingsUnset {
        id: String,
        action: String,
    },
    KeybindingsUnsetApply {
        id: String,
        action: String,
    },
    KeybindingsValidate(String),
    StateInspect,
    StateRepair(String),
    BindingsRender,
    BindingsInitialize,
    BindingsReconcile,
}

pub fn run(arguments: Vec<String>) -> Result<Value, StoreError> {
    execute(parse(arguments)?)
}

fn parse(arguments: Vec<String>) -> Result<CliCommand, StoreError> {
    let command = match arguments.as_slice() {
        [command, action] if command == "settings" && action == "show" => CliCommand::SettingsShow,
        [command, action] if command == "presets" && action == "list" => CliCommand::PresetsList,
        [command, action, id] if command == "presets" && action == "show" => {
            CliCommand::PresetsShow(id.clone())
        }
        [command, action, input_flag, input]
            if command == "presets" && action == "create" && input_flag == "--input" =>
        {
            CliCommand::PresetsCreate(input.clone())
        }
        [command, action, id, input_flag, input]
            if command == "presets" && action == "update" && input_flag == "--input" =>
        {
            CliCommand::PresetsUpdate {
                id: id.clone(),
                input: input.clone(),
            }
        }
        [command, action, id, input_flag, input, apply]
            if command == "presets"
                && action == "update"
                && input_flag == "--input"
                && apply == "--apply" =>
        {
            CliCommand::PresetsUpdateApply {
                id: id.clone(),
                input: input.clone(),
            }
        }
        [command, action, id] if command == "presets" && action == "delete" => {
            CliCommand::PresetsDelete(id.clone())
        }
        [command, action, input_flag, input]
            if command == "presets" && action == "validate" && input_flag == "--input" =>
        {
            CliCommand::PresetsValidate(input.clone())
        }
        [command, action, input_flag, input]
            if command == "presets" && action == "import" && input_flag == "--input" =>
        {
            CliCommand::PresetsImport {
                input: input.clone(),
                mode: ImportMode::Reject,
            }
        }
        [command, action, input_flag, input, mode_flag, mode]
            if command == "presets"
                && action == "import"
                && input_flag == "--input"
                && mode_flag == "--mode" =>
        {
            CliCommand::PresetsImport {
                input: input.clone(),
                mode: parse_import_mode(mode)?,
            }
        }
        [command, action, input_flag, input, mode_flag, mode, apply]
            if command == "presets"
                && action == "import"
                && input_flag == "--input"
                && mode_flag == "--mode"
                && mode == "replace"
                && apply == "--apply" =>
        {
            CliCommand::PresetsImportApply(input.clone())
        }
        [command, action, id] if command == "presets" && action == "export" => {
            CliCommand::PresetsExport(id.clone())
        }
        [command, action, source, name] if command == "presets" && action == "duplicate" => {
            CliCommand::PresetsDuplicate {
                source: source.clone(),
                name: name.clone(),
            }
        }
        [command, action, id, name] if command == "presets" && action == "rename" => {
            CliCommand::PresetsRename {
                id: id.clone(),
                name: name.clone(),
            }
        }
        [command, action, id] if command == "presets" && action == "activate" => {
            CliCommand::PresetsActivate(id.clone())
        }
        [command, action, id, apply]
            if command == "presets" && action == "activate" && apply == "--apply" =>
        {
            CliCommand::PresetsActivateApply(id.clone())
        }
        [command, action, preset_flag, id]
            if command == "keybindings" && action == "list" && preset_flag == "--preset" =>
        {
            CliCommand::KeybindingsList(id.clone())
        }
        [command, action, preset_flag, id, semantic_action, accelerator]
            if command == "keybindings" && action == "set" && preset_flag == "--preset" =>
        {
            CliCommand::KeybindingsSet {
                id: id.clone(),
                action: semantic_action.clone(),
                accelerator: accelerator.clone(),
            }
        }
        [command, action, preset_flag, id, semantic_action, accelerator, apply]
            if command == "keybindings"
                && action == "set"
                && preset_flag == "--preset"
                && apply == "--apply" =>
        {
            CliCommand::KeybindingsSetApply {
                id: id.clone(),
                action: semantic_action.clone(),
                accelerator: accelerator.clone(),
            }
        }
        [command, action, preset_flag, id, semantic_action]
            if command == "keybindings" && action == "unset" && preset_flag == "--preset" =>
        {
            CliCommand::KeybindingsUnset {
                id: id.clone(),
                action: semantic_action.clone(),
            }
        }
        [command, action, preset_flag, id, semantic_action, apply]
            if command == "keybindings"
                && action == "unset"
                && preset_flag == "--preset"
                && apply == "--apply" =>
        {
            CliCommand::KeybindingsUnsetApply {
                id: id.clone(),
                action: semantic_action.clone(),
            }
        }
        [command, action, input_flag, input]
            if command == "keybindings" && action == "validate" && input_flag == "--input" =>
        {
            CliCommand::KeybindingsValidate(input.clone())
        }
        [command, action] if command == "state" && action == "inspect" => CliCommand::StateInspect,
        [command, action, bundle_flag, input, apply]
            if command == "state"
                && action == "repair"
                && bundle_flag == "--bundle"
                && apply == "--apply" =>
        {
            CliCommand::StateRepair(input.clone())
        }
        [command, action] if command == "bindings" && action == "render" => {
            CliCommand::BindingsRender
        }
        [command, action] if command == "bindings" && action == "initialize" => {
            CliCommand::BindingsInitialize
        }
        [command, action] if command == "bindings" && action == "reconcile" => {
            CliCommand::BindingsReconcile
        }
        _ => return Err(StoreError::invalid_command()),
    };
    Ok(command)
}

fn parse_import_mode(mode: &str) -> Result<ImportMode, StoreError> {
    match mode {
        "reject" => Ok(ImportMode::Reject),
        "copy" => Ok(ImportMode::Copy),
        "replace" => Ok(ImportMode::Replace),
        _ => Err(StoreError::invalid_command()),
    }
}

fn execute(command: CliCommand) -> Result<Value, StoreError> {
    let paths = StorePaths::from_environment();
    match command {
        CliCommand::StateInspect => {
            serde_json::to_value(StateInspector::inspect(paths)).map_err(StoreError::io)
        }
        CliCommand::PresetsCreate(input) => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.create_user_preset(candidate)
        }
        CliCommand::PresetsUpdate { id, input } => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.update_user_preset(&id, candidate)
        }
        CliCommand::PresetsUpdateApply { id, input } => {
            let candidate = read_json_input(&input)?;
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(update_active_bindings_and_apply(
                &id,
                candidate,
                &binding_paths,
                &validator,
                &reloader,
            ))
        }
        CliCommand::PresetsValidate(input) => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.validate_preset_candidate(candidate)
        }
        CliCommand::PresetsImport { input, mode } => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.import_preset(candidate, mode)
        }
        CliCommand::PresetsImportApply(input) => {
            let candidate = read_json_input(&input)?;
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(import_replace_active_and_apply(
                candidate,
                &binding_paths,
                &validator,
                &reloader,
            ))
        }
        CliCommand::KeybindingsValidate(input) => {
            let candidate = read_json_input(&input)?;
            validate_keybinding_map(candidate)
        }
        CliCommand::PresetsActivateApply(id) => {
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(activate_and_apply(
                &id,
                &binding_paths,
                &validator,
                &reloader,
            ))
        }
        CliCommand::KeybindingsSetApply {
            id,
            action,
            accelerator,
        } => {
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(mutate_keybinding_and_apply(
                &id,
                &action,
                Some(&accelerator),
                &binding_paths,
                &validator,
                &reloader,
            ))
        }
        CliCommand::KeybindingsUnsetApply { id, action } => {
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(mutate_keybinding_and_apply(
                &id,
                &action,
                None,
                &binding_paths,
                &validator,
                &reloader,
            ))
        }
        CliCommand::StateRepair(input) => {
            let value = read_json_input(&input)?;
            let bundle: RepairBundle = serde_json::from_value(value)
                .map_err(|error| StoreError::invalid(error.to_string()))?;
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(repair_state(bundle, &binding_paths, &validator, &reloader))
        }
        CliCommand::BindingsInitialize => {
            let binding_paths = BindingPaths::from_environment();
            let (validator, reloader) = niri_services();
            binding_value(apply_active_bindings(&binding_paths, &validator, &reloader))
        }
        CliCommand::BindingsReconcile => {
            let binding_paths = BindingPaths::from_environment();
            let (_, reloader) = niri_services();
            let report =
                reconcile_bindings(&binding_paths, &reloader).map_err(binding_store_error)?;
            serde_json::to_value(report).map_err(StoreError::io)
        }
        CliCommand::BindingsRender => {
            let store = open_store(paths)?;
            let active: PresetDocument = serde_json::from_value(store.active_preset_json()?)
                .map_err(|error| StoreError::invalid(error.to_string()))?;
            let kdl = compile_bindings(&active).map_err(binding_store_error)?;
            Ok(json!({"activePresetId": active.id, "kdl": kdl}))
        }
        command => {
            let store = open_store(paths)?;
            match command {
                CliCommand::SettingsShow => store.settings_json(),
                CliCommand::PresetsList => store.presets_json(),
                CliCommand::PresetsShow(id) => store.preset_json(&id),
                CliCommand::PresetsDelete(id) => store.delete_user_preset(&id),
                CliCommand::PresetsExport(id) => store.export_preset(&id),
                CliCommand::PresetsDuplicate { source, name } => {
                    store.duplicate_preset(&source, &name)
                }
                CliCommand::PresetsRename { id, name } => store.rename_preset(&id, &name),
                CliCommand::PresetsActivate(id) => store.activate_preset(&id),
                CliCommand::KeybindingsList(id) => keybindings_list(&store, &id),
                CliCommand::KeybindingsSet {
                    id,
                    action,
                    accelerator,
                } => keybindings_set(&store, &id, &action, &accelerator),
                CliCommand::KeybindingsUnset { id, action } => {
                    keybindings_unset(&store, &id, &action)
                }
                CliCommand::PresetsCreate(_)
                | CliCommand::PresetsUpdate { .. }
                | CliCommand::PresetsUpdateApply { .. }
                | CliCommand::PresetsValidate(_)
                | CliCommand::PresetsImport { .. }
                | CliCommand::PresetsImportApply(_)
                | CliCommand::PresetsActivateApply(_)
                | CliCommand::KeybindingsValidate(_)
                | CliCommand::KeybindingsSetApply { .. }
                | CliCommand::KeybindingsUnsetApply { .. }
                | CliCommand::StateInspect
                | CliCommand::StateRepair(_)
                | CliCommand::BindingsRender
                | CliCommand::BindingsInitialize
                | CliCommand::BindingsReconcile => unreachable!("handled before store open"),
            }
        }
    }
}

fn niri_services() -> (NiriValidator, NiriReloader) {
    let validator = std::env::var_os("SLEEPY_NIRI_VALIDATOR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("niri"));
    let niri = std::env::var_os("SLEEPY_NIRI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("niri"));
    (NiriValidator::new(validator), NiriReloader::new(niri))
}

fn binding_value<T: serde::Serialize>(
    result: Result<T, crate::bindings::BindingError>,
) -> Result<Value, StoreError> {
    serde_json::to_value(result.map_err(binding_store_error)?).map_err(StoreError::io)
}

fn binding_store_error(error: crate::bindings::BindingError) -> StoreError {
    StoreError::binding_with_details(
        error.code(),
        error.message().to_owned(),
        error.details().cloned(),
    )
}

fn open_store(paths: StorePaths) -> Result<StateStore, StoreError> {
    StateStore::open(paths, default_state()?)
}

fn keybindings_list(store: &StateStore, id: &str) -> Result<Value, StoreError> {
    let preset = store.preset_json(id)?;
    Ok(json!({
        "presetId": id,
        "keybindings": preset.get("keybindings").cloned().unwrap_or_else(|| json!({})),
    }))
}

fn keybindings_set(
    store: &StateStore,
    id: &str,
    action: &str,
    accelerator: &str,
) -> Result<Value, StoreError> {
    store.mutate_user_keybinding(id, action, Some(accelerator))
}

fn keybindings_unset(store: &StateStore, id: &str, action: &str) -> Result<Value, StoreError> {
    store.mutate_user_keybinding(id, action, None)
}

fn validate_keybinding_map(candidate: Value) -> Result<Value, StoreError> {
    let bindings: BTreeMap<String, String> = serde_json::from_value(candidate)
        .map_err(|error| StoreError::invalid(error.to_string()))?;
    let canonical = bindings
        .into_iter()
        .map(|(action, accelerator)| {
            canonicalize_accelerator(&accelerator)
                .map(|accelerator| (action, accelerator))
                .map_err(|error| StoreError::invalid(error.to_string()))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validate_keybindings_with_reserved(&canonical, &packaged_reserved_keybindings())
        .map_err(StoreError::keybinding_conflict)?;
    validate_keybindings(&canonical).map_err(|error| StoreError::invalid(error.to_string()))?;
    Ok(json!({ "keybindings": canonical }))
}

fn read_json_input(input: &str) -> Result<Value, StoreError> {
    let bytes = if input == "-" {
        read_bounded(&mut io::stdin().lock())?
    } else {
        let path = PathBuf::from(input);
        let mut file = open_regular_file(&path)?;
        read_bounded(&mut file)?
    };
    let input = String::from_utf8(bytes)
        .map_err(|error| StoreError::invalid(format!("JSON input is not UTF-8: {error}")))?;
    serde_json::from_str(&input).map_err(|error| StoreError::invalid(error.to_string()))
}

fn read_bounded(reader: &mut impl Read) -> Result<Vec<u8>, StoreError> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_JSON_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(StoreError::io)?;
    if bytes.len() as u64 > MAX_JSON_INPUT_BYTES {
        return Err(StoreError::invalid(format!(
            "JSON input exceeds {MAX_JSON_INPUT_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn open_regular_file(path: &Path) -> Result<File, StoreError> {
    open_regular_file_with_observer(path, |_| {})
}

#[cfg(target_os = "linux")]
fn open_regular_file_with_observer(
    path: &Path,
    mut opened_directory: impl FnMut(&Path),
) -> Result<File, StoreError> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => components.push(component.to_owned()),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(StoreError::unsafe_path(path.display()));
            }
        }
    }
    let final_component = components
        .pop()
        .ok_or_else(|| StoreError::invalid("JSON input path must name a file"))?;
    let anchor = if path.is_absolute() { "/" } else { "." };
    let anchor_name = CString::new(anchor).expect("static anchor contains no NUL");
    let mut directory = openat_owned(
        libc::AT_FDCWD,
        &anchor_name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
    )
    .map_err(StoreError::io)?;
    let mut opened_path = if path.is_absolute() {
        PathBuf::from("/")
    } else {
        PathBuf::new()
    };

    for component in components {
        opened_path.push(&component);
        let component_name = c_path_component(&component)?;
        directory = openat_owned(
            directory.as_raw_fd(),
            &component_name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
        .map_err(|error| map_component_open_error(&opened_path, error))?;
        opened_directory(&opened_path);
    }

    let final_name = c_path_component(&final_component)?;
    let final_path = opened_path.join(&final_component);
    let descriptor = openat_owned(
        directory.as_raw_fd(),
        &final_name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
    )
    .map_err(|error| map_component_open_error(&final_path, error))?;
    let file = File::from(descriptor);
    if !file.metadata().map_err(StoreError::io)?.is_file() {
        return Err(StoreError::invalid("JSON input must be a regular file"));
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn openat_owned(directory: i32, component: &CString, flags: i32) -> Result<OwnedFd, io::Error> {
    // SAFETY: `component` is NUL-terminated, `directory` is AT_FDCWD or a live
    // directory descriptor, and ownership of a successful descriptor is
    // transferred exactly once into `OwnedFd`.
    let descriptor = unsafe { libc::openat(directory, component.as_ptr(), flags) };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: `openat` returned a new owned descriptor above.
        Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
    }
}

#[cfg(target_os = "linux")]
fn c_path_component(component: &std::ffi::OsStr) -> Result<CString, StoreError> {
    CString::new(component.as_bytes())
        .map_err(|_| StoreError::invalid("JSON input path contains a NUL byte"))
}

#[cfg(target_os = "linux")]
fn map_component_open_error(path: &Path, error: io::Error) -> StoreError {
    if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
        StoreError::unsafe_path(path.display())
    } else {
        StoreError::io(error)
    }
}

#[cfg(not(target_os = "linux"))]
fn open_regular_file(_path: &Path) -> Result<File, StoreError> {
    Err(StoreError::unsupported(
        "secure descriptor-relative JSON file input is unsupported on this platform",
    ))
}

fn default_state() -> Result<Defaults, StoreError> {
    Ok(Defaults::packaged())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::{fs, io::Read, os::unix::fs::symlink};

    use tempfile::TempDir;

    use super::open_regular_file_with_observer;

    #[test]
    fn secure_input_resolution_keeps_open_directory_after_path_swap() {
        let root = TempDir::new().unwrap();
        let safe_directory = root.path().join("safe");
        let moved_directory = root.path().join("safe-opened");
        let attacker_directory = root.path().join("attacker");
        fs::create_dir(&safe_directory).unwrap();
        fs::create_dir(&attacker_directory).unwrap();
        fs::write(safe_directory.join("preset.json"), b"safe-bytes").unwrap();
        fs::write(attacker_directory.join("preset.json"), b"attacker-bytes").unwrap();
        let input_path = safe_directory.join("preset.json");
        let mut swapped = false;

        let mut file = open_regular_file_with_observer(&input_path, |opened_path| {
            if !swapped && opened_path == safe_directory {
                fs::rename(&safe_directory, &moved_directory).unwrap();
                symlink(&attacker_directory, &safe_directory).unwrap();
                swapped = true;
            }
        })
        .unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();

        assert!(swapped);
        assert_eq!(bytes, b"safe-bytes");
    }
}
