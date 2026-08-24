use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

use serde_json::{json, Value};
use sleepy_sdk::{
    canonicalize_accelerator, packaged_reserved_keybindings, validate_keybindings,
    validate_keybindings_with_reserved,
};

use crate::{Defaults, ImportMode, StateInspector, StateStore, StoreError, StorePaths};

const MAX_JSON_INPUT_BYTES: u64 = 1024 * 1024;
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0o400000;

enum CliCommand {
    SettingsShow,
    PresetsList,
    PresetsShow(String),
    PresetsCreate(String),
    PresetsUpdate {
        id: String,
        input: String,
    },
    PresetsDelete(String),
    PresetsValidate(String),
    PresetsImport {
        input: String,
        mode: ImportMode,
    },
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
    KeybindingsList(String),
    KeybindingsSet {
        id: String,
        action: String,
        accelerator: String,
    },
    KeybindingsUnset {
        id: String,
        action: String,
    },
    KeybindingsValidate(String),
    StateInspect,
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
        [command, action, preset_flag, id, semantic_action]
            if command == "keybindings" && action == "unset" && preset_flag == "--preset" =>
        {
            CliCommand::KeybindingsUnset {
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
        CliCommand::PresetsValidate(input) => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.validate_preset_candidate(candidate)
        }
        CliCommand::PresetsImport { input, mode } => {
            let candidate = read_json_input(&input)?;
            open_store(paths)?.import_preset(candidate, mode)
        }
        CliCommand::KeybindingsValidate(input) => {
            let candidate = read_json_input(&input)?;
            validate_keybinding_map(candidate)
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
                | CliCommand::PresetsValidate(_)
                | CliCommand::PresetsImport { .. }
                | CliCommand::KeybindingsValidate(_)
                | CliCommand::StateInspect => unreachable!("handled before store open"),
            }
        }
    }
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
    let mut preset = store.preset_json(id)?;
    let canonical = canonicalize_accelerator(accelerator)
        .map_err(|error| StoreError::invalid(error.to_string()))?;
    preset
        .get_mut("keybindings")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| StoreError::invalid("preset keybindings must be an object"))?
        .insert(action.to_owned(), Value::String(canonical));
    store.update_user_preset(id, preset)
}

fn keybindings_unset(store: &StateStore, id: &str, action: &str) -> Result<Value, StoreError> {
    let mut preset = store.preset_json(id)?;
    preset
        .get_mut("keybindings")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| StoreError::invalid("preset keybindings must be an object"))?
        .remove(action);
    store.update_user_preset(id, preset)
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
        reject_input_symlinks(&path)?;
        let metadata = fs::metadata(&path).map_err(StoreError::io)?;
        if !metadata.is_file() {
            return Err(StoreError::invalid("JSON input must be a regular file"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(target_os = "linux")]
        options.custom_flags(O_NOFOLLOW);
        let mut file = options.open(&path).map_err(StoreError::io)?;
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

fn reject_input_symlinks(path: &Path) -> Result<(), StoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(StoreError::io)?.join(path)
    };
    for ancestor in absolute.ancestors() {
        if fs::symlink_metadata(ancestor)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(StoreError::unsafe_path(ancestor.display()));
        }
    }
    Ok(())
}

fn default_state() -> Result<Defaults, StoreError> {
    Defaults::from_json(
        json!({
            "schemaVersion": 1,
            "activePresetId": "builtin.sleepy",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        }),
        vec![json!({
            "schemaVersion": 1,
            "id": "builtin.sleepy",
            "name": "Sleepy",
            "origin": "builtin",
            "basePresetId": null,
            "layouts": {},
            "drawers": { "leftQuickSettings": {} },
            "keybindings": {},
            "pluginRequirements": []
        })],
    )
}
