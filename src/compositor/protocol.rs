// SPDX-License-Identifier: GPL-3.0-only

use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use sleepy_sdk::{HyprlandSnapshot, Monitor, Window, Workspace};

use super::{bounds_error, inconsistent_error, parse_error, CompositorError};

const MAX_MONITORS: usize = 64;
const MAX_WORKSPACES: usize = 1_024;
const MAX_WINDOWS: usize = 16_384;
const MAX_GROUP_MEMBERS: usize = MAX_WINDOWS;
const MAX_NAME_BYTES: usize = 256;
const MAX_APPLICATION_ID_BYTES: usize = 512;
const MAX_TITLE_BYTES: usize = 4_096;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpstreamMonitor {
    id: i64,
    name: String,
    width: u64,
    height: u64,
    scale: f64,
    focused: bool,
    active_workspace: UpstreamWorkspaceRef,
    special_workspace: UpstreamWorkspaceRef,
}

#[derive(Deserialize)]
struct UpstreamWorkspace {
    id: i64,
    name: String,
    monitor: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpstreamClient {
    address: String,
    workspace: UpstreamWorkspaceRef,
    floating: bool,
    monitor: i64,
    #[serde(rename = "class")]
    application_id: String,
    title: String,
    pinned: bool,
    fullscreen: i64,
    grouped: Vec<String>,
    #[serde(rename = "focusHistoryID")]
    focus_history_id: i64,
}

#[derive(Deserialize)]
struct UpstreamWorkspaceRef {
    id: i64,
    name: String,
}

pub fn parse_full_snapshot(
    monitors_json: &[u8],
    workspaces_json: &[u8],
    clients_json: &[u8],
) -> Result<HyprlandSnapshot, CompositorError> {
    let upstream_monitors: Vec<UpstreamMonitor> = parse_json(monitors_json, "monitor")?;
    let upstream_workspaces: Vec<UpstreamWorkspace> = parse_json(workspaces_json, "workspace")?;
    let upstream_clients: Vec<UpstreamClient> = parse_json(clients_json, "client")?;
    enforce_maximum(upstream_monitors.len(), MAX_MONITORS, "monitors")?;
    enforce_maximum(upstream_workspaces.len(), MAX_WORKSPACES, "workspaces")?;
    enforce_maximum(upstream_clients.len(), MAX_WINDOWS, "windows")?;

    let mut monitor_ids = HashSet::new();
    let mut monitor_numeric_ids = HashSet::new();
    let mut monitor_numbers = HashMap::new();
    let mut monitors = Vec::with_capacity(upstream_monitors.len());
    let mut focused_monitor_workspace = None;
    let mut workspace_refs = Vec::with_capacity(upstream_monitors.len() * 2);
    for monitor in upstream_monitors {
        validate_numeric_id(monitor.id, "monitor id")?;
        validate_bounded_string(&monitor.name, MAX_NAME_BYTES, "monitor name", false)?;
        if !monitor_ids.insert(monitor.name.clone()) {
            return Err(parse_error(
                "Hyprland monitors contain duplicate stable IDs",
            ));
        }
        if !monitor_numeric_ids.insert(monitor.id) {
            return Err(parse_error(
                "Hyprland monitors contain duplicate numeric IDs",
            ));
        }
        monitor_numbers.insert(monitor.name.clone(), monitor.id);
        let width = u32::try_from(monitor.width)
            .map_err(|_| parse_error("Hyprland monitor width exceeds u32"))?;
        let height = u32::try_from(monitor.height)
            .map_err(|_| parse_error("Hyprland monitor height exceeds u32"))?;
        if width == 0
            || height == 0
            || !monitor.scale.is_finite()
            || monitor.scale <= 0.0
            || monitor.scale > 16.0
        {
            return Err(parse_error(
                "Hyprland monitor geometry and scale must be finite and positive",
            ));
        }
        validate_workspace_ref(&monitor.active_workspace, true)?;
        workspace_refs.push((
            monitor.name.clone(),
            monitor.active_workspace.id,
            monitor.active_workspace.name.clone(),
        ));
        if monitor.special_workspace.id != 0 {
            validate_workspace_ref(&monitor.special_workspace, true)?;
            workspace_refs.push((
                monitor.name.clone(),
                monitor.special_workspace.id,
                monitor.special_workspace.name.clone(),
            ));
        } else if !monitor.special_workspace.name.is_empty() {
            return Err(parse_error(
                "Hyprland inactive special workspace must have an empty name",
            ));
        }
        if monitor.focused {
            if focused_monitor_workspace.is_some() {
                return Err(parse_error("Hyprland reported multiple focused monitors"));
            }
            focused_monitor_workspace = Some((
                monitor.name.clone(),
                if monitor.special_workspace.id == 0 {
                    monitor.active_workspace.id
                } else {
                    monitor.special_workspace.id
                },
            ));
        }
        monitors.push(Monitor {
            id: monitor.name.clone(),
            name: monitor.name,
            width,
            height,
            scale: monitor.scale,
            focused: monitor.focused,
        });
    }

    let mut workspace_ids = HashSet::new();
    let mut workspaces = Vec::with_capacity(upstream_workspaces.len());
    let mut workspace_locations = HashMap::with_capacity(upstream_workspaces.len());
    for workspace in upstream_workspaces {
        validate_workspace_id(workspace.id)?;
        validate_bounded_string(&workspace.name, MAX_NAME_BYTES, "workspace name", false)?;
        validate_bounded_string(
            &workspace.monitor,
            MAX_NAME_BYTES,
            "workspace monitor",
            false,
        )?;
        if !monitor_ids.contains(&workspace.monitor) {
            return Err(inconsistent_error(
                "Hyprland workspace references an unknown monitor",
            ));
        }
        let id = workspace.id.to_string();
        if !workspace_ids.insert(id.clone()) {
            return Err(parse_error(
                "Hyprland workspaces contain duplicate stable IDs",
            ));
        }
        workspace_locations.insert(
            workspace.id,
            (workspace.name.clone(), workspace.monitor.clone()),
        );
        workspaces.push(Workspace {
            id,
            name: workspace.name,
            monitor_id: workspace.monitor,
            focused: focused_monitor_workspace
                .as_ref()
                .is_some_and(|(_, id)| *id == workspace.id),
        });
    }
    for (monitor_name, workspace_id, workspace_name) in workspace_refs {
        let Some((actual_name, actual_monitor)) = workspace_locations.get(&workspace_id) else {
            return Err(inconsistent_error(
                "Hyprland monitor references an unknown workspace",
            ));
        };
        if actual_name != &workspace_name || actual_monitor != &monitor_name {
            return Err(inconsistent_error(
                "Hyprland monitor and workspace queries disagree",
            ));
        }
    }

    let mut window_ids = HashSet::new();
    for client in &upstream_clients {
        validate_address(&client.address)?;
        if !window_ids.insert(client.address.clone()) {
            return Err(parse_error("Hyprland clients contain duplicate addresses"));
        }
    }
    let mut group_member_total = 0_usize;
    let mut group_sets = HashMap::with_capacity(upstream_clients.len());
    for client in &upstream_clients {
        enforce_maximum(client.grouped.len(), MAX_GROUP_MEMBERS, "group members")?;
        group_member_total = group_member_total
            .checked_add(client.grouped.len())
            .ok_or_else(|| bounds_error("Hyprland aggregate group topology overflowed"))?;
        enforce_maximum(
            group_member_total,
            MAX_GROUP_MEMBERS,
            "aggregate group members",
        )?;
        let mut members = HashSet::with_capacity(client.grouped.len());
        for grouped_address in &client.grouped {
            validate_address(grouped_address)?;
            if !members.insert(grouped_address.as_str()) {
                return Err(parse_error(
                    "Hyprland group contains duplicate member addresses",
                ));
            }
            if !window_ids.contains(grouped_address) {
                return Err(parse_error("Hyprland group references an unknown window"));
            }
        }
        if !client.grouped.is_empty() && !members.contains(client.address.as_str()) {
            return Err(parse_error(
                "Hyprland grouped window does not include itself",
            ));
        }
        group_sets.insert(client.address.as_str(), members);
    }
    let mut validated_group_members = HashSet::new();
    for client in &upstream_clients {
        if client.grouped.is_empty() || validated_group_members.contains(client.address.as_str()) {
            continue;
        }
        let members = group_sets
            .get(client.address.as_str())
            .expect("every client has a validated group set");
        for grouped_address in members {
            let peer_members = group_sets
                .get(grouped_address)
                .expect("group foreign keys were validated");
            if peer_members != members {
                return Err(parse_error(
                    "Hyprland group members do not report reciprocal topology",
                ));
            }
            validated_group_members.insert(*grouped_address);
        }
    }

    let mut windows = Vec::with_capacity(upstream_clients.len());
    let mut focused_window_seen = false;
    for client in upstream_clients {
        validate_workspace_ref(&client.workspace, true)?;
        validate_bounded_string(
            &client.application_id,
            MAX_APPLICATION_ID_BYTES,
            "window application id",
            false,
        )?;
        validate_bounded_string(&client.title, MAX_TITLE_BYTES, "window title", true)?;
        if !workspace_ids.contains(&client.workspace.id.to_string()) {
            return Err(inconsistent_error(
                "Hyprland window references an unknown workspace",
            ));
        }
        let (workspace_name, workspace_monitor) = workspace_locations
            .get(&client.workspace.id)
            .expect("workspace foreign key was validated");
        let workspace_monitor_number = monitor_numbers
            .get(workspace_monitor)
            .expect("workspace monitor foreign key was validated");
        if workspace_name != &client.workspace.name || client.monitor != *workspace_monitor_number {
            return Err(inconsistent_error(
                "Hyprland client and workspace queries disagree",
            ));
        }
        if client.fullscreen < 0 || client.fullscreen > 2 {
            return Err(parse_error("Hyprland fullscreen mode is outside 0..=2"));
        }
        let focused = client.focus_history_id == 0;
        if focused && std::mem::replace(&mut focused_window_seen, true) {
            return Err(parse_error("Hyprland reported multiple focused windows"));
        }
        if focused {
            let Some((monitor_name, workspace_id)) = &focused_monitor_workspace else {
                return Err(inconsistent_error(
                    "Hyprland focused window has no focused monitor",
                ));
            };
            let (_, workspace_monitor) = workspace_locations
                .get(&client.workspace.id)
                .expect("workspace foreign key was validated");
            let focused_monitor_number = monitor_numbers
                .get(monitor_name)
                .expect("focused monitor was validated");
            let pinned_on_focused_monitor = client.pinned
                && workspace_monitor == monitor_name
                && client.monitor == *focused_monitor_number;
            if client.workspace.id != *workspace_id && !pinned_on_focused_monitor {
                return Err(inconsistent_error(
                    "Hyprland focused window and focused workspace disagree",
                ));
            }
        }
        windows.push(Window {
            id: client.address,
            title: client.title,
            application_id: client.application_id,
            workspace_id: client.workspace.id.to_string(),
            focused,
            fullscreen: client.fullscreen != 0,
            floating: client.floating,
            pinned: client.pinned,
            grouped: !client.grouped.is_empty(),
        });
    }

    Ok(HyprlandSnapshot {
        action_capabilities: crate::desktop::hyprland_action_capabilities(),
        monitors,
        workspaces,
        windows,
    })
}

fn parse_json<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    description: &str,
) -> Result<T, CompositorError> {
    serde_json::from_slice(bytes)
        .map_err(|error| parse_error(format!("invalid Hyprland {description} JSON: {error}")))
}

fn enforce_maximum(
    actual: usize,
    maximum: usize,
    description: &str,
) -> Result<(), CompositorError> {
    if actual > maximum {
        Err(bounds_error(format!(
            "Hyprland {description} exceed maximum of {maximum}"
        )))
    } else {
        Ok(())
    }
}

fn validate_numeric_id(id: i64, description: &str) -> Result<(), CompositorError> {
    if id < 0 {
        Err(parse_error(format!(
            "Hyprland {description} must be nonnegative"
        )))
    } else {
        Ok(())
    }
}

fn validate_workspace_id(id: i64) -> Result<(), CompositorError> {
    if id == 0 {
        Err(parse_error("Hyprland workspace id must not be zero"))
    } else {
        Ok(())
    }
}

fn validate_workspace_ref(
    workspace: &UpstreamWorkspaceRef,
    required: bool,
) -> Result<(), CompositorError> {
    if required {
        validate_workspace_id(workspace.id)?;
    }
    validate_bounded_string(
        &workspace.name,
        MAX_NAME_BYTES,
        "workspace reference name",
        !required,
    )
}

fn validate_address(address: &str) -> Result<(), CompositorError> {
    let digits = address
        .strip_prefix("0x")
        .ok_or_else(|| parse_error("Hyprland window address must use the canonical 0x prefix"))?;
    if digits.is_empty()
        || digits.len() > 16
        || digits == "0"
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(parse_error(
            "Hyprland window address must contain 1..=16 lowercase hexadecimal digits",
        ));
    }
    Ok(())
}

fn validate_bounded_string(
    value: &str,
    maximum: usize,
    description: &str,
    allow_empty: bool,
) -> Result<(), CompositorError> {
    if (!allow_empty && value.is_empty())
        || value.len() > maximum
        || value.chars().any(char::is_control)
    {
        return Err(parse_error(format!(
            "Hyprland {description} is empty, overlong, or contains controls"
        )));
    }
    Ok(())
}
