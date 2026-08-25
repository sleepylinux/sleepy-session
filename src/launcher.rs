// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::store::SecureDir;

const MAX_ENTRY_BYTES: u64 = 256 * 1024;
const MAX_ENTRIES: usize = 16_384;
const MAX_DIRECTORIES: usize = 4_096;
const MAX_DEPTH: usize = 32;
const MAX_RESOURCES: usize = 256;
const MAX_RESOURCE_BYTES: usize = 16 * 1024;
const MAX_RESOURCE_TOTAL_BYTES: usize = 1024 * 1024;
const METRICS_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DesktopAction {
    pub id: String,
    pub name: String,
    exec: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DesktopEntry {
    pub desktop_id: String,
    pub name: String,
    pub icon: Option<String>,
    pub actions: Vec<DesktopAction>,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    exec: String,
}

#[derive(Debug, Default)]
pub struct DesktopEntryIndex {
    entries: BTreeMap<String, DesktopEntry>,
}

#[derive(Debug, Default)]
pub struct LaunchResources {
    pub files: Vec<String>,
    pub urls: Vec<String>,
}

impl DesktopEntryIndex {
    pub fn scan_xdg<F>(try_exec: F) -> io::Result<Self>
    where
        F: Fn(&str) -> bool,
    {
        let data_home = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
            })
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "XDG data home is unavailable")
            })?;
        let data_dirs = std::env::var_os("XDG_DATA_DIRS")
            .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .unwrap_or_else(|| {
                vec![
                    PathBuf::from("/usr/local/share"),
                    PathBuf::from("/usr/share"),
                ]
            });
        let mut roots = vec![data_home.join("applications")];
        roots.extend(data_dirs.into_iter().map(|root| root.join("applications")));
        if roots.iter().any(|root| !root.is_absolute()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "XDG application roots must be absolute",
            ));
        }
        let desktops = std::env::var("XDG_CURRENT_DESKTOP")
            .unwrap_or_default()
            .split(':')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        Self::scan(&roots, &desktops, try_exec)
    }

    pub fn scan<F>(roots: &[PathBuf], desktops: &[String], try_exec: F) -> io::Result<Self>
    where
        F: Fn(&str) -> bool,
    {
        let mut shadowed = BTreeSet::new();
        let mut entries = BTreeMap::new();
        for root in roots {
            if !root.is_dir() {
                continue;
            }
            let mut paths = Vec::new();
            collect_desktop_files(root, root, &mut paths)?;
            paths.sort();
            for path in paths {
                if shadowed.len() >= MAX_ENTRIES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "too many Desktop Entries",
                    ));
                }
                let relative = path.strip_prefix(root).map_err(io::Error::other)?;
                let desktop_id = relative.to_string_lossy().replace('/', "-");
                if !shadowed.insert(desktop_id.clone()) {
                    continue;
                }
                if let Ok(Some(entry)) = parse_entry(&path, desktop_id, desktops, &try_exec) {
                    entries.insert(entry.desktop_id.clone(), entry);
                }
            }
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> Vec<&DesktopEntry> {
        self.entries.values().collect()
    }

    pub fn get(&self, desktop_id: &str) -> Option<&DesktopEntry> {
        self.entries.get(desktop_id)
    }

    pub fn launch_argv(
        &self,
        desktop_id: &str,
        action_id: Option<&str>,
        resources: &LaunchResources,
    ) -> io::Result<Vec<String>> {
        validate_resources(resources)?;
        let entry = self.entries.get(desktop_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Desktop Entry is not indexed")
        })?;
        let exec = match action_id {
            None => &entry.exec,
            Some(id) => {
                &entry
                    .actions
                    .iter()
                    .find(|action| action.id == id)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "Desktop Action is not indexed")
                    })?
                    .exec
            }
        };
        expand_exec(exec, entry, resources)
    }
}

fn collect_desktop_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<PathBuf>,
) -> io::Result<()> {
    let mut pending = vec![(directory.to_owned(), 0usize)];
    let mut directory_count = 0usize;
    while let Some((directory, depth)) = pending.pop() {
        directory_count += 1;
        if directory_count > MAX_DIRECTORIES || depth > MAX_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Desktop Entry traversal exceeded limits below {}",
                    root.display()
                ),
            ));
        }
        for item in fs::read_dir(directory)? {
            let item = item?;
            let file_type = item.file_type()?;
            let path = item.path();
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                pending.push((path, depth + 1));
            } else if file_type.is_file()
                && path.extension().is_some_and(|value| value == "desktop")
            {
                output.push(path);
            }
            if output.len() > MAX_ENTRIES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("too many entries below {}", root.display()),
                ));
            }
        }
    }
    Ok(())
}

fn validate_resources(resources: &LaunchResources) -> io::Result<()> {
    if resources.files.len().saturating_add(resources.urls.len()) > MAX_RESOURCES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many launch resources",
        ));
    }
    let mut total = 0usize;
    for resource in resources.files.iter().chain(&resources.urls) {
        if resource.is_empty() || resource.contains('\0') || resource.len() > MAX_RESOURCE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch resource is invalid or oversized",
            ));
        }
        total = total.checked_add(resource.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "launch resources overflow")
        })?;
        if total > MAX_RESOURCE_TOTAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "launch resources exceed aggregate limit",
            ));
        }
    }
    Ok(())
}

fn parse_entry<F>(
    path: &Path,
    desktop_id: String,
    desktops: &[String],
    try_exec: &F,
) -> io::Result<Option<DesktopEntry>>
where
    F: Fn(&str) -> bool,
{
    let Some(input) = read_bounded_nofollow(path, MAX_ENTRY_BYTES)? else {
        return Ok(None);
    };
    if input.contains('\0') {
        return Ok(None);
    }
    let mut groups: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut group = String::new();
    for raw in input.lines().take(8193) {
        let line = raw.trim_end_matches('\r');
        if line.starts_with('[') && line.ends_with(']') {
            group = line[1..line.len() - 1].to_owned();
        } else if !line.is_empty() && !line.starts_with('#') {
            let Some((key, value)) = line.split_once('=') else {
                return Ok(None);
            };
            groups
                .entry(group.clone())
                .or_default()
                .insert(key.to_owned(), value.to_owned());
        }
    }
    if input.lines().count() > 8192 {
        return Ok(None);
    }
    let Some(main) = groups.get("Desktop Entry") else {
        return Ok(None);
    };
    if main.get("Type").map(String::as_str) != Some("Application")
        || boolean(main.get("Hidden"))
        || boolean(main.get("NoDisplay"))
        || !desktop_visible(main, desktops)
    {
        return Ok(None);
    }
    let (Some(name), Some(exec)) = (main.get("Name"), main.get("Exec")) else {
        return Ok(None);
    };
    if name.is_empty()
        || exec.is_empty()
        || main
            .get("TryExec")
            .is_some_and(|program| !try_exec(program))
    {
        return Ok(None);
    }
    let mut actions = Vec::new();
    for id in list(main.get("Actions")) {
        if id.is_empty() || id.contains('/') {
            continue;
        }
        if let Some(values) = groups.get(&format!("Desktop Action {id}")) {
            if let (Some(action_name), Some(action_exec)) = (values.get("Name"), values.get("Exec"))
            {
                actions.push(DesktopAction {
                    id: id.to_owned(),
                    name: action_name.clone(),
                    exec: action_exec.clone(),
                });
            }
        }
    }
    let candidate = DesktopEntry {
        desktop_id,
        name: name.clone(),
        icon: main.get("Icon").cloned(),
        actions,
        path: path.to_owned(),
        exec: exec.clone(),
    };
    if expand_exec(&candidate.exec, &candidate, &LaunchResources::default()).is_err()
        || candidate.actions.iter().any(|action| {
            expand_exec(&action.exec, &candidate, &LaunchResources::default()).is_err()
        })
    {
        return Ok(None);
    }
    Ok(Some(candidate))
}

fn boolean(value: Option<&String>) -> bool {
    value.is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn list(value: Option<&String>) -> impl Iterator<Item = &str> {
    value
        .into_iter()
        .flat_map(|value| value.split(';'))
        .filter(|value| !value.is_empty())
}

fn desktop_visible(values: &BTreeMap<String, String>, desktops: &[String]) -> bool {
    let current: BTreeSet<_> = desktops.iter().map(String::as_str).collect();
    let only = list(values.get("OnlyShowIn")).collect::<Vec<_>>();
    let excluded = list(values.get("NotShowIn")).collect::<BTreeSet<_>>();
    (only.is_empty() || only.iter().any(|name| current.contains(name)))
        && current.is_disjoint(&excluded)
}

fn expand_exec(
    exec: &str,
    entry: &DesktopEntry,
    resources: &LaunchResources,
) -> io::Result<Vec<String>> {
    let tokens = tokenize(exec)?;
    if tokens.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Exec is empty"));
    }
    let mut output = Vec::new();
    for token in tokens {
        if token == "%F" {
            output.extend(resources.files.iter().cloned());
            continue;
        }
        if token == "%U" {
            output.extend(resources.urls.iter().cloned());
            continue;
        }
        if token == "%i" {
            if let Some(icon) = &entry.icon {
                output.extend(["--icon".to_owned(), icon.clone()]);
            }
            continue;
        }
        let mut expanded = String::new();
        let chars: Vec<char> = token.chars().collect();
        let mut index = 0;
        while index < chars.len() {
            if chars[index] != '%' {
                expanded.push(chars[index]);
                index += 1;
                continue;
            }
            index += 1;
            let Some(code) = chars.get(index).copied() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "trailing Exec field code",
                ));
            };
            match code {
                '%' => expanded.push('%'),
                'c' => expanded.push_str(&entry.name),
                'k' => expanded.push_str(&entry.path.to_string_lossy()),
                'f' => {
                    if let Some(file) = resources.files.first() {
                        expanded.push_str(file);
                    }
                }
                'u' => {
                    if let Some(url) = resources.urls.first() {
                        expanded.push_str(url);
                    }
                }
                // Removed by the current Desktop Entry specification. Legacy
                // entries remain launchable, but the obsolete substitution is
                // never interpreted or forwarded.
                'd' | 'D' | 'n' | 'N' | 'v' | 'm' => {}
                'F' | 'U' | 'i' => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "multi-argument field code must be a complete argument",
                    ))
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported Exec field code",
                    ))
                }
            }
            index += 1;
        }
        if !expanded.is_empty() {
            output.push(expanded);
        }
    }
    if output.is_empty() || output[0].is_empty() {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Exec has no program",
        ))
    } else {
        Ok(output)
    }
}

fn tokenize(input: &str) -> io::Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut active = false;
    for character in input.chars() {
        if escaped {
            token.push(character);
            escaped = false;
            active = true;
        } else if character == '\\' {
            escaped = true;
            active = true;
        } else if character == '"' {
            quoted = !quoted;
            active = true;
        } else if character.is_whitespace() && !quoted {
            if active {
                tokens.push(std::mem::take(&mut token));
                active = false;
            }
        } else {
            token.push(character);
            active = true;
        }
    }
    if escaped || quoted {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed Exec quoting",
        ));
    }
    if active {
        tokens.push(token);
    }
    Ok(tokens)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Usage {
    count: u64,
    last_used: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetricsDocument {
    schema_version: u32,
    entries: BTreeMap<String, Usage>,
}

pub struct LauncherMetrics {
    directory: SecureDir,
    name: OsString,
    entries: BTreeMap<String, Usage>,
}

impl LauncherMetrics {
    pub fn open(path: &Path) -> io::Result<Self> {
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "metrics path has no parent")
        })?;
        let name = path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "metrics path has no filename")
            })?
            .to_owned();
        let directory = SecureDir::open_writable(parent, true).map_err(io::Error::other)?;
        directory
            .validate_private_file_if_present(&name)
            .map_err(io::Error::other)?;
        let entries =
            if let Some(bytes) = directory.read_optional(&name).map_err(io::Error::other)? {
                if bytes.len() > MAX_ENTRY_BYTES as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "launcher metrics exceed limit",
                    ));
                }
                let document: MetricsDocument = serde_json::from_slice(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                if document.schema_version != METRICS_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown launcher metrics schema",
                    ));
                }
                document.entries
            } else {
                BTreeMap::new()
            };
        Ok(Self {
            directory,
            name,
            entries,
        })
    }

    pub fn record_launch(&mut self, desktop_id: &str, now: u64) -> io::Result<()> {
        if !desktop_id.ends_with(".desktop") || desktop_id.contains('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid desktop id",
            ));
        }
        let usage = self.entries.entry(desktop_id.to_owned()).or_default();
        usage.count = usage.count.saturating_add(1);
        usage.last_used = usage.last_used.max(now);
        self.persist()
    }

    pub fn rank(&self, query: &str, candidates: &[&str]) -> Vec<String> {
        let query = query.to_lowercase();
        let mut scored = candidates
            .iter()
            .filter_map(|candidate| {
                fuzzy_score(&query, &candidate.to_lowercase()).map(|fuzzy| {
                    let usage = self.entries.get(*candidate).cloned().unwrap_or_default();
                    ((*candidate).to_owned(), fuzzy, usage.count, usage.last_used)
                })
            })
            .collect::<Vec<_>>();
        scored.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| b.3.cmp(&a.3))
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.into_iter().map(|value| value.0).collect()
    }

    fn persist(&self) -> io::Result<()> {
        let document = MetricsDocument {
            schema_version: METRICS_VERSION,
            entries: self.entries.clone(),
        };
        let mut bytes = serde_json::to_vec(&document).map_err(io::Error::other)?;
        bytes.push(b'\n');
        self.directory
            .atomic_replace(&self.name, &bytes, || Ok(()), || Ok(()), || Ok(()))
            .map_err(io::Error::other)
    }
}

fn read_bounded_nofollow(path: &Path, limit: u64) -> io::Result<Option<String>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => return Ok(None),
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() > limit {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit as usize {
        return Ok(None);
    }
    String::from_utf8(bytes).map(Some).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is not UTF-8", path.display()),
        )
    })
}

fn fuzzy_score(query: &str, candidate: &str) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }
    let mut position = 0usize;
    let mut score = 0i64;
    for character in query.chars() {
        let found = candidate[position..].find(character)?;
        score += 100 - found as i64;
        position += found + character.len_utf8();
    }
    Some(score)
}
