// SPDX-License-Identifier: GPL-3.0-only

//! Clipboard production adapter. Full clipboard bytes never enter generic
//! desktop events; the v3 snapshot contains bounded previews and metadata.

use std::{io, time::Duration};

use sleepy_sdk::{ClipboardEntry, StableId};

use crate::system::CommandSpec;

const ENTRY_PREFIX: &str = "clipboard:";

pub fn list_spec() -> CommandSpec {
    bounded(["list"])
}

pub fn clear_spec() -> CommandSpec {
    bounded(["wipe"])
}

pub fn decode_spec(entry_id: &StableId) -> io::Result<CommandSpec> {
    Ok(bounded(vec!["decode".to_owned(), numeric_entry(entry_id)?]))
}

pub fn parse_entries(bytes: &[u8]) -> io::Result<Vec<ClipboardEntry>> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "clipboard metadata is not UTF-8",
        )
    })?;
    let mut entries = Vec::new();
    for line in text.lines().filter(|line| !line.is_empty()) {
        let fields = line.splitn(4, '\t').collect::<Vec<_>>();
        if fields.len() != 4 || fields[1].trim().is_empty() {
            return invalid("clipboard metadata row is malformed");
        }
        let numeric = numeric(fields[0])?;
        let byte_length = fields[3].parse::<u64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "clipboard length is invalid")
        })?;
        entries.push(ClipboardEntry {
            id: format!("{ENTRY_PREFIX}{numeric}"),
            mime_type: fields[1].to_owned(),
            preview: fields[2].chars().take(512).collect(),
            byte_length,
        });
        if entries.len() > 500 {
            return invalid("clipboard history exceeds 500 entries");
        }
    }
    Ok(entries)
}

pub(crate) fn numeric_entry(entry_id: &StableId) -> io::Result<String> {
    numeric(
        entry_id
            .as_str()
            .strip_prefix(ENTRY_PREFIX)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid clipboard ID"))?,
    )
}

pub(crate) fn parse_list(bytes: &[u8]) -> io::Result<Vec<(String, String)>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "cliphist list is not UTF-8"))?;
    let mut rows = Vec::new();
    for line in text.lines().filter(|line| !line.is_empty()).take(501) {
        let (id, preview) = line.split_once('\t').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "cliphist row is malformed")
        })?;
        rows.push((numeric(id)?, preview.chars().take(512).collect()));
    }
    if rows.len() > 500 {
        return invalid("clipboard history exceeds 500 entries");
    }
    Ok(rows)
}

fn bounded<I, S>(args: I) -> CommandSpec
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut spec = CommandSpec::new("cliphist", args);
    spec.timeout = Duration::from_secs(5);
    spec
}

fn numeric(value: &str) -> io::Result<String> {
    let number = value
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid clipboard entry ID"))?;
    if number.to_string() != value {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "clipboard entry ID is not canonical",
        ));
    }
    Ok(value.to_owned())
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}
