// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, TimeDelta, TimeZone, Utc};
use sleepy_sdk::{CalendarEvent, CalendarSnapshot, CalendarSourceError, WIRE_SCHEMA_VERSION};

const MAX_ICS_BYTES: u64 = 2 * 1024 * 1024;
const MAX_UNFOLDED_LINES: usize = 65_536;

pub struct IcsCalendarProvider {
    sources: Vec<PathBuf>,
    max_occurrences: usize,
}

impl IcsCalendarProvider {
    pub fn new(sources: Vec<PathBuf>, max_occurrences: usize) -> Self {
        Self {
            sources,
            max_occurrences: max_occurrences.clamp(1, 4096),
        }
    }

    pub fn snapshot(&self, window_start: &str, window_end: &str) -> io::Result<CalendarSnapshot> {
        let start = parse_utc(window_start)?;
        let end = parse_utc(window_end)?;
        if start >= end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "calendar window is not ordered",
            ));
        }
        let mut events = Vec::new();
        let mut source_errors = Vec::new();
        for source in &self.sources {
            let source_id = source
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("unknown")
                .to_owned();
            match parse_source(source, &source_id, start, end, self.max_occurrences) {
                Ok(mut parsed) => events.append(&mut parsed),
                Err(error) => source_errors.push(CalendarSourceError {
                    source_id,
                    message: error.to_string(),
                }),
            }
        }
        events.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(CalendarSnapshot {
            schema_version: WIRE_SCHEMA_VERSION,
            provider_id: "local-ics".into(),
            window_start: window_start.into(),
            window_end: window_end.into(),
            events,
            source_errors,
        })
    }
}

impl sleepy_sdk::CalendarProvider for IcsCalendarProvider {
    fn snapshot(
        &self,
        window_start: &str,
        window_end: &str,
    ) -> Result<CalendarSnapshot, sleepy_sdk::ContractError> {
        IcsCalendarProvider::snapshot(self, window_start, window_end)
            .map_err(|_| calendar_contract_error())
    }
}

fn calendar_contract_error() -> sleepy_sdk::ContractError {
    sleepy_sdk::validate_calendar_snapshot("{}")
        .expect_err("an empty object is always an invalid strict calendar snapshot")
}

#[derive(Default)]
struct RawEvent {
    uid: Option<String>,
    summary: Option<String>,
    location: Option<String>,
    start: Option<ParsedTime>,
    end: Option<ParsedTime>,
    rule: Option<Recurrence>,
    rdates: Vec<DateTime<Utc>>,
    exdates: BTreeSet<DateTime<Utc>>,
}

#[derive(Clone)]
struct ParsedTime {
    at: DateTime<Utc>,
    all_day: bool,
    local: Option<(NaiveDateTime, String)>,
}

#[derive(Clone)]
struct Recurrence {
    frequency: Frequency,
    count: Option<usize>,
    until: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy)]
enum Frequency {
    Daily,
    Weekly,
}

fn parse_source(
    path: &PathBuf,
    source_id: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    limit: usize,
) -> io::Result<Vec<CalendarEvent>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_ICS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ICS source is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_ICS_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_ICS_BYTES as usize {
        return Err(invalid("ICS source exceeded limit"));
    }
    let text = String::from_utf8(bytes).map_err(|_| invalid("ICS source is not UTF-8"))?;
    let lines = unfold(&text)?;
    if lines.first().map(String::as_str) != Some("BEGIN:VCALENDAR")
        || lines.last().map(String::as_str) != Some("END:VCALENDAR")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ICS source is not a VCALENDAR",
        ));
    }
    let mut raw = None::<RawEvent>;
    let mut parsed = Vec::new();
    for line in lines {
        if line == "BEGIN:VEVENT" {
            if raw.is_some() {
                return Err(invalid("nested VEVENT"));
            }
            raw = Some(RawEvent::default());
            continue;
        }
        if line == "END:VEVENT" {
            let event = raw.take().ok_or_else(|| invalid("unmatched VEVENT end"))?;
            parsed.extend(expand(event, source_id, window_start, window_end, limit)?);
            if parsed.len() > limit {
                return Err(invalid("calendar expansion exceeded limit"));
            }
            continue;
        }
        let Some(event) = raw.as_mut() else { continue };
        let Some((left, value)) = line.split_once(':') else {
            return Err(invalid("malformed ICS content line"));
        };
        let mut fields = left.split(';');
        let name = fields.next().unwrap_or_default();
        let parameters = fields
            .filter_map(|field| field.split_once('='))
            .collect::<Vec<_>>();
        match name {
            "UID" => event.uid = Some(unescape(value)),
            "SUMMARY" => event.summary = Some(unescape(value)),
            "LOCATION" => event.location = Some(unescape(value)),
            "DTSTART" => event.start = Some(parse_time(value, &parameters)?),
            "DTEND" => event.end = Some(parse_time(value, &parameters)?),
            "RRULE" => event.rule = Some(parse_rule(value)?),
            "RDATE" => event.rdates.extend(parse_date_list(value, &parameters)?),
            "EXDATE" => event.exdates.extend(parse_date_list(value, &parameters)?),
            _ => {}
        }
    }
    if raw.is_some() {
        return Err(invalid("unterminated VEVENT"));
    }
    Ok(parsed)
}

fn unfold(input: &str) -> io::Result<Vec<String>> {
    let mut lines = Vec::<String>::new();
    for raw in input.lines() {
        let line = raw.trim_end_matches('\r');
        if line.starts_with([' ', '\t']) {
            let previous = lines
                .last_mut()
                .ok_or_else(|| invalid("orphan folded line"))?;
            previous.push_str(&line[1..]);
        } else {
            lines.push(line.to_owned());
        }
        if lines.len() > MAX_UNFOLDED_LINES {
            return Err(invalid("too many ICS lines"));
        }
    }
    Ok(lines)
}

fn parse_time(value: &str, parameters: &[(&str, &str)]) -> io::Result<ParsedTime> {
    let all_day = parameters
        .iter()
        .any(|(key, value)| *key == "VALUE" && *value == "DATE");
    if all_day {
        let date = NaiveDate::parse_from_str(value, "%Y%m%d")
            .map_err(|_| invalid("invalid all-day date"))?;
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| invalid("invalid all-day date"))?;
        return Ok(ParsedTime {
            at: Utc.from_utc_datetime(&midnight),
            all_day: true,
            local: None,
        });
    }
    if value.ends_with('Z') {
        let at = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ")
            .map_err(|_| invalid("invalid UTC date-time"))?;
        return Ok(ParsedTime {
            at: Utc.from_utc_datetime(&at),
            all_day: false,
            local: None,
        });
    }
    let timezone = parameters
        .iter()
        .find(|(key, _)| *key == "TZID")
        .map(|(_, value)| *value)
        .ok_or_else(|| invalid("floating ICS time is unsupported"))?;
    let local = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S")
        .map_err(|_| invalid("invalid local date-time"))?;
    let at = resolve_local_time(timezone, local)?;
    Ok(ParsedTime {
        at,
        all_day: false,
        local: Some((local, timezone.to_owned())),
    })
}

fn parse_date_list(value: &str, parameters: &[(&str, &str)]) -> io::Result<Vec<DateTime<Utc>>> {
    value
        .split(',')
        .map(|item| parse_time(item, parameters).map(|parsed| parsed.at))
        .collect()
}

fn parse_rule(value: &str) -> io::Result<Recurrence> {
    let mut frequency = None;
    let mut count = None;
    let mut until = None;
    for part in value.split(';') {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| invalid("malformed RRULE"))?;
        match key {
            "FREQ" => {
                frequency = Some(match value {
                    "DAILY" => Frequency::Daily,
                    "WEEKLY" => Frequency::Weekly,
                    _ => return Err(invalid("unsupported RRULE frequency")),
                })
            }
            "COUNT" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| invalid("invalid RRULE count"))?;
                if parsed == 0 {
                    return Err(invalid("invalid RRULE count"));
                }
                count = Some(parsed)
            }
            "UNTIL" => until = Some(parse_time(value, &[])?.at),
            "INTERVAL" if value == "1" => {}
            _ => return Err(invalid("unsupported RRULE component")),
        }
    }
    Ok(Recurrence {
        frequency: frequency.ok_or_else(|| invalid("RRULE missing frequency"))?,
        count,
        until,
    })
}

fn expand(
    raw: RawEvent,
    source_id: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    limit: usize,
) -> io::Result<Vec<CalendarEvent>> {
    let uid = raw
        .uid
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("VEVENT missing UID"))?;
    let summary = raw
        .summary
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("VEVENT missing SUMMARY"))?;
    let start = raw.start.ok_or_else(|| invalid("VEVENT missing DTSTART"))?;
    let end = raw.end.unwrap_or_else(|| ParsedTime {
        at: if start.all_day {
            start.at + TimeDelta::days(1)
        } else {
            start.at
        },
        all_day: start.all_day,
        local: None,
    });
    if end.at <= start.at {
        return Err(invalid("VEVENT interval is not ordered"));
    }
    let duration = end.at - start.at;
    let civil_duration = start.local.as_ref().zip(end.local.as_ref()).and_then(
        |((start_local, start_tz), (end_local, end_tz))| {
            (start_tz == end_tz).then_some(*end_local - *start_local)
        },
    );
    let mut starts = vec![(start.at, start.local.clone())];
    if let Some(rule) = raw.rule {
        let step = match rule.frequency {
            Frequency::Daily => Days::new(1),
            Frequency::Weekly => Days::new(7),
        };
        let count = rule.count.unwrap_or(usize::MAX);
        let mut current = start.at;
        let mut current_local = start.local.clone();
        for _ in 1..count {
            if let Some((local, timezone)) = current_local.as_mut() {
                *local = local
                    .checked_add_days(step)
                    .ok_or_else(|| invalid("RRULE overflow"))?;
                current = resolve_local_time(timezone, *local)?;
            } else {
                current = current
                    .checked_add_days(step)
                    .ok_or_else(|| invalid("RRULE overflow"))?;
            }
            if rule.until.is_some_and(|until| current > until) {
                break;
            }
            if current >= window_end {
                break;
            }
            if starts.len() >= limit {
                return Err(invalid("RRULE expansion exceeded limit"));
            }
            starts.push((current, current_local.clone()));
        }
    }
    starts.extend(raw.rdates.into_iter().map(|at| (at, None)));
    starts.sort_by_key(|value| value.0);
    starts.dedup_by_key(|value| value.0);
    if starts.len() > limit {
        return Err(invalid("calendar expansion exceeded limit"));
    }
    let mut events = Vec::new();
    for (at, local) in starts {
        if raw.exdates.contains(&at) {
            continue;
        }
        let occurrence_end = match (local, civil_duration) {
            (Some((local, timezone)), Some(civil)) => resolve_local_time(&timezone, local + civil)?,
            _ => at + duration,
        };
        if occurrence_end > window_start && at < window_end {
            events.push(CalendarEvent {
                id: format!("{uid}@{}", at.timestamp()),
                summary: summary.clone(),
                starts_at: format_time(at),
                ends_at: format_time(occurrence_end),
                all_day: start.all_day,
                source_id: source_id.to_owned(),
                location: raw.location.clone(),
            });
        }
    }
    Ok(events)
}

fn parse_utc(value: &str) -> io::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| invalid("invalid UTC window"))
}

fn format_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn unescape(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
}

fn resolve_local_time(name: &str, local: NaiveDateTime) -> io::Result<DateTime<Utc>> {
    if name.is_empty()
        || name.starts_with('/')
        || name
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("invalid ICS timezone name"));
    }
    let root = std::env::var_os("TZDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/share/zoneinfo"));
    let data = fs::read(root.join(name)).map_err(|_| invalid("unknown ICS timezone"))?;
    let zone = Tzif::parse(&data)?;
    let naive_epoch = local.and_utc().timestamp();
    let mut matches = zone
        .offsets
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|offset| {
            let candidate = naive_epoch.checked_sub(i64::from(offset))?;
            (zone.offset_at(candidate) == offset).then_some(candidate)
        })
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    if matches.len() != 1 {
        return Err(invalid("ambiguous or nonexistent local time"));
    }
    DateTime::from_timestamp(matches[0], 0).ok_or_else(|| invalid("timezone conversion overflow"))
}

struct Tzif {
    transitions: Vec<i64>,
    indices: Vec<u8>,
    offsets: Vec<i32>,
    default_index: usize,
}

impl Tzif {
    fn parse(data: &[u8]) -> io::Result<Self> {
        if data.len() < 44 || &data[..4] != b"TZif" {
            return Err(invalid("invalid timezone data"));
        }
        let version = data[4];
        let first = counts(&data[..44])?;
        let first_size = block_size(first, 4)?;
        let (header, width) = if matches!(version, b'2' | b'3' | b'4') {
            let offset = 44usize
                .checked_add(first_size)
                .ok_or_else(|| invalid("timezone data overflow"))?;
            if data.len() < offset + 44 || &data[offset..offset + 4] != b"TZif" {
                return Err(invalid("invalid timezone v2 data"));
            }
            (&data[offset..offset + 44], 8)
        } else {
            (&data[..44], 4)
        };
        let count = counts(header)?;
        let start = header.as_ptr() as usize - data.as_ptr() as usize + 44;
        let need = block_size(count, width)?;
        if data.len() < start + need || count.types == 0 {
            return Err(invalid("truncated timezone data"));
        }
        let mut cursor = start;
        let mut transitions = Vec::with_capacity(count.times);
        for _ in 0..count.times {
            let value = if width == 8 {
                read_i64(data, cursor)?
            } else {
                i64::from(read_i32(data, cursor)?)
            };
            transitions.push(value);
            cursor += width;
        }
        let indices = data[cursor..cursor + count.times].to_vec();
        cursor += count.times;
        let mut offsets = Vec::with_capacity(count.types);
        let mut daylight = Vec::with_capacity(count.types);
        for _ in 0..count.types {
            offsets.push(read_i32(data, cursor)?);
            daylight.push(data[cursor + 4] != 0);
            cursor += 6;
        }
        if indices
            .iter()
            .any(|index| usize::from(*index) >= offsets.len())
        {
            return Err(invalid("invalid timezone transition type"));
        }
        let default_index = daylight.iter().position(|value| !*value).unwrap_or(0);
        Ok(Self {
            transitions,
            indices,
            offsets,
            default_index,
        })
    }

    fn offset_at(&self, epoch: i64) -> i32 {
        match self
            .transitions
            .partition_point(|transition| *transition <= epoch)
        {
            0 => self.offsets[self.default_index],
            position => self.offsets[usize::from(self.indices[position - 1])],
        }
    }
}

#[derive(Clone, Copy)]
struct TzifCounts {
    times: usize,
    types: usize,
    chars: usize,
    leaps: usize,
    std: usize,
    gmt: usize,
}

fn counts(header: &[u8]) -> io::Result<TzifCounts> {
    if header.len() < 44 {
        return Err(invalid("truncated timezone header"));
    }
    let number = |offset: usize| {
        read_u32(header, offset).and_then(|value| {
            usize::try_from(value).map_err(|_| invalid("timezone count overflow"))
        })
    };
    Ok(TzifCounts {
        gmt: number(20)?,
        std: number(24)?,
        leaps: number(28)?,
        times: number(32)?,
        types: number(36)?,
        chars: number(40)?,
    })
}

fn read_u32(data: &[u8], offset: usize) -> io::Result<u32> {
    let bytes: [u8; 4] = data
        .get(offset..offset + 4)
        .ok_or_else(|| invalid("truncated timezone integer"))?
        .try_into()
        .map_err(|_| invalid("invalid timezone integer"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_i32(data: &[u8], offset: usize) -> io::Result<i32> {
    let bytes: [u8; 4] = data
        .get(offset..offset + 4)
        .ok_or_else(|| invalid("truncated timezone integer"))?
        .try_into()
        .map_err(|_| invalid("invalid timezone integer"))?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_i64(data: &[u8], offset: usize) -> io::Result<i64> {
    let bytes: [u8; 8] = data
        .get(offset..offset + 8)
        .ok_or_else(|| invalid("truncated timezone integer"))?
        .try_into()
        .map_err(|_| invalid("invalid timezone integer"))?;
    Ok(i64::from_be_bytes(bytes))
}

fn block_size(count: TzifCounts, width: usize) -> io::Result<usize> {
    count
        .times
        .checked_mul(width)
        .and_then(|value| value.checked_add(count.times))
        .and_then(|value| value.checked_add(count.types.checked_mul(6)?))
        .and_then(|value| value.checked_add(count.chars))
        .and_then(|value| value.checked_add(count.leaps.checked_mul(width + 4)?))
        .and_then(|value| value.checked_add(count.std))
        .and_then(|value| value.checked_add(count.gmt))
        .ok_or_else(|| invalid("timezone data overflow"))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
