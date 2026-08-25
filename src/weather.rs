// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    CacheStatus, CapabilityFailure, ForecastPoint, ProviderStatus, WeatherLocation,
    WeatherSnapshot, WIRE_SCHEMA_VERSION,
};

use crate::{
    store::SecureDir,
    system::{CommandRunner, CommandSpec, ProcessCommandRunner},
};

const MAX_HTTP_BODY: usize = 2 * 1024 * 1024;
const CACHE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

pub trait HttpTransport: Clone + Send + Sync + 'static {
    fn execute(&self, request: HttpRequest) -> io::Result<HttpResponse>;
}

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> u64;
}

#[derive(Clone)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(now: u64) -> Self {
        Self(Arc::new(AtomicU64::new(now)))
    }
    pub fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

#[derive(Clone, Default)]
pub struct CurlTransport;

impl HttpTransport for CurlTransport {
    fn execute(&self, request: HttpRequest) -> io::Result<HttpResponse> {
        if !is_https(&request.url) {
            return Err(invalid("HTTP transport requires HTTPS"));
        }
        let mut args = vec![
            "--silent".into(),
            "--show-error".into(),
            "--include".into(),
            "--max-time".into(),
            request.timeout.as_secs().max(1).to_string(),
        ];
        for (name, value) in &request.headers {
            args.extend(["--header".into(), format!("{name}: {value}")]);
        }
        args.push(request.url);
        let mut command = CommandSpec::new("curl", args);
        command.timeout = request.timeout;
        let output = ProcessCommandRunner
            .run(&command)
            .map_err(io::Error::other)?;
        if output.status != 0 {
            return Err(io::Error::other("curl request failed"));
        }
        parse_curl_response(&output.stdout)
    }
}

fn parse_curl_response(bytes: &[u8]) -> io::Result<HttpResponse> {
    if bytes.len() > MAX_HTTP_BODY + 64 * 1024 {
        return Err(invalid("HTTP response exceeded limit"));
    }
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| invalid("HTTP response has no header boundary"))?;
    let headers_text =
        std::str::from_utf8(&bytes[..split]).map_err(|_| invalid("HTTP headers are not UTF-8"))?;
    let mut lines = headers_text.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| invalid("HTTP status is malformed"))?;
    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    let body = bytes[split + 4..].to_vec();
    if body.len() > MAX_HTTP_BODY {
        return Err(invalid("HTTP body exceeded limit"));
    }
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetCache {
    schema_version: u32,
    location_key: String,
    expires_at: u64,
    last_modified: Option<String>,
    forecast: Vec<ForecastPoint>,
}

pub struct MetNoProvider<T, C> {
    endpoint: String,
    user_agent: String,
    cache_path: PathBuf,
    transport: T,
    clock: C,
    cache: Mutex<Option<MetCache>>,
}

impl<T: HttpTransport, C: Clock> MetNoProvider<T, C> {
    pub fn new(
        endpoint: &str,
        user_agent: &str,
        cache_path: PathBuf,
        transport: T,
        clock: C,
    ) -> io::Result<Self> {
        validate_endpoint_and_agent(endpoint, user_agent)?;
        let cache = read_json_if_present::<MetCache>(&cache_path)?;
        if cache
            .as_ref()
            .is_some_and(|cache| cache.schema_version != CACHE_VERSION)
        {
            return Err(invalid("unknown MET cache schema"));
        }
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').into(),
            user_agent: user_agent.into(),
            cache_path,
            transport,
            clock,
            cache: Mutex::new(cache),
        })
    }

    pub fn snapshot(&self, location: &WeatherLocation) -> io::Result<WeatherSnapshot> {
        validate_location(location)?;
        let latitude = format_coordinate(location.latitude);
        let longitude = format_coordinate(location.longitude);
        let key = format!("{latitude},{longitude}");
        let now = self.clock.now();
        let existing = self
            .cache
            .lock()
            .map_err(|_| io::Error::other("MET cache lock poisoned"))?
            .clone()
            .filter(|cache| cache.location_key == key);
        if let Some(cache) = &existing {
            if now < cache.expires_at {
                return Ok(weather(
                    location,
                    ProviderStatus::Online,
                    CacheStatus::Fresh,
                    cache.forecast.clone(),
                    None,
                ));
            }
        }
        let mut headers = BTreeMap::from([
            ("User-Agent".into(), self.user_agent.clone()),
            ("Accept".into(), "application/json".into()),
        ]);
        if let Some(last_modified) = existing
            .as_ref()
            .and_then(|cache| cache.last_modified.clone())
        {
            headers.insert("If-Modified-Since".into(), last_modified);
        }
        let request = HttpRequest {
            url: format!("{}?lat={latitude}&lon={longitude}", self.endpoint),
            headers,
            timeout: Duration::from_secs(6),
        };
        let response = match self.transport.execute(request) {
            Ok(response) => response,
            Err(error) => {
                return Ok(unavailable(
                    location,
                    existing,
                    ProviderStatus::Offline,
                    &error.to_string(),
                ))
            }
        };
        match response.status {
            200 | 203 => {
                let forecast = match parse_met(&response.body) {
                    Ok(forecast) => forecast,
                    Err(error) => {
                        return Ok(unavailable(
                            location,
                            existing,
                            ProviderStatus::Error,
                            &error.to_string(),
                        ))
                    }
                };
                let cache = MetCache {
                    schema_version: CACHE_VERSION,
                    location_key: key,
                    expires_at: expires(&response.headers, now),
                    last_modified: header(&response.headers, "Last-Modified").map(str::to_owned),
                    forecast: forecast.clone(),
                };
                write_json_private(&self.cache_path, &cache)?;
                *self
                    .cache
                    .lock()
                    .map_err(|_| io::Error::other("MET cache lock poisoned"))? = Some(cache);
                Ok(weather(
                    location,
                    ProviderStatus::Online,
                    CacheStatus::Fresh,
                    forecast,
                    None,
                ))
            }
            304 => {
                let mut cache =
                    existing.ok_or_else(|| invalid("MET returned 304 without a safe cache"))?;
                cache.expires_at = expires(&response.headers, now);
                write_json_private(&self.cache_path, &cache)?;
                let forecast = cache.forecast.clone();
                *self
                    .cache
                    .lock()
                    .map_err(|_| io::Error::other("MET cache lock poisoned"))? = Some(cache);
                Ok(weather(
                    location,
                    ProviderStatus::Online,
                    CacheStatus::Fresh,
                    forecast,
                    None,
                ))
            }
            429 => Ok(unavailable(
                location,
                existing,
                ProviderStatus::Offline,
                "MET.no rate limited the request",
            )),
            _ => Ok(unavailable(
                location,
                existing,
                ProviderStatus::Error,
                &format!("MET.no returned HTTP {}", response.status),
            )),
        }
    }
}

impl<T: HttpTransport, C: Clock> sleepy_sdk::WeatherProvider for MetNoProvider<T, C> {
    fn snapshot(
        &self,
        location: &WeatherLocation,
    ) -> Result<WeatherSnapshot, sleepy_sdk::ContractError> {
        MetNoProvider::snapshot(self, location).map_err(|_| weather_contract_error())
    }
}

fn weather_contract_error() -> sleepy_sdk::ContractError {
    sleepy_sdk::validate_weather_snapshot("{}")
        .expect_err("an empty object is always an invalid strict weather snapshot")
}

fn parse_met(body: &[u8]) -> io::Result<Vec<ForecastPoint>> {
    if body.len() > MAX_HTTP_BODY {
        return Err(invalid("MET response exceeded limit"));
    }
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| invalid("MET response is malformed"))?;
    let series = value
        .pointer("/properties/timeseries")
        .and_then(|value| value.as_array())
        .ok_or_else(|| invalid("MET response omitted timeseries"))?;
    if series.len() > 512 {
        return Err(invalid("MET response has too many forecast points"));
    }
    series
        .iter()
        .map(|point| {
            let at = point
                .get("time")
                .and_then(|value| value.as_str())
                .ok_or_else(|| invalid("MET point omitted time"))?;
            DateTime::parse_from_rfc3339(at).map_err(|_| invalid("MET point time is invalid"))?;
            let temperature_c = point
                .pointer("/data/instant/details/air_temperature")
                .and_then(|value| value.as_f64())
                .filter(|value| value.is_finite())
                .ok_or_else(|| invalid("MET point omitted temperature"))?;
            let symbol = point
                .pointer("/data/next_1_hours/summary/symbol_code")
                .or_else(|| point.pointer("/data/next_6_hours/summary/symbol_code"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid("MET point omitted symbol"))?;
            Ok(ForecastPoint {
                at: at.to_owned(),
                temperature_c,
                symbol: symbol.to_owned(),
            })
        })
        .collect()
}

fn weather(
    location: &WeatherLocation,
    status: ProviderStatus,
    cache: CacheStatus,
    forecast: Vec<ForecastPoint>,
    diagnostic: Option<&str>,
) -> WeatherSnapshot {
    WeatherSnapshot {
        schema_version: WIRE_SCHEMA_VERSION,
        provider_id: "met-no".into(),
        location: location.clone(),
        status,
        cache,
        attribution: "Weather data from MET Norway".into(),
        forecast,
        diagnostic: diagnostic.map(|message| CapabilityFailure {
            message: message.into(),
        }),
    }
}

fn unavailable(
    location: &WeatherLocation,
    cache: Option<MetCache>,
    status: ProviderStatus,
    message: &str,
) -> WeatherSnapshot {
    let (cache_status, forecast) = cache.map_or((CacheStatus::Missing, Vec::new()), |cache| {
        (CacheStatus::Stale, cache.forecast)
    });
    weather(location, status, cache_status, forecast, Some(message))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GeocodingResult {
    pub display_name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub attribution: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GeocodeCache {
    schema_version: u32,
    queries: BTreeMap<String, Vec<GeocodingResult>>,
}

pub struct NominatimProvider<T, C> {
    endpoint: String,
    user_agent: String,
    cache_path: PathBuf,
    transport: T,
    clock: C,
    cache: Mutex<BTreeMap<String, Vec<GeocodingResult>>>,
}

static NOMINATIM_LAST_REQUEST: OnceLock<Mutex<Option<u64>>> = OnceLock::new();

impl<T: HttpTransport, C: Clock> NominatimProvider<T, C> {
    pub fn new(
        endpoint: &str,
        user_agent: &str,
        cache_path: PathBuf,
        transport: T,
        clock: C,
    ) -> io::Result<Self> {
        validate_endpoint_and_agent(endpoint, user_agent)?;
        let cache = read_json_if_present::<GeocodeCache>(&cache_path)?.map_or(
            Ok(BTreeMap::new()),
            |document| {
                if document.schema_version == CACHE_VERSION {
                    Ok(document.queries)
                } else {
                    Err(invalid("unknown geocoding cache schema"))
                }
            },
        )?;
        Ok(Self {
            endpoint: endpoint.into(),
            user_agent: user_agent.into(),
            cache_path,
            transport,
            clock,
            cache: Mutex::new(cache),
        })
    }

    pub fn autocomplete(&self, _query: &str) -> io::Result<Vec<GeocodingResult>> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Nominatim autocomplete is forbidden",
        ))
    }

    pub fn submit(&self, query: &str) -> io::Result<Vec<GeocodingResult>> {
        let query = query.trim();
        if query.len() < 2 || query.len() > 200 || query.contains(['@', '\n', '\r', '\0']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "geocoding query is invalid or sensitive",
            ));
        }
        let key = query.to_lowercase();
        if let Some(cached) = self
            .cache
            .lock()
            .map_err(|_| io::Error::other("geocoding cache lock poisoned"))?
            .get(&key)
            .cloned()
        {
            return Ok(cached);
        }
        let now = self.clock.now();
        let mut last_request = NOMINATIM_LAST_REQUEST
            .get_or_init(|| Mutex::new(None))
            .lock()
            .map_err(|_| io::Error::other("geocoding limiter lock poisoned"))?;
        if last_request.is_some_and(|last| now < last.saturating_add(1)) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Nominatim requests are limited to one per second",
            ));
        }
        *last_request = Some(now);
        drop(last_request);
        let request = HttpRequest {
            url: format!(
                "{}?q={}&format=jsonv2&limit=8",
                self.endpoint,
                percent_encode(query)
            ),
            headers: BTreeMap::from([
                ("User-Agent".into(), self.user_agent.clone()),
                ("Accept".into(), "application/json".into()),
            ]),
            timeout: Duration::from_secs(6),
        };
        let response = self.transport.execute(request)?;
        if response.status != 200 {
            return Err(io::Error::other(format!(
                "Nominatim returned HTTP {}",
                response.status
            )));
        }
        let raw: Vec<NominatimResult> = serde_json::from_slice(&response.body)
            .map_err(|_| invalid("Nominatim response is malformed"))?;
        if raw.len() > 8 {
            return Err(invalid("Nominatim response exceeded result limit"));
        }
        let results = raw
            .into_iter()
            .map(|result| {
                let latitude = result
                    .lat
                    .parse()
                    .map_err(|_| invalid("Nominatim latitude is invalid"))?;
                let longitude = result
                    .lon
                    .parse()
                    .map_err(|_| invalid("Nominatim longitude is invalid"))?;
                validate_location(&WeatherLocation {
                    display_name: result.display_name.clone(),
                    latitude,
                    longitude,
                })?;
                Ok(GeocodingResult {
                    display_name: result.display_name,
                    latitude,
                    longitude,
                    attribution: "© OpenStreetMap contributors".into(),
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| io::Error::other("geocoding cache lock poisoned"))?;
        cache.insert(key, results.clone());
        write_json_private(
            &self.cache_path,
            &GeocodeCache {
                schema_version: CACHE_VERSION,
                queries: cache.clone(),
            },
        )?;
        Ok(results)
    }
}

#[derive(Deserialize)]
struct NominatimResult {
    display_name: String,
    lat: String,
    lon: String,
}

fn validate_endpoint_and_agent(endpoint: &str, user_agent: &str) -> io::Result<()> {
    let authority = endpoint
        .strip_prefix("https://")
        .and_then(|rest| rest.split('/').next());
    if !is_https(endpoint)
        || endpoint.chars().any(char::is_whitespace)
        || endpoint.contains(['\n', '\r', '#'])
        || authority.is_none_or(|value| value.is_empty() || value.contains('@'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider endpoint must use HTTPS",
        ));
    }
    if user_agent.trim() != user_agent
        || user_agent.len() < 8
        || user_agent.contains(['\n', '\r'])
        || (!user_agent.contains('@') && !user_agent.contains("http"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider User-Agent must identify the application and contact",
        ));
    }
    Ok(())
}

fn is_https(url: &str) -> bool {
    url.starts_with("https://") && url.len() > 8
}

fn validate_location(location: &WeatherLocation) -> io::Result<()> {
    if location.display_name.trim().is_empty()
        || !location.latitude.is_finite()
        || !location.longitude.is_finite()
        || !(-90.0..=90.0).contains(&location.latitude)
        || !(-180.0..=180.0).contains(&location.longitude)
    {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "weather location is invalid",
        ))
    } else {
        Ok(())
    }
}

fn format_coordinate(value: f64) -> String {
    let formatted = format!("{value:.4}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

fn expires(headers: &BTreeMap<String, String>, now: u64) -> u64 {
    let Some(value) = header(headers, "Expires") else {
        return now.saturating_add(300);
    };
    value
        .parse()
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc2822(value)
                .ok()
                .map(|time| time.timestamp().max(0) as u64)
        })
        .unwrap_or(now.saturating_add(300))
}

fn header<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn read_json_if_present<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<T>> {
    let (directory, name) = secure_cache_path(path)?;
    directory
        .validate_private_file_if_present(&name)
        .map_err(io::Error::other)?;
    let Some(bytes) = directory.read_optional(&name).map_err(io::Error::other)? else {
        return Ok(None);
    };
    if bytes.len() > MAX_HTTP_BODY {
        return Err(invalid("provider cache exceeded limit"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn write_json_private<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let (directory, name) = secure_cache_path(path)?;
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    if bytes.len() >= MAX_HTTP_BODY {
        return Err(invalid("provider cache exceeded limit"));
    }
    bytes.push(b'\n');
    directory
        .atomic_replace(&name, &bytes, || Ok(()), || Ok(()), || Ok(()))
        .map_err(io::Error::other)
}

fn secure_cache_path(path: &Path) -> io::Result<(SecureDir, std::ffi::OsString)> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache path has no parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cache path has no filename"))?
        .to_owned();
    let directory = SecureDir::open_writable(parent, true).map_err(io::Error::other)?;
    Ok((directory, name))
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
