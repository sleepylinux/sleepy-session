//! Typed pw-dump events, excluding observation clients such as wpctl itself.
use serde_json::Value;
use std::{collections::BTreeMap, io};

const MAX_FRAME: usize = 1024 * 1024;
const MAX_OBJECTS: usize = 4096;

#[derive(Default)]
pub(super) struct AudioMonitor {
    frame: Vec<u8>,
    depth: usize,
    quoted: bool,
    escaped: bool,
    // Only relevant objects are retained; client churn cannot grow this map.
    objects: BTreeMap<u32, bool>, // true = metadata, false = node/device
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl AudioMonitor {
    pub(super) fn feed(&mut self, bytes: &[u8]) -> io::Result<bool> {
        let mut changed = false;
        for &byte in bytes {
            if self.frame.is_empty() {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'[' {
                    return Err(invalid("pw-dump expected a JSON array"));
                }
            }
            if self.frame.len() == MAX_FRAME {
                return Err(invalid("pw-dump frame exceeds 1 MiB"));
            }
            self.frame.push(byte);
            if self.quoted {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.quoted = false;
                }
                continue;
            }
            match byte {
                b'"' => self.quoted = true,
                b'[' | b'{' => {
                    self.depth += 1;
                    if self.depth > 64 {
                        return Err(invalid("pw-dump nesting exceeds bound"));
                    }
                }
                b']' | b'}' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| invalid("pw-dump unmatched delimiter"))?;
                    if self.depth == 0 {
                        let value: Value = serde_json::from_slice(&self.frame)
                            .map_err(|_| invalid("pw-dump invalid JSON"))?;
                        self.frame.clear();
                        changed |= self.batch(value)?;
                    }
                }
                _ => {}
            }
        }
        Ok(changed)
    }

    fn batch(&mut self, value: Value) -> io::Result<bool> {
        let mut changed = false;
        for event in value
            .as_array()
            .ok_or_else(|| invalid("pw-dump expected array"))?
        {
            let id = event
                .get("id")
                .and_then(Value::as_u64)
                .and_then(|id| u32::try_from(id).ok())
                .ok_or_else(|| invalid("pw-dump missing object ID"))?;
            if event.get("info") == Some(&Value::Null) {
                changed |= self.objects.remove(&id).is_some();
                continue;
            }
            let kind = match event.get("type").and_then(Value::as_str) {
                Some("PipeWire:Interface:Node" | "PipeWire:Interface:Device") => Some(false),
                Some("PipeWire:Interface:Metadata") => Some(true),
                Some(_) => {
                    self.objects.remove(&id);
                    None
                }
                None => self.objects.get(&id).copied(),
            };
            if let Some(metadata) = kind {
                self.objects.insert(id, metadata);
                if self.objects.len() > MAX_OBJECTS {
                    return Err(invalid("pw-dump object count exceeds bound"));
                }
                if metadata {
                    changed |=
                        event
                            .get("metadata")
                            .and_then(Value::as_array)
                            .is_some_and(|entries| {
                                entries.iter().any(|entry| {
                                    entry.get("key") == Some(&Value::Null)
                                        || matches!(
                                            entry.get("key").and_then(Value::as_str),
                                            Some(
                                                "default.audio.sink"
                                                    | "default.audio.source"
                                                    | "default.configured.audio.sink"
                                                    | "default.configured.audio.source"
                                            )
                                        )
                                })
                            });
                } else {
                    changed = true;
                }
            }
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readonly_clients_do_not_trigger_audio_readback() {
        let mut monitor = AudioMonitor::default();
        for _ in 0..100 {
            assert!(!monitor.feed(br#"[{"id":71,"type":"PipeWire:Interface:Client","info":{"props":{"application.name":"wpctl"}}}]"#).unwrap());
            assert!(!monitor.feed(br#"[{"id":71,"info":null}]"#).unwrap());
        }
    }

    #[test]
    fn captured_vm_client_churn_is_ignored_after_initial_graph() {
        let batches: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/fixtures/system/pw-dump-client-churn-vm.json"
        ))
        .unwrap();
        let mut monitor = AudioMonitor::default();
        for (index, batch) in batches.iter().enumerate() {
            let bytes = serde_json::to_vec(batch).unwrap();
            let mut changed = false;
            for chunk in bytes.chunks(3) {
                changed |= monitor.feed(chunk).unwrap();
            }
            assert_eq!(changed, index == 0, "batch {index}");
        }
        assert!(monitor.frame.is_empty());
    }

    #[test]
    fn external_volume_mute_hotplug_and_defaults_still_trigger() {
        let mut monitor = AudioMonitor::default();
        for event in [
            r#"[{"id":42,"type":"PipeWire:Interface:Node","info":{"params":{"Props":[{"mute":false,"channelVolumes":[0.5]}]}}}]"#,
            r#"[{"id":42,"type":"PipeWire:Interface:Node","info":{"params":{"Props":[{"mute":true}]}}}]"#,
            r#"[{"id":43,"type":"PipeWire:Interface:Device","info":{}}]"#,
            r#"[{"id":43,"info":null}]"#,
            r#"[{"id":10,"type":"PipeWire:Interface:Metadata","metadata":[{"key":"default.audio.sink","value":{"name":"fixture"}}]}]"#,
            r#"[{"id":10,"type":"PipeWire:Interface:Metadata","metadata":[{"key":"default.audio.source","value":null}]}]"#,
            r#"[{"id":42,"info":null}]"#,
        ] {
            assert!(monitor.feed(event.as_bytes()).unwrap(), "{event}");
        }
        assert!(!monitor.feed(br#"[{"id":10,"type":"PipeWire:Interface:Metadata","metadata":[{"key":"clock.force-rate","value":48000}]}]"#).unwrap());
        assert!(!monitor.feed(br#"[{"id":999,"info":null}]"#).unwrap());
    }

    #[test]
    fn retained_object_set_is_bounded() {
        let mut monitor = AudioMonitor::default();
        let objects: Vec<Value> = (0..=MAX_OBJECTS)
            .map(|id| {
                serde_json::json!({
                    "id": id, "type": "PipeWire:Interface:Node", "info": {}
                })
            })
            .collect();
        let error = monitor
            .feed(&serde_json::to_vec(&objects).unwrap())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("object count"));
    }

    #[test]
    fn framing_handles_escaped_strings_and_rejects_unbounded_input() {
        let mut monitor = AudioMonitor::default();
        assert!(!monitor.feed(br#"[{"id":1,"type":"PipeWire:Interface:Client","info":{"name":"brackets ] [ } { and \"quote"}}]"#).unwrap());
        assert!(monitor.feed(b"{}").is_err());
        let mut monitor = AudioMonitor::default();
        assert!(monitor.feed(&vec![b'['; 65]).is_err());
        let mut monitor = AudioMonitor::default();
        assert!(!monitor.feed(b"[\"").unwrap());
        assert!(monitor.feed(&vec![b'x'; MAX_FRAME]).is_err());
    }
}
