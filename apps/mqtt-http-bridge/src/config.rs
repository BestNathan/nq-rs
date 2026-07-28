use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BridgeConfig {
    pub id: String,
    pub mqtt_topics: Vec<String>,
    pub http: HttpConfig,
    pub template: String,
    pub batch: BatchConfig,
    #[serde(default)]
    pub payload_parse: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HttpConfig {
    pub url: String,
    #[serde(default = "default_http_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_http_method() -> String {
    "POST".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchConfig {
    pub size: u32,
    pub interval_ms: u64,
    #[serde(default = "default_max_inflight")]
    pub max_inflight: u32,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_max_inflight() -> u32 {
    4
}

fn default_request_timeout_ms() -> u64 {
    10_000
}

#[derive(Debug, Deserialize)]
pub struct BridgesFile {
    pub bridges: Vec<BridgeConfig>,
}

#[allow(dead_code)]
impl BridgeConfig {
    pub fn is_batch_mode(&self) -> bool {
        self.batch.size > 1
    }

    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors: Vec<String> = Vec::new();
        if self.id.is_empty() {
            errors.push("id must not be empty".into());
        }
        if self.mqtt_topics.is_empty() {
            errors.push("mqtt_topics must not be empty".into());
        }
        if self.http.url.is_empty() {
            errors.push("http.url must not be empty".into());
        }
        if self.template.is_empty() {
            errors.push("template must not be empty".into());
        }
        if self.batch.size == 0 {
            errors.push("batch.size must be >= 1".into());
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

pub fn load_from_file(path: &str) -> Result<Vec<BridgeConfig>, anyhow::Error> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read config file {path}: {e}"))?;
    let file: BridgesFile = serde_yaml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("failed to parse config file {path}: {e}"))?;
    Ok(file.bridges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_minimal_config() {
        let yaml = r#"
bridges:
  - id: "test"
    mqtt_topics: ["test/topic"]
    http:
      url: "https://example.com/webhook"
    template: "{\"topic\": \"${topic}\"}"
    batch:
      size: 1
      interval_ms: 1000
"#;
        let file: BridgesFile = serde_yaml::from_str(yaml).unwrap();
        let c = &file.bridges[0];
        assert_eq!(c.id, "test");
        assert_eq!(c.http.method, "POST");
        assert_eq!(c.batch.max_inflight, 4);
        assert!(!c.is_batch_mode());
        assert!(!c.payload_parse);
    }

    #[test]
    fn deserialize_full_batch_config() {
        let yaml = r#"
bridges:
  - id: "batch"
    mqtt_topics: ["devices/+/events"]
    http:
      url: "https://api.example.com/ingest"
      method: PUT
      headers:
        Authorization: "Bearer token"
    template: "[${foreach ,}{\"t\":\"${topic}\"}${end}]"
    batch:
      size: 100
      interval_ms: 500
      max_inflight: 8
      request_timeout_ms: 15000
    payload_parse: true
"#;
        let file: BridgesFile = serde_yaml::from_str(yaml).unwrap();
        let c = &file.bridges[0];
        assert!(c.is_batch_mode());
        assert_eq!(c.batch.max_inflight, 8);
        assert_eq!(c.batch.request_timeout_ms, 15_000);
        assert!(c.payload_parse);
        assert_eq!(c.http.headers.get("Authorization").map(String::as_str), Some("Bearer token"));
    }

    #[test]
    fn validate_rejects_empty_id() {
        let config = BridgeConfig {
            id: String::new(),
            mqtt_topics: vec!["test".into()],
            http: HttpConfig {
                url: "https://x.com".into(),
                method: "POST".into(),
                headers: HashMap::new(),
            },
            template: "x".into(),
            batch: BatchConfig {
                size: 1,
                interval_ms: 1000,
                max_inflight: 4,
                request_timeout_ms: 10000,
            },
            payload_parse: false,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_batch_size() {
        let config = BridgeConfig {
            id: "test".into(),
            mqtt_topics: vec!["test".into()],
            http: HttpConfig {
                url: "https://x.com".into(),
                method: "POST".into(),
                headers: HashMap::new(),
            },
            template: "x".into(),
            batch: BatchConfig {
                size: 0,
                interval_ms: 1000,
                max_inflight: 4,
                request_timeout_ms: 10000,
            },
            payload_parse: false,
        };
        let err = config.validate().unwrap_err();
        assert!(err.iter().any(|e| e.contains("batch.size")));
    }
}
