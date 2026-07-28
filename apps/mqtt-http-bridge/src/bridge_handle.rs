use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::config::BridgeConfig;
use crate::template::Template;
use anyhow::Result;
use reqwest::Client;

/// A self-contained batch ready for HTTP dispatch.
/// All data is owned — no references to handle state.
#[derive(Debug)]
pub struct BatchToSend {
    #[allow(dead_code)]
    pub url: String,
    #[allow(dead_code)]
    pub method: String,
    #[allow(dead_code)]
    pub headers: HashMap<String, String>,
    pub body: String,
    #[allow(dead_code)]
    pub message_count: usize,
}

pub struct BridgeHandle {
    config: BridgeConfig,
    #[allow(dead_code)]
    template: Template,
    #[allow(dead_code)]
    buffer: VecDeque<HashMap<String, String>>,
    #[allow(dead_code)]
    first_item_at: Option<Instant>,
    #[allow(dead_code)]
    http_client: Client,
}

impl BridgeHandle {
    #[allow(dead_code)]
    pub fn new(config: BridgeConfig, http_client: Client) -> Result<Self> {
        let is_batch = config.is_batch_mode();
        let template = Template::parse(&config.template, is_batch)
            .map_err(|e| anyhow::anyhow!("template error for '{}': {e}", config.id))?;
        Ok(Self { config, template, buffer: VecDeque::new(), first_item_at: None, http_client })
    }

    /// Read-only access to config (used by runner for topic lookups, etc.).
    #[allow(dead_code)]
    pub fn config(&self) -> &BridgeConfig {
        &self.config
    }

    #[allow(dead_code)]
    pub fn topics(&self) -> &[String] {
        self.config.mqtt_topics.as_slice()
    }

    /// Push a variable map into the buffer. Returns Some(batch) when buffer
    /// reaches batch.size, None if still accumulating.
    #[allow(dead_code)]
    pub fn push(&mut self, vars: HashMap<String, String>) -> Option<BatchToSend> {
        if self.buffer.is_empty() {
            self.first_item_at = Some(Instant::now());
        }
        self.buffer.push_back(vars);

        if self.buffer.len() >= self.config.batch.size as usize { self.drain() } else { None }
    }

    /// True if the first buffered item has waited longer than interval_ms.
    #[allow(dead_code)]
    pub fn should_flush_by_timer(&self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        self.first_item_at.is_some_and(|first| {
            Instant::now().duration_since(first)
                >= Duration::from_millis(self.config.batch.interval_ms)
        })
    }

    /// Drain all buffered messages into a BatchToSend via the batch template.
    #[allow(dead_code)]
    pub fn drain(&mut self) -> Option<BatchToSend> {
        if self.buffer.is_empty() {
            return None;
        }
        let messages: Vec<HashMap<String, String>> = self.buffer.drain(..).collect();
        self.first_item_at = None;
        let body = self.template.render_batch(&messages);
        Some(BatchToSend {
            url: self.config.http.url.clone(),
            method: self.config.http.method.clone(),
            headers: self.config.http.headers.clone(),
            body,
            message_count: messages.len(),
        })
    }

    /// For single mode: render immediately (no buffer).
    #[allow(dead_code)]
    pub fn render_single(&self, vars: &HashMap<String, String>) -> BatchToSend {
        BatchToSend {
            url: self.config.http.url.clone(),
            method: self.config.http.method.clone(),
            headers: self.config.http.headers.clone(),
            body: self.template.render_single(vars),
            message_count: 1,
        }
    }

    /// Dispatch a batch via HTTP.
    #[allow(dead_code)]
    pub async fn dispatch(&self, batch: &BatchToSend) -> Result<()> {
        let timeout = Duration::from_millis(self.config.batch.request_timeout_ms);

        let mut req = match self.config.http.method.as_str() {
            "POST" => self.http_client.post(&batch.url),
            "PUT" => self.http_client.put(&batch.url),
            "PATCH" => self.http_client.patch(&batch.url),
            other => return Err(anyhow::anyhow!("unsupported method: {other}")),
        };

        for (k, v) in &batch.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let response = tokio::time::timeout(timeout, req.body(batch.body.clone()).send())
            .await
            .map_err(|_| anyhow::anyhow!("request timeout"))?
            .map_err(|e| anyhow::anyhow!("HTTP error: {e}"))?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let preview: String =
                response.text().await.unwrap_or_default().chars().take(200).collect();
            Err(anyhow::anyhow!("HTTP {status}: {preview}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(batch_size: u32) -> BridgeConfig {
        use crate::config::{BatchConfig, HttpConfig};
        BridgeConfig {
            id: "test-handle".into(),
            mqtt_topics: vec!["t/test".into()],
            http: HttpConfig {
                url: "https://example.com/webhook".into(),
                method: "POST".into(),
                headers: HashMap::new(),
            },
            template: if batch_size > 1 {
                "[${foreach ,}{\"t\":\"${topic}\"}${end}]".into()
            } else {
                "{\"t\":\"${topic}\"}".into()
            },
            batch: BatchConfig {
                size: batch_size,
                interval_ms: 1000,
                max_inflight: 4,
                request_timeout_ms: 10_000,
            },
            payload_parse: false,
        }
    }

    fn vars(topic: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("topic".into(), topic.into());
        m.insert("payload".into(), "p".into());
        m.insert("clientid".into(), "c1".into());
        m.insert("timestamp".into(), "1".into());
        m
    }

    #[test]
    fn single_mode_renders_immediately() {
        let h = BridgeHandle::new(make_config(1), reqwest::Client::new()).unwrap();
        let batch = h.render_single(&vars("t/1"));
        assert_eq!(batch.body, "{\"t\":\"t/1\"}");
        assert_eq!(batch.message_count, 1);
    }

    #[test]
    fn batch_buffers_until_size() {
        let mut h = BridgeHandle::new(make_config(3), reqwest::Client::new()).unwrap();
        assert!(h.push(vars("a")).is_none());
        assert!(h.push(vars("b")).is_none());
        let batch = h.push(vars("c")).unwrap();
        assert_eq!(batch.message_count, 3);
        assert_eq!(batch.body, "[{\"t\":\"a\"},{\"t\":\"b\"},{\"t\":\"c\"}]");
    }

    #[test]
    fn drain_empty_returns_none() {
        let mut h = BridgeHandle::new(make_config(3), reqwest::Client::new()).unwrap();
        assert!(h.drain().is_none());
    }

    #[test]
    fn timer_false_on_empty() {
        let h = BridgeHandle::new(make_config(3), reqwest::Client::new()).unwrap();
        assert!(!h.should_flush_by_timer());
    }

    #[test]
    fn invalid_template_fails_construction() {
        let mut cfg = make_config(1);
        cfg.template = "${foreach ,}broken".into();
        assert!(BridgeHandle::new(cfg, reqwest::Client::new()).is_err());
    }
}
