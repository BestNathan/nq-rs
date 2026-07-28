#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use flume::Receiver;
use nq_app::runner::Runner;
use rand::Rng;
use rand::distr::Alphanumeric;
use reqwest::Client as HttpClient;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use nq_observability::metrics::KeyValue;

use crate::bridge_handle::{BatchToSend, BridgeHandle};
use crate::config::BridgeConfig;
use crate::metrics::BRIDGE_METRICS;

/// Generates a random alphanumeric string for unique MQTT client IDs.
fn random_string(length: usize) -> String {
    let mut rng = rand::rng();
    (0..length).map(|_| rng.sample(Alphanumeric) as char).collect()
}

/// Commands from the API server.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Command {
    Add(BridgeConfig, tokio::sync::oneshot::Sender<Result<BridgeConfig>>),
    Remove(String, tokio::sync::oneshot::Sender<Result<()>>),
    Update(String, BridgeConfig, tokio::sync::oneshot::Sender<Result<BridgeConfig>>),
    List(tokio::sync::oneshot::Sender<Vec<BridgeConfig>>),
    Get(String, tokio::sync::oneshot::Sender<Option<BridgeConfig>>),
}

/// Mutable state shared between the eventloop and command processing.
#[allow(dead_code)]
struct Inner {
    handles: HashMap<String, BridgeHandle>,
    /// topic -> list of handle IDs that subscribe to it
    topic_index: HashMap<String, Vec<String>>,
}

pub struct BridgeRunner {
    inner: Mutex<Inner>,
    mqtt_options: MqttOptions,
    command_rx: Receiver<Command>,
    http_client: HttpClient,
}

impl BridgeRunner {
    pub fn new(
        initial_configs: Vec<BridgeConfig>,
        command_rx: Receiver<Command>,
        http_client: HttpClient,
    ) -> Result<Self> {
        let mqtt_options = MqttOptions::new(
            format!("nq-rs/mqtt-http-bridge/{}", random_string(10)),
            nq_env::emqx::host(),
            1883,
        );

        let mut inner = Inner { handles: HashMap::new(), topic_index: HashMap::new() };

        for config in initial_configs {
            if let Err(errs) = config.validate() {
                warn!(config_id = config.id, errors = ?errs, "skipping invalid initial config");
                continue;
            }
            Self::add_to_inner(&mut inner, config, &http_client);
        }

        Ok(Self { inner: Mutex::new(inner), mqtt_options, command_rx, http_client })
    }

    /// Add a config to Inner (does NOT subscribe to MQTT -- caller handles that).
    fn add_to_inner(inner: &mut Inner, config: BridgeConfig, http_client: &HttpClient) {
        let topics = config.mqtt_topics.clone();
        let id = config.id.clone();

        match BridgeHandle::new(config, http_client.clone()) {
            Ok(handle) => {
                for topic in &topics {
                    inner.topic_index.entry(topic.clone()).or_default().push(id.clone());
                }
                inner.handles.insert(id.clone(), handle);
                info!(config_id = id, topics = ?topics, "bridge handle added");
            }
            Err(e) => {
                warn!(config_id = id, error = ?e, "failed to create handle");
            }
        }
    }

    /// Remove a handle from Inner. Returns (topics_to_unsubscribe, drained_batch_to_dispatch).
    fn remove_from_inner(
        inner: &mut Inner,
        id: &str,
    ) -> Result<(Vec<String>, Option<BatchToSend>)> {
        let mut handle =
            inner.handles.remove(id).ok_or_else(|| anyhow::anyhow!("config '{id}' not found"))?;

        let topics = handle.topics().to_vec();

        // Drain remaining messages before removing (caller spawns dispatch)
        let drain = handle.drain();

        // Clean topic index
        for entries in inner.topic_index.values_mut() {
            entries.retain(|hid| hid != id);
        }
        inner.topic_index.retain(|_, v| !v.is_empty());

        info!(config_id = id, "bridge handle removed");
        Ok((topics, drain))
    }

    /// Extract template variables from an incoming MQTT publish.
    fn extract_vars(topic: &str, payload: &str, payload_parse: bool) -> HashMap<String, String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or_else(|_| "0".to_string(), |d| d.as_millis().to_string());

        let mut vars = HashMap::new();
        vars.insert("topic".to_string(), topic.to_string());
        vars.insert("payload".to_string(), payload.to_string());
        vars.insert("clientid".to_string(), String::new());
        vars.insert("timestamp".to_string(), timestamp);

        if payload_parse {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(payload) {
                flatten_json(&mut vars, "payload", &val);
            } else {
                warn!("payload_parse=true but payload is not valid JSON");
            }
        }

        vars
    }
}

/// Recursively flatten a JSON value into dot-separated keys.
fn flatten_json(vars: &mut HashMap<String, String>, prefix: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(obj) => {
            for (k, v) in obj {
                let key = format!("{prefix}.{k}");
                flatten_json(vars, &key, v);
            }
        }
        serde_json::Value::String(s) => {
            vars.insert(prefix.to_string(), s.clone());
        }
        other => {
            vars.insert(prefix.to_string(), other.to_string());
        }
    }
}

#[async_trait]
impl Runner for BridgeRunner {
    fn name(&self) -> &'static str {
        "mqtt-http-bridge-runner"
    }

    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        // Create eventloop inside run() because EventLoop is !Send
        let (mqtt_client, mut eventloop) = AsyncClient::new(self.mqtt_options.clone(), 1000);

        // Flush timer: check all handles periodically
        let mut flush_tick = tokio::time::interval(std::time::Duration::from_millis(200));

        // Subscribe to all initial topics
        {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            for topic in inner.topic_index.keys() {
                let client = mqtt_client.clone();
                let t = topic.clone();
                tokio::spawn(async move {
                    if let Err(e) = client.subscribe(&t, QoS::AtMostOnce).await {
                        warn!(topic = t, error = ?e, "subscribe failed");
                    }
                });
            }
        }

        info!("bridge runner running with {} handles", {
            self.inner.lock().unwrap_or_else(|e| e.into_inner()).handles.len()
        });

        loop {
            select! {
                _ = canceltoken.cancelled() => break,

                notification = eventloop.poll() => {
                    match notification {
                        Ok(Event::Incoming(Incoming::Publish(publish))) => {
                            let topic_value = publish.topic.clone();
                            let payload = String::from_utf8_lossy(&publish.payload).to_string();
                            BRIDGE_METRICS.messages_received.add(1, &[]);

                            // Lock, push to handles, collect ready batches
                            let ready: Vec<(String, BatchToSend)> = {
                                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                let mut batches = Vec::new();
                                let handle_ids: Vec<String> = inner
                                    .topic_index
                                    .get(&topic_value)
                                    .cloned()
                                    .unwrap_or_default();

                                for hid in handle_ids {
                                    if let Some(handle) = inner.handles.get_mut(&hid) {
                                        let payload_parse = handle.config().payload_parse;
                                        let vars = Self::extract_vars(&topic_value, &payload, payload_parse);

                                        if handle.config().is_batch_mode() {
                                            if let Some(batch) = handle.push(vars) {
                                                batches.push((hid, batch));
                                            }
                                        } else {
                                            batches.push((hid, handle.render_single(&vars)));
                                        }
                                    }
                                }
                                batches
                            };

                            // Dispatch outside lock
                            for (hid, batch) in ready {
                                let timeout_ms = {
                                    let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                    inner.handles.get(&hid)
                                        .map(|h| h.config().batch.request_timeout_ms)
                                        .unwrap_or(10_000)
                                };
                                let client = self.http_client.clone();
                                tokio::spawn(async move {
                                    dispatch_batch(&client, &hid, timeout_ms, &batch).await;
                                });
                            }
                        }
                        Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                            info!("mqtt connected to broker");
                        }
                        Err(e) => {
                            warn!(error = ?e, "mqtt eventloop error");
                        }
                        _ => {}
                    }
                },

                _ = flush_tick.tick() => {
                    let ready: Vec<(String, BatchToSend)> = {
                        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                        let mut batches = Vec::new();
                        for (hid, handle) in inner.handles.iter_mut() {
                            if handle.should_flush_by_timer()
                                && let Some(batch) = handle.drain()
                            {
                                batches.push((hid.clone(), batch));
                            }
                        }
                        batches
                    };

                    for (hid, batch) in ready {
                        let timeout_ms = {
                            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                            inner.handles.get(&hid)
                                .map(|h| h.config().batch.request_timeout_ms)
                                .unwrap_or(10_000)
                        };
                        let client = self.http_client.clone();
                        tokio::spawn(async move {
                            dispatch_batch(&client, &hid, timeout_ms, &batch).await;
                        });
                    }
                },

                cmd = self.command_rx.recv_async() => {
                    match cmd {
                        Ok(Command::Add(config, reply)) => {
                            let result = {
                                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                if inner.handles.contains_key(&config.id) {
                                    Err(anyhow::anyhow!("config '{}' already exists", config.id))
                                } else {
                                    // Subscribe to topics
                                    for topic in &config.mqtt_topics {
                                        let client = mqtt_client.clone();
                                        let t = topic.clone();
                                        tokio::spawn(async move {
                                            let _ = client.subscribe(&t, QoS::AtMostOnce).await;
                                        });
                                    }
                                    Self::add_to_inner(&mut inner, config.clone(), &self.http_client);
                                    Ok(config)
                                }
                            };
                            let _ = reply.send(result);
                        }
                        Ok(Command::Remove(id, reply)) => {
                            let result = {
                                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                match Self::remove_from_inner(&mut inner, &id) {
                                    Ok((topics, drain)) => {
                                        for topic in topics {
                                            let client = mqtt_client.clone();
                                            tokio::spawn(async move {
                                                let _ = client.unsubscribe(&topic).await;
                                            });
                                        }
                                        // Dispatch drained batch outside lock
                                        if let Some(batch) = drain {
                                            let client = self.http_client.clone();
                                            let cid = id.clone();
                                            tokio::spawn(async move {
                                                dispatch_batch(&client, &cid, 10_000, &batch).await;
                                            });
                                        }
                                        Ok(())
                                    }
                                    Err(e) => Err(e),
                                }
                            };
                            let _ = reply.send(result);
                        }
                        Ok(Command::Update(id_val, config, reply)) => {
                            let result = {
                                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                match Self::remove_from_inner(&mut inner, &id_val) {
                                    Ok((topics, drain)) => {
                                        for topic in topics {
                                            let client = mqtt_client.clone();
                                            tokio::spawn(async move {
                                                let _ = client.unsubscribe(&topic).await;
                                            });
                                        }
                                        if let Some(batch) = drain {
                                            let client = self.http_client.clone();
                                            let cid = id_val.clone();
                                            tokio::spawn(async move {
                                                dispatch_batch(&client, &cid, 10_000, &batch).await;
                                            });
                                        }
                                        for topic in &config.mqtt_topics {
                                            let client = mqtt_client.clone();
                                            let t = topic.clone();
                                            tokio::spawn(async move {
                                                let _ = client.subscribe(&t, QoS::AtMostOnce).await;
                                            });
                                        }
                                        Self::add_to_inner(&mut inner, config.clone(), &self.http_client);
                                        Ok(config)
                                    }
                                    Err(e) => Err(e),
                                }
                            };
                            let _ = reply.send(result);
                        }
                        Ok(Command::List(reply)) => {
                            let configs: Vec<BridgeConfig> = {
                                let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                inner.handles.values().map(|h| h.config().clone()).collect()
                            };
                            let _ = reply.send(configs);
                        }
                        Ok(Command::Get(id_val, reply)) => {
                            let config = {
                                let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                                inner.handles.get(&id_val).map(|h| h.config().clone())
                            };
                            let _ = reply.send(config);
                        }
                        Err(_) => {
                            debug!("command channel closed");
                            break;
                        }
                    }
                },
            }
        }

        // Graceful shutdown: drain all handles
        info!("bridge runner draining handles for shutdown");
        let final_batches: Vec<(String, BatchToSend)> = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let mut batches = Vec::new();
            for (hid, handle) in inner.handles.iter_mut() {
                if let Some(batch) = handle.drain() {
                    batches.push((hid.clone(), batch));
                }
            }
            batches
        };

        for (hid, batch) in final_batches {
            let client = self.http_client.clone();
            tokio::spawn(async move {
                dispatch_batch(&client, &hid, 10_000, &batch).await;
            });
        }

        info!("bridge runner done");
        Ok(())
    }
}

/// Dispatch a batch via HTTP. Standalone function (no lock held).
async fn dispatch_batch(
    http_client: &HttpClient,
    config_id: &str,
    request_timeout_ms: u64,
    batch: &BatchToSend,
) {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(request_timeout_ms);

    let req_builder = match batch.method.as_str() {
        "POST" => http_client.post(&batch.url),
        "PUT" => http_client.put(&batch.url),
        "PATCH" => http_client.patch(&batch.url),
        _ => {
            warn!(config_id = config_id, method = batch.method, "unsupported method, skipping");
            return;
        }
    };

    let mut req = req_builder;
    for (k, v) in &batch.headers {
        req = req.header(k.as_str(), v.as_str());
    }

    match tokio::time::timeout(timeout, req.body(batch.body.clone()).send()).await {
        Ok(Ok(resp)) => {
            let status = resp.status();
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            if status.is_success() {
                BRIDGE_METRICS.messages_forwarded.add(batch.message_count as u64, &[]);
                BRIDGE_METRICS.batches_sent.add(1, &[]);
                BRIDGE_METRICS.batch_size.record(batch.message_count as f64, &[]);
                BRIDGE_METRICS.latency_ms.record(elapsed, &[]);
                debug!(config_id = config_id, count = batch.message_count, "batch sent");
            } else {
                let status_class = match status.as_u16() / 100 {
                    4 => "4xx",
                    5 => "5xx",
                    _ => "other",
                };
                BRIDGE_METRICS.failures.add(1, &[KeyValue::new("status_class", status_class)]);
                let preview: String =
                    resp.text().await.unwrap_or_default().chars().take(200).collect();
                warn!(config_id = config_id, status = %status, body = preview, "HTTP error");
            }
        }
        Ok(Err(e)) => {
            BRIDGE_METRICS.failures.add(1, &[KeyValue::new("status_class", "network")]);
            warn!(config_id = config_id, error = ?e, "HTTP request failed");
        }
        Err(_) => {
            BRIDGE_METRICS.failures.add(1, &[KeyValue::new("status_class", "timeout")]);
            warn!(config_id = config_id, "request timeout");
        }
    }
}
