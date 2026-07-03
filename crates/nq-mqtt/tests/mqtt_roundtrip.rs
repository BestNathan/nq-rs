//! Verify the MQTT publishing pipeline: connect to EMQX, subscribe to
//! option ticker topics, and confirm messages arrive with valid data.
//!
//! Run with:
//!   cargo test -p nq-mqtt --test mqtt_roundtrip -- --nocapture

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;

const EMQX_HOST: &str = "192.168.2.86";
const EMQX_PORT: u16 = 31883;
const TOPIC: &str = "t/deribit/option_ticker/#";

#[tokio::test]
async fn test_subscribe_to_option_tickers() -> anyhow::Result<()> {
    let client_id = format!("test-subscriber-{}", std::process::id());
    let mut options = MqttOptions::new(&client_id, EMQX_HOST, EMQX_PORT);
    options.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(options, 100);

    // Subscribe to all option tickers
    client.subscribe(TOPIC, QoS::AtLeastOnce).await?;
    println!("Subscribed to: {}", TOPIC);

    // Collect messages for up to 30 seconds
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut messages: Vec<(String, Value)> = Vec::new();
    let mut topic_set = std::collections::HashSet::new();

    while tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), eventloop.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                let topic = publish.topic.clone();
                let payload: Value = serde_json::from_slice(&publish.payload)?;
                let instrument =
                    payload.get("instrument_name").and_then(|v| v.as_str()).unwrap_or("unknown");

                topic_set.insert(topic);
                messages.push((instrument.to_string(), payload));

                if messages.len() >= 10 {
                    println!("Collected 10 messages, stopping.");
                    break;
                }
            }
            Ok(Ok(_other)) => {} // ConnAck, PingResp, etc.
            Ok(Err(e)) => {
                eprintln!("MQTT error: {:?}", e);
                break;
            }
            Err(_timeout) => {
                println!("Timed out waiting for messages...");
                break;
            }
        }
    }

    println!("\n=== Results ===");
    println!("Total messages received: {}", messages.len());
    println!("Unique topics: {}", topic_set.len());

    if messages.is_empty() {
        eprintln!("NO MESSAGES received! The publishing pipeline may be broken.");
        eprintln!("Check that option-monitor is running and publishing.");
    } else {
        println!("\nSample messages:");
        for (instrument, payload) in messages.iter().take(5) {
            let best_bid = payload.get("best_bid_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let best_ask = payload.get("best_ask_price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            println!("  {}: bid={:.4}, ask={:.4}", instrument, best_bid, best_ask);
        }
    }

    client.disconnect().await?;
    assert!(!messages.is_empty(), "Expected at least 1 ticker message");
    Ok(())
}
