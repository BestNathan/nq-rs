use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

use nq_deribit::model::currency::Currency;
use nq_deribit::model::instrument::InstrumentInfo;

/// JSON-RPC response wrapper returned by Deribit REST API.
#[derive(Deserialize, Debug)]
struct JsonRpcResponse<T> {
    result: T,
}

/// Max seconds until expiration to include an option.
const MAX_EXPIRY_SECS: u64 = 30 * 24 * 3600; // 30 days

pub struct InstrumentFetcher {
    http_client: Client,
    rest_base_url: String,
}

impl InstrumentFetcher {
    pub fn new(http_client: Client, rest_base_url: String) -> Self {
        Self { http_client, rest_base_url }
    }

    pub async fn fetch_all_options(&self, currencies: &[Currency]) -> Result<Vec<InstrumentInfo>> {
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let cutoff_ms = (now_secs + MAX_EXPIRY_SECS) * 1000;

        let mut all_options = Vec::new();

        for currency in currencies {
            match self.fetch_options(*currency).await {
                Ok(options) => {
                    let before = options.len();
                    let filtered: Vec<InstrumentInfo> = options
                        .into_iter()
                        .filter(|o| o.expiration_timestamp <= cutoff_ms)
                        .collect();
                    info!(currency = ?currency, before, after = filtered.len(),
                        cutoff_days = 30, "fetched and filtered options");
                    all_options.extend(filtered);
                }
                Err(e) => {
                    warn!(currency = ?currency, error = ?e, "failed to fetch options, will retry on next poll");
                }
            }
        }

        Ok(all_options)
    }

    async fn fetch_options(&self, currency: Currency) -> Result<Vec<InstrumentInfo>> {
        let url = format!(
            "{}/public/get_instruments?currency={}&kind=option&expired=false",
            self.rest_base_url, currency
        );

        let response: JsonRpcResponse<Vec<InstrumentInfo>> = self
            .http_client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("get_instruments HTTP request for {:?}", currency))?
            .json()
            .await
            .with_context(|| format!("get_instruments JSON parse for {:?}", currency))?;

        Ok(response.result)
    }
}
