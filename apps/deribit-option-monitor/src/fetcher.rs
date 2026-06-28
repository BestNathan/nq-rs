use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, warn};

use nq_deribit::model::currency::Currency;
use nq_deribit::model::instrument::InstrumentInfo;

/// JSON-RPC response wrapper returned by Deribit REST API.
#[derive(Deserialize, Debug)]
struct JsonRpcResponse<T> {
    result: T,
}

pub struct InstrumentFetcher {
    http_client: Client,
    rest_base_url: String,
}

impl InstrumentFetcher {
    pub fn new(http_client: Client, rest_base_url: String) -> Self {
        Self {
            http_client,
            rest_base_url,
        }
    }

    pub async fn fetch_all_options(&self, currencies: &[Currency]) -> Result<Vec<InstrumentInfo>> {
        let mut all_options = Vec::new();

        for currency in currencies {
            match self.fetch_options(*currency).await {
                Ok(options) => {
                    debug!(currency = ?currency, count = options.len(), "fetched options");
                    all_options.extend(options);
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
