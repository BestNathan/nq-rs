use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, warn};

use nq_deribit::connection::Connection;
use nq_deribit::model::currency::Currency;
use nq_deribit::model::instrument::InstrumentInfo;
use nq_deribit::request::market_data::{GetInstrumentsRequest, GetInstrumentsResponse};

pub struct InstrumentFetcher {
    connection: Arc<Connection>,
}

impl InstrumentFetcher {
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
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
        let req = GetInstrumentsRequest::options(currency);
        let resp: GetInstrumentsResponse = self.connection.call_api(req).await
            .with_context(|| format!("get_instruments for {:?}", currency))?;
        Ok(resp)
    }
}
