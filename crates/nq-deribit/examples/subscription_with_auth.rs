use anyhow::{Error, Result};
use nq_app::application::Application;
use nq_deribit::client;
use nq_deribit::client::ConfigBuilder;
use nq_deribit::model::currency::Currency;
use nq_deribit::model::instrument::InstrumentKind;
use nq_deribit::model::interval::Interval;
use nq_deribit::subscription::channel::Channel;
use nq_deribit::subscription::trades::{TradesByInstrumentChannel, TradesByKindChannel};
use reqwest::Proxy;
use std::env;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = ConfigBuilder::default()
        .proxy(Proxy::all(env::var("PROXY")?)?)
        .public_subscribe_channels(vec![
            // TradesByKindChannel(InstrumentKind::Option, Currency::BTC, Interval::Agg2).to_channel_str(),
            // TradesByKindChannel(InstrumentKind::Option, Currency::BTC, Interval::Ms100).to_channel_str(),
            // TradesByInstrumentChannel("BTC-PERPETUAL".to_string(), Interval::Ms100).to_channel_str(),
            TradesByInstrumentChannel("BTC-PERPETUAL".to_string(), Interval::Raw).to_channel_str(),
        ])
        .client_id(env::var("DERIBIT_API_CLIENT_ID").ok())
        .client_secret(env::var("DERIBIT_API_CLIENT_SECRET").ok())
        .build()?;
    let deribit = client::Client::builder().config(config).build()?;

    let subsriber = deribit.subscription_client();

    tokio::spawn(async move {
        while let Ok(msg) = subsriber.recv().await {
            info!("{:?}", msg)
        }

        Ok::<(), Error>(())
    });

    let app = Application::new();

    app.add_runner(Arc::new(deribit));
    app.run(CancellationToken::new()).await;
    Ok(())
}
