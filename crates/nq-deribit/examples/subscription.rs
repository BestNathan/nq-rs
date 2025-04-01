use std::{env, sync::Arc};

use anyhow::{Error, Result};
use nq_app::application::Application;
use nq_deribit::client;
use reqwest::Proxy;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    unsafe {
        env::set_var("RUST_LOG", "trace");
    }
    let deribit = client::Client::builder()
        .set_proxy(Proxy::all("http://192.168.2.98:8890").unwrap())
        .build()?;

    let (writer, subsriber) = deribit.split();

    tokio::spawn(async move {
        writer
            .subscribe(vec!["trades.option.BTC.100ms".to_string()])
            .await?;

        while let Some(msg) = subsriber.recv().await {
            println!("{:?}", msg)
        }

        Ok::<(), Error>(())
    });

    let app = Application::new();

    app.add_runner(Arc::new(deribit));
    app.run(CancellationToken::new()).await;
    Ok(())
}
