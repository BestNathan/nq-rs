mod config;

use config::CustomConfig;
use fluvio::{RecordKey, TopicProducerPool};
use fluvio_connector_common::{connector, tracing::debug, Result};
use futures_util::TryStreamExt;
use sqlx::postgres;

use serde_json::Value;
use sqlx::Row;

#[connector(source)]
async fn start(config: CustomConfig, producer: TopicProducerPool) -> Result<()> {
    println!(
        "Starting sql-source source connector with custome config: {:?}, topic: {:?}",
        config,
        producer.topic(),
    );

    let pool = postgres::PgPoolOptions::new().connect(&config.url).await?;

    let sql = format!("select row_to_json(r) from ({}) r", &config.query);

    let mut rows = sqlx::query(&sql).fetch(&pool);

    while let Some(row) = rows.try_next().await? {
        let value: Value = row.try_get("row_to_json").unwrap();
        debug!("json: {}", value);

        producer.send(RecordKey::NULL, value.to_string()).await?;
    }

    producer.flush().await?;
    Ok(())
}
