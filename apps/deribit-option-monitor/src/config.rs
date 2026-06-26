use std::env;

use nq_deribit::model::currency::Currency;
use nq_deribit::model::interval::Interval;

const DEFAULT_CURRENCIES: &str = "BTC,ETH";
const DEFAULT_TICKER_INTERVAL: &str = "agg2";
const DEFAULT_MQTT_TOPIC_PREFIX: &str = "t/deribit/option_ticker";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 300;
const DEFAULT_POOL_CAPACITY: usize = 200;

pub struct AppConfig {
    pub currencies: Vec<Currency>,
    pub ticker_interval: Interval,
    pub mqtt_topic_prefix: String,
    pub poll_interval_secs: u64,
    pub pool_capacity: usize,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let currencies_str = env::var("DERIBIT_OPTION_CURRENCIES")
            .unwrap_or(DEFAULT_CURRENCIES.to_string());
        let currencies: Vec<Currency> = currencies_str
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter_map(|s| Currency::try_from(s).ok())
            .collect();

        let interval_str = env::var("DERIBIT_OPTION_TICKER_INTERVAL")
            .unwrap_or(DEFAULT_TICKER_INTERVAL.to_string());
        let ticker_interval = Interval::from(interval_str);

        let mqtt_topic_prefix = env::var("DERIBIT_OPTION_MQTT_TOPIC_PREFIX")
            .unwrap_or(DEFAULT_MQTT_TOPIC_PREFIX.to_string());

        let poll_interval_secs = env::var("DERIBIT_OPTION_POLL_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS);

        let pool_capacity = env::var("DERIBIT_OPTION_POOL_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POOL_CAPACITY);

        Self {
            currencies,
            ticker_interval,
            mqtt_topic_prefix,
            poll_interval_secs,
            pool_capacity,
        }
    }
}
