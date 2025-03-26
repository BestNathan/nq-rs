use std::env;

pub fn ws_url() -> String {
    env::var("DERIBIT_WS_URL").unwrap_or("wss://www.deribit.com/ws/api/v2".to_string())
}
