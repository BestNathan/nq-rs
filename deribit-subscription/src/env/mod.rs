use std::env;

use anyhow::Result;

pub fn proxy() -> Result<reqwest::Proxy> {
    let ap = env::var("ALL_PROXY")?;
    let p = reqwest::Proxy::all(ap)?;
    Ok(p)
}

pub fn deribit_ws_url() -> String {
    env::var("DERIBIT_WS_URL").unwrap_or("wss://www.deribit.com/ws/api/v2".to_string())
}

pub fn emqx_host() -> String {
    env::var("EMQX_HOST").unwrap_or("192.168.2.106".to_string())
}
