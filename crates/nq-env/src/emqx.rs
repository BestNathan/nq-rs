use std::env;

pub fn host() -> String {
    env::var("EMQX_HOST").unwrap_or("192.168.2.106".to_string())
}
