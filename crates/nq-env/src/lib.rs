use std::env;

pub mod deribit;
pub mod emqx;

pub fn proxy() -> String {
    env::var("ALL_PROXY").unwrap_or("".to_string())
}
