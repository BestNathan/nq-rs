use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub mod authentication;
pub mod market_data;
pub mod session_management;
pub mod subscribe;
pub mod support;

pub trait Request: Serialize {
    const METHOD: &'static str;
    const HAS_PAYLOAD: bool = true;
    type Response: Serialize + for<'a> Deserialize<'a> + Debug;

    fn no_payload(&self) -> bool {
        !Self::HAS_PAYLOAD
    }
}

#[macro_export]
macro_rules! impl_request {
    ($struct_name:ident, $response_type:ident, $method:literal) => {
        impl $crate::request::Request for $struct_name {
            const METHOD: &'static str = $method;
            type Response = $response_type;
        }
    };
    ($struct_name:ident, $response_type:ident, $method:literal, $has_payload:literal) => {
        impl $crate::request::Request for $struct_name {
            const METHOD: &'static str = $method;
            const HAS_PAYLOAD: bool = $has_payload;
            type Response = $response_type;
        }
    };
}
