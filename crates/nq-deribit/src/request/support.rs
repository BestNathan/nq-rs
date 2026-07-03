use crate::impl_request;
use serde::{Deserialize, Serialize};

impl_request!(HelloRequest, HelloResponse, "public/hello");
impl_request!(TestRequest, TestResponse, "public/test");

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct HelloRequest {
    pub client_name: String,
    pub client_version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct HelloResponse {
    pub version: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct TestRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_result: Option<String>,
}

impl TestRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expect(result: &str) -> Self {
        Self { expected_result: Some(result.into()) }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TestResponse {
    pub version: String,
}
