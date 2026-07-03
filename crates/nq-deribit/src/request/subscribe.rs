use crate::impl_request;
use serde::{Deserialize, Serialize};

impl_request!(PublicSubscribeRequest, SubscribeResponse, "public/subscribe");
impl_request!(PrivateSubscribeRequest, SubscribeResponse, "private/subscribe");
impl_request!(PublicUnsubscribeRequest, UnsubscribeResponse, "public/unsubscribe");
impl_request!(PrivateUnsubscribeRequest, UnsubscribeResponse, "private/unsubscribe");

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PublicSubscribeRequest {
    pub channels: Vec<String>,
}

impl PublicSubscribeRequest {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PrivateSubscribeRequest {
    pub channels: Vec<String>,
}

impl PrivateSubscribeRequest {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SubscribeResponse(pub Vec<String>);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PublicUnsubscribeRequest {
    pub channels: Vec<String>,
}

impl PublicUnsubscribeRequest {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PrivateUnsubscribeRequest {
    pub channels: Vec<String>,
}

impl PrivateUnsubscribeRequest {
    pub fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnsubscribeResponse(pub Vec<String>);
