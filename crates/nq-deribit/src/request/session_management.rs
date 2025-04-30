use crate::impl_request;
use serde::{Deserialize, Serialize};

impl_request!(
    SetHeartbeatRequest,
    SetHeartbeatResponse,
    "public/set_heartbeat"
);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct SetHeartbeatRequest {
    pub interval: u64,
}

impl SetHeartbeatRequest {
    pub fn with_interval(interval: u64) -> SetHeartbeatRequest {
        SetHeartbeatRequest { interval }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SetHeartbeatResponse {
    Ok,
}
