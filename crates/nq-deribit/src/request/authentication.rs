use crate::impl_request;
use serde::{Deserialize, Serialize};

impl_request!(AuthRequest, AuthResponse, "public/auth");

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum GrantType {
    #[default]
    ClientCredentials,
    ClientSignature,
    RefreshToken,
}

impl GrantType {
    pub fn credential_auth_req(self, id: &str, secret: &str) -> Option<AuthRequest> {
        match self {
            GrantType::ClientCredentials => Some(AuthRequest::credential_auth(id, secret)),
            _ => None,
        }
    }

    pub fn refresh_token_auth_req(self, token: &str) -> Option<AuthRequest> {
        match self {
            GrantType::RefreshToken => Some(AuthRequest::refresh_token_auth(token)),
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct AuthRequest {
    pub grant_type: GrantType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl AuthRequest {
    pub fn credential_auth(id: &str, secret: &str) -> AuthRequest {
        AuthRequest {
            grant_type: GrantType::ClientCredentials,
            client_id: Some(id.into()),
            client_secret: Some(secret.into()),
            ..Default::default()
        }
    }

    pub fn refresh_token_auth(refresh_token: &str) -> AuthRequest {
        AuthRequest {
            grant_type: GrantType::RefreshToken,
            refresh_token: Some(refresh_token.into()),
            ..Default::default()
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AuthResponse {
    pub access_token: Option<String>,
    expires_in: i64,
    refresh_token: String,
    scope: String,
    state: Option<String>,
    token_type: String,
}
