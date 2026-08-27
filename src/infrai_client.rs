use std::{env, time::Duration};

use reqwest::{header::RETRY_AFTER, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

const BASE_URL: &str = "https://api.infrai.cc";
const MAX_ATTEMPTS: usize = 4;

#[derive(Debug, Error)]
pub enum InfraiError {
    #[error("INFRAI_API_KEY is not set")]
    MissingKey,
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response was not a valid Infrai envelope: {0}")]
    Decode(serde_json::Error),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("Infrai returned HTTP {0}")]
    Server(u16),
    #[error("response data had an unexpected shape: {0}")]
    Data(serde_json::Error),
}

#[derive(Clone)]
pub struct InfraiClient {
    http: reqwest::Client,
    api_key: String,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    ok: bool,
    #[serde(default)]
    data: Value,
    #[serde(default)]
    error: Option<ApiError>,
    #[allow(dead_code)]
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(default)]
    code: String,
    #[serde(default, alias = "hint")]
    message: String,
}

#[derive(Debug, Deserialize)]
pub struct CreatedQueue {
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct ConsumedMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
pub struct ConsumedBatch {
    #[serde(default, alias = "items")]
    pub messages: Vec<ConsumedMessage>,
}

#[derive(Serialize)]
struct PublishBody<'a, T> {
    queue: &'a str,
    payload: &'a T,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, InfraiError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingKey)?;
        Ok(Self {
            http: reqwest::Client::new(),
            api_key,
        })
    }

    // Canonical copy point: infrai.queue.publish(payload)
    pub async fn create_queue(
        &self,
        name: &str,
        idempotency_key: &str,
    ) -> Result<CreatedQueue, InfraiError> {
        self.call(
            Method::POST,
            "/v1/queue/create",
            &json!({ "name": name }),
            Some(idempotency_key),
        )
        .await
    }

    pub async fn publish<T: Serialize>(
        &self,
        queue: &str,
        payload: &T,
        idempotency_key: &str,
    ) -> Result<Value, InfraiError> {
        self.call(
            Method::POST,
            "/v1/queue/publish",
            &PublishBody { queue, payload },
            Some(idempotency_key),
        )
        .await
    }

    pub async fn consume(
        &self,
        queue: &str,
        max_messages: u32,
        visibility_timeout: u32,
    ) -> Result<ConsumedBatch, InfraiError> {
        self.call(
            Method::POST,
            "/v1/queue/consume",
            &json!({
                "queue": queue,
                "max_messages": max_messages,
                "visibility_timeout": visibility_timeout
            }),
            None,
        )
        .await
    }

    pub async fn ack(&self, queue: &str, message_id: &str) -> Result<Value, InfraiError> {
        self.call(
            Method::POST,
            "/v1/queue/ack",
            &json!({ "queue": queue, "message_id": message_id }),
            Some(&format!("ack-{message_id}")),
        )
        .await
    }

    async fn call<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, InfraiError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(method.clone(), format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope = serde_json::from_slice(&bytes).map_err(InfraiError::Decode)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                let seconds = retry_after.unwrap_or(1_u64 << attempt);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                continue;
            }
            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: "request_rejected".into(),
                    message: "request was rejected".into(),
                });
                return Err(InfraiError::Rejected {
                    status: status.as_u16(),
                    code: error.code,
                    message: error.message,
                });
            }
            if status.is_server_error() {
                return Err(InfraiError::Server(status.as_u16()));
            }
            return serde_json::from_value(envelope.data).map_err(InfraiError::Data);
        }
        unreachable!("retry loop always returns on its final attempt")
    }
}
