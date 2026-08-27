use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::infrai_client::{InfraiClient, InfraiError};

pub const MAX_ATTEMPTS: u8 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SaaSOperation {
    OnboardTenant,
    SuspendAccount,
    RestoreAccount,
    RebuildAdminIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SaaSJob {
    pub tenant_id: String,
    pub operation: SaaSOperation,
    pub attempt: u8,
    pub requested_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeadLetter {
    pub job: SaaSJob,
    pub reason: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FailureDecision {
    Retry(SaaSJob),
    DeadLetter(DeadLetter),
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Api(#[from] InfraiError),
    #[error("message {message_id} has invalid job data: {source}")]
    InvalidJob {
        message_id: String,
        source: serde_json::Error,
    },
}

pub fn decide_failure(mut job: SaaSJob, reason: impl Into<String>) -> FailureDecision {
    job.attempt += 1;
    if job.attempt >= MAX_ATTEMPTS {
        FailureDecision::DeadLetter(DeadLetter {
            job,
            reason: reason.into(),
        })
    } else {
        FailureDecision::Retry(job)
    }
}

pub async fn handle_one_failure(
    client: &InfraiClient,
    primary_queue: &str,
    dead_letter_queue: &str,
    message_id: &str,
    payload: Value,
    reason: &str,
) -> Result<FailureDecision, WorkerError> {
    let job: SaaSJob =
        serde_json::from_value(payload).map_err(|source| WorkerError::InvalidJob {
            message_id: message_id.to_owned(),
            source,
        })?;
    let decision = decide_failure(job, reason);

    match &decision {
        FailureDecision::Retry(next) => {
            client
                .publish(
                    primary_queue,
                    next,
                    &format!("retry-{message_id}-{}", next.attempt),
                )
                .await?;
        }
        FailureDecision::DeadLetter(dead) => {
            client
                .publish(
                    dead_letter_queue,
                    dead,
                    &format!("dead-letter-{message_id}"),
                )
                .await?;
        }
    }
    client.ack(primary_queue, message_id).await?;
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn third_failure_quarantines_tenant_onboarding_job() {
        let job = SaaSJob {
            tenant_id: "tenant-acme".into(),
            operation: SaaSOperation::OnboardTenant,
            attempt: 2,
            requested_by: "admin-17".into(),
        };

        let decision = decide_failure(job, "CRM account is not ready");

        assert_eq!(
            decision,
            FailureDecision::DeadLetter(DeadLetter {
                job: SaaSJob {
                    tenant_id: "tenant-acme".into(),
                    operation: SaaSOperation::OnboardTenant,
                    attempt: 3,
                    requested_by: "admin-17".into(),
                },
                reason: "CRM account is not ready".into(),
            })
        );
    }
}
