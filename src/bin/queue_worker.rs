use saas_dead_letter_worker::{
    infrai_client::InfraiClient,
    queue_worker::{handle_one_failure, FailureDecision},
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = InfraiClient::from_env()?;
    let primary_queue = env::var("INFRAI_PRIMARY_QUEUE")?;
    let dead_letter_queue = env::var("INFRAI_DEAD_LETTER_QUEUE")?;
    let batch = client.consume(&primary_queue, 1, 30).await?;

    for message in batch.messages {
        let decision = handle_one_failure(
            &client,
            &primary_queue,
            &dead_letter_queue,
            &message.message_id,
            message.payload,
            "upstream account is pending",
        )
        .await?;
        match decision {
            FailureDecision::Retry(job) => println!(
                "tenant={} operation={:?} action=retry attempt={}",
                job.tenant_id, job.operation, job.attempt
            ),
            FailureDecision::DeadLetter(dead) => println!(
                "tenant={} operation={:?} action=dead_letter attempt={}",
                dead.job.tenant_id, dead.job.operation, dead.job.attempt
            ),
        }
    }
    Ok(())
}
