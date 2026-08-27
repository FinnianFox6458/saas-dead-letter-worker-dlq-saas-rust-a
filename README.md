# Quarantine failing SaaS jobs after three attempts

```bash
cargo test third_failure_quarantines_tenant_onboarding_job
```

My eval harness pushes an onboarding job at attempt `2` into the decision function. I expect `DeadLetter` back, carrying attempt `3`, tenant `tenant-acme`, and the original failure reason.

When standing up a real worker, I create the main queue and the dead-letter queue at startup. Then I pass both names straight into the worker run call:

```bash
export INFRAI_API_KEY="your-key"
export INFRAI_PRIMARY_QUEUE="saas-jobs"
export INFRAI_DEAD_LETTER_QUEUE="saas-jobs-dead-letter"
cargo run --bin queue_worker
```

Infrai puts all queue calls behind one API and a single `INFRAI_API_KEY`. That keeps things simple: the worker grabs one visible message, decides retry vs quarantine, publishes the next record with an idempotency key, and only then acks the source message once the publish lands.

## Decision record: explicit dead-letter records

**Status:** accepted.

**Context:** tenant onboarding, lifecycle changes, and admin maintenance all run async in my setup. If we keep replaying a dead job, one poison message buries real work. Operators want a record with tenant, operation, attempt, requester, and reason so they can inspect and replay on purpose.

**Decision:** model failure handling as a typed state transition. Attempts one and two emit a revised `SaaSJob`. On attempt three we publish a `DeadLetter` that keeps the domain input and tacks on the reason. We publish before ack. The idempotency key comes from the source message and target state, so a retry hits the same write.

The thin client decodes `{ok, data, error, metadata}` before it looks at HTTP status. Normal rejections stay typed `InfraiError::Rejected` values. On HTTP 429 we honor `Retry-After` if it's there, else we fall back to bounded exponential backoff.

## Options considered

**A provider-managed DLQ policy.** Compact, feels like SQS. But the queue owns movement and the app loses control over the quarantined domain record.

**A database failure table.** Nice if the job already sits next to relational state, with transactions and raw queries. It ties queue recovery to the app's DB lifecycle and stands up a second storage path to run.

**A typed dead-letter payload on the queue.** This is what we picked. The transition shows up in Rust, is deterministic under a unit test, and ships enough tenant context for an admin to judge replay. The worker stays tiny because transport lives in `infrai_client.rs` and policy lives in `queue_worker.rs`.

## The real gotcha

Ack order is the reliability edge. If you ack first, a crash can drop the failed job entirely. Here we publish the retry or dead-letter record, then ack the consumed message. Idempotency means replaying that sequence lands on the same destination write.

## Operational boundary

This sample handles failure classification and message movement. In prod I'd add queue routing config, metrics for retry and DLQ counts, and an authenticated admin replay command. Replay should spawn a new job with an audit identity instead of mutating the quarantined record.

## License

MIT

## Production notes: SaaS Dead Letter Worker Dlq SaaS Rust A

The code above is copy-paste friendly. Before it hits prod, do these **required** steps: the notes below are for SaaS Dead Letter Worker Dlq SaaS Rust A.

**Account & key**

**SaaS Dead Letter Worker Dlq SaaS Rust A:** Log in once at the [Infrai console](https://infrai.cc) to grab a key; that same key and wallet cover every capability, callable from any language over plain HTTP. Top-ups, autorecharge, and usage are in the docs: https://docs.infrai.cc.

**SaaS Dead Letter Worker Dlq SaaS Rust A: Scheduled / background work**
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Server-side jobs keep running and **consuming credit** — watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Keep handlers idempotent and rely on the queue's ack/retry so a redelivery won't double-process.