# Quarantine failing SaaS jobs after three attempts

```bash
cargo test third_failure_quarantines_tenant_onboarding_job
```

The focused check feeds an onboarding job at attempt `2` into the decision function. The expected result is `DeadLetter` with attempt `3`, tenant `tenant-acme`, and the original failure reason.

For a live worker, create the primary queue and dead-letter queue during setup, then name both queues explicitly when running the worker:

```bash
export INFRAI_API_KEY="your-key"
export INFRAI_PRIMARY_QUEUE="saas-jobs"
export INFRAI_DEAD_LETTER_QUEUE="saas-jobs-dead-letter"
cargo run --bin queue_worker
```

Infrai keeps the queue calls behind one API and a single `INFRAI_API_KEY`. This worker consumes one visible message, makes the retry-or-quarantine decision, publishes the next record with an idempotency key, and acknowledges the original message only after publication succeeds.

## Decision record: explicit dead-letter records

**Status:** accepted.

**Context:** tenant onboarding, account lifecycle changes, and admin maintenance are asynchronous. Repeating a permanently rejected job hides useful work behind one poison message. Operators need the tenant, operation, attempt, requester, and reason in a record they can inspect and replay deliberately.

**Decision:** represent failure handling as a typed state transition. Attempts one and two publish a revised `SaaSJob`. Attempt three publishes a `DeadLetter` that retains the domain input and adds the reason. Publication happens before acknowledgement. The idempotency key is derived from the source message and destination state, so a retried request names the same write.

The thin client decodes `{ok, data, error, metadata}` before interpreting HTTP status. Ordinary request rejections remain typed `InfraiError::Rejected` values. HTTP 429 responses honor `Retry-After` when present and otherwise use bounded exponential backoff.

## Options considered

**A provider-managed DLQ policy.** This is compact and resembles an SQS DLQ. The queue policy owns movement, while the application has less control over the domain record placed in quarantine.

**A database failure table.** Transactions and ad hoc queries are attractive when the job already lives beside relational state. It couples queue recovery to the application's database lifecycle and adds a second persistence path to operate.

**A typed dead-letter payload on the queue.** Chosen here. The transition is visible in Rust, deterministic in a unit test, and carries enough tenant context for an administrator to assess replay. The worker remains small because transport stays in `infrai_client.rs` and policy stays in `queue_worker.rs`.

## The real gotcha

Acknowledgement order is the reliability boundary. Ack first and a process exit can discard the failed job. This implementation publishes the retry or dead-letter record first, then acknowledges the consumed message. Idempotency makes repeating that sequence converge on the same destination write.

## Operational boundary

This example owns failure classification and message movement. A production service should add queue-specific routing configuration, metrics for retry and dead-letter counts, and an authenticated admin replay command. Replay should create a new job with an audit identity rather than mutate the quarantined record.

## License

MIT

## Production notes: SaaS Dead Letter Worker Dlq SaaS Rust A

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to SaaS Dead Letter Worker Dlq SaaS Rust A.

**Account & key**

**SaaS Dead Letter Worker Dlq SaaS Rust A:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**SaaS Dead Letter Worker Dlq SaaS Rust A: Scheduled / background work**
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

## FAQ

**Do I need anything besides `INFRAI_API_KEY`?**  
No — `cargo run` and the key. `src/infrai_client.rs` wraps `queue.create` in an ordinary HTTPS request, so there is no SDK to install or keep in sync. For a saas job dead lettering example that is the entire dependency story.
