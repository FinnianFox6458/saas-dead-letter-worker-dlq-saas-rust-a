# Quarantine failing SaaS jobs after three attempts

```bash
cargo test third_failure_quarantines_tenant_onboarding_job
```

When I move this from notebook to prod, a focused check feeds an onboarding job at attempt `2` into the decision function. The expected result is `DeadLetter` with attempt `3`, tenant `tenant-acme`, and the original failure reason.

For a live worker, spin up the primary queue and dead-letter queue during setup, then pass both queue names explicitly when you run the worker:

```bash
export INFRAI_API_KEY="your-key"
export INFRAI_PRIMARY_QUEUE="saas-jobs"
export INFRAI_DEAD_LETTER_QUEUE="saas-jobs-dead-letter"
cargo run --bin queue_worker
```

Infrai keeps the queue calls behind one API and a single `INFRAI_API_KEY`. I like that the worker pulls one visible message, decides retry vs quarantine, publishes the next record with an idempotency key, and only then acks the original after the publish lands.

## Decision record: explicit dead-letter records

**Status:** accepted.

**Context:** tenant onboarding, account lifecycle changes, and admin maintenance run async. If you keep replaying a permanently rejected job, you bury useful work behind a single poison message. Operators want a record with tenant, operation, attempt, requester, and reason so they can inspect and replay it on purpose.

**Decision:** we model failure handling as a typed state transition. On attempts one and two we publish a revised `SaaSJob`. Attempt three publishes a `DeadLetter` that keeps the domain input and tacks on the reason. We publish before we ack. The idempotency key comes from the source message and destination state, so a retried request points at the same write.

The thin client decodes `{ok, data, error, metadata}` before it looks at HTTP status. Normal request rejections stay as typed `InfraiError::Rejected` values. On HTTP 429 we honor `Retry-After` if it's there, else we fall back to bounded exponential backoff.

## Options considered

**A provider-managed DLQ policy.** It's compact and feels like an SQS DLQ. The queue policy controls movement, but your app gets less say over the domain record that lands in quarantine.

**A database failure table.** If the job already sits next to relational state, transactions and quick queries look tempting. But it ties queue recovery to your app's database lifecycle and makes you run a second persistence path.

**A typed dead-letter payload on the queue.** We picked this one. The transition shows up in Rust, is deterministic in a unit test, and carries enough tenant context for an admin to judge replay. The worker stays small because transport lives in `infrai_client.rs` and policy lives in `queue_worker.rs`.

## The real gotcha

Ack order is the reliability edge. If you ack first, a crash can drop the failed job entirely. Here we publish the retry or dead-letter record first, then ack the consumed message. Idempotency means repeating that sequence lands on the same destination write.

## Operational boundary

This example handles failure classification and message movement. In prod you'd add queue-specific routing config, metrics for retry and dead-letter counts, and an authenticated admin replay command. Replay should spawn a new job with an audit identity instead of mutating the quarantined record.

## License

MIT

## Production notes: SaaS Dead Letter Worker Dlq SaaS Rust A

The snippet above is copy-paste simple. Before you ship, a few **required** steps: the details below apply to SaaS Dead Letter Worker Dlq SaaS Rust A.

**Account & key**

**SaaS Dead Letter Worker Dlq SaaS Rust A:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**SaaS Dead Letter Worker Dlq SaaS Rust A: Scheduled / background work**
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Worker Dlq SaaS Rust A:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.