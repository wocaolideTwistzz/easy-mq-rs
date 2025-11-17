# easy-mq-rs

[简体中文](README.zh-CN.md)

`easy-mq-rs` is a lightweight task queue and worker framework powered by Redis Streams. It exposes a simple API for producing jobs with rich scheduling options and building resilient consumers with configurable queue refresh strategies.

## Features

- **Redis-backed broker** – works with standalone or clustered Redis deployments.
- **Rich task options** – priority, retry policy, timeout, deadline, scheduling, and slot.
- **Configurable consumers** – concurrency control, refresh strategies, and backoff policies.
- **Flexible handlers** – implement the `Handler` trait or wrap async closures via `AsyncHandler`.
- **Operational APIs** – inspect queues/topics, peek tasks, or delete resources programmatically.
- **Ready-to-run examples** – interactive producer and consumer demos under `examples/`.

## Getting Started

### Prerequisites

- Rust 1.80+ (edition 2024)
- Redis 6+ reachable at `REDIS_URL` (defaults to `redis://127.0.0.1:6379`)

### Install

Add the crate to your project:

```toml
[dependencies]
easy-mq-rs = { git = "https://github.com/wocaolideTwistzz/easy-mq-rs" }
# or use a local path override when developing locally
```

Enable Redis cluster support when needed:

```toml
[dependencies]
easy-mq-rs = { version = "0.1", features = ["cluster"] }
```

### Run the examples

```bash
export REDIS_URL="redis://127.0.0.1:6379"
cargo run --example producer
cargo run --example consumer
```

The producer showcases various task options, while the consumer prints live queue stats and processes jobs concurrently.

## Usage

### Create an instance and enqueue tasks

```rust
use easy_mq_rs::{instance::Instance, task::Task};

# async fn demo() -> easy_mq_rs::errors::Result<()> {
let instance = Instance::from_redis_url("redis://127.0.0.1:6379")?;

let mut task = Task::try_new("send_email", b"payload".to_vec())?;
task = task
    .with_priority(-1)
    .with_retry(5, 1_000)
    .with_timeout(5_000);

instance.add_task(&task).await?;
# Ok(())
# }
```

### Build a consumer

```rust
use easy_mq_rs::{
    consumer::{config::ConsumerConfig, handler::Handler, Consumer},
    task::Task,
};

struct EmailHandler;

#[async_trait::async_trait]
impl Handler for EmailHandler {
    async fn process_task(&self, task: &Task) -> easy_mq_rs::errors::Result<Option<Vec<u8>>> {
        println!("processing {}:{}", task.topic, task.id);
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> easy_mq_rs::errors::Result<()> {
    let consumer = Consumer::from_redis_url(
        "redis://127.0.0.1:6379",
        ConsumerConfig::default(),
    )?;

    consumer.run(EmailHandler).await;
    Ok(())
}
```

## Configuration Highlights

### Task options

| Option | Description |
| --- | --- |
| `priority` | Lower values run earlier; negatives allowed for urgent work. |
| `retention_ms` | How long completed tasks remain queryable (default 7 days). |
| `retry` | Configure `max_retries` and `interval_ms` between attempts. |
| `timeout_ms` | Upper bound for handler execution. |
| `deadline_ms` | Cancel or skip tasks exceeding the deadline. |
| `scheduled_at` | Delay execution by timestamp or wait for dependencies via `ScheduledAt::DependsOn`. |
| `slot` | Control co-location for Redis cluster deployments. |

### Consumer configuration

- **Refresh strategies**: poll all queues, specific topics, intervals, or provide a fixed list (`RefreshQueuesStrategy`).
- **Backoff strategies**: fixed, exponential, or Fibonacci (`DequeueBackoffStrategy`).
- **Claimer knobs**: tune scheduled, dependent, timeout, deadline, retention, and cancel intervals via `ConsumerConfig` fields.

## Observability & Management

Use the `Instance` APIs to introspect and administrate:

- `queue_stats`, `topic_stats`, `all_queues`, `topics`
- `peek_task`, `delete_topic`, `delete_queue`

These helpers make it straightforward to build dashboards or operational automation.

## Development

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-targets
```