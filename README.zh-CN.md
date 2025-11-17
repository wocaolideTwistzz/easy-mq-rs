# easy-mq-rs

[English](README.md)

`easy-mq-rs` 是一个基于 Redis Streams 的轻量级任务队列与 Worker 框架，提供简单易用的 API 来投递拥有丰富调度选项的任务，并构建具备弹性与可配置拉取策略的消费者。

## 特性

- **Redis 驱动的 Broker**：兼容单机与集群 Redis。
- **丰富的任务配置**：支持优先级、重试策略、超时、截止时间、调度与 slot。
- **可配置的消费者**：并发度、队列刷新策略、退避策略均可自定义。
- **灵活的处理器模型**：实现 `Handler` trait 或使用 `AsyncHandler` 包装异步闭包。
- **运维友好的 API**：可编程地查看队列 / 主题、窥视任务、删除资源。
- **开箱即用的示例**：`examples/` 中提供交互式生产者与消费者 Demo。

## 快速开始

### 环境要求

- Rust 1.80+（edition 2024）
- Redis 6+，可通过 `REDIS_URL` 访问（默认 `redis://127.0.0.1:6379`）

### 安装

将依赖加入项目：

```toml
[dependencies]
easy-mq-rs = { git = "https://github.com/wocaolideTwistzz/easy-mq-rs" }
# 开发时可使用本地 path override
```

如需 Redis 集群支持，可开启对应 feature：

```toml
[dependencies]
easy-mq-rs = { version = "0.1", features = ["cluster"] }
```

### 运行示例

```bash
export REDIS_URL="redis://127.0.0.1:6379"
cargo run --example producer
cargo run --example consumer
```

生产者示例展示了多种任务选项，消费者示例会打印实时队列统计并并发处理任务。

## 使用方式

### 创建 Instance 并入队任务

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

### 构建消费者

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

## 配置速览

### 任务选项

| 选项 | 描述 |
| --- | --- |
| `priority` | 数值越小优先级越高，可使用负数表示紧急任务。 |
| `retention_ms` | 任务完成后保留的时间（默认 7 天）。 |
| `retry` | 定义 `max_retries` 与重试间隔 `interval_ms`。 |
| `timeout_ms` | 任务处理的最大执行时长。 |
| `deadline_ms` | 超过截止时间会被取消或跳过。 |
| `scheduled_at` | 通过时间戳延迟执行，或使用 `ScheduledAt::DependsOn` 等待依赖任务。 |
| `slot` | 在 Redis 集群模式下控制任务共址，支持跨 topic 依赖。 |

### 消费者配置

- **刷新策略**：刷新全部队列、指定主题、定时刷新，或提供固定队列列表（`RefreshQueuesStrategy`）。
- **退避策略**：固定、指数、斐波那契退避（`DequeueBackoffStrategy`）。
- **Claimer 参数**：通过 `ConsumerConfig` 字段调整定时任务、依赖任务、超时任务、截止任务、保留任务以及取消 Pending 任务的扫描间隔。

## 观测与运维

使用 `Instance` API 可进行自定义运维：

- `queue_stats`、`topic_stats`、`all_queues`、`topics`
- `peek_task`、`delete_topic`、`delete_queue`

借助这些接口可以轻松构建监控面板或自动化工具。

## 开发

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-targets
```

