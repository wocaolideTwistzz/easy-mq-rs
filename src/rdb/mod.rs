pub mod constant;
pub mod scripts;

use bincode::config;
use deadpool_redis::redis::aio::ConnectionLike;

use crate::{
    errors::{Error, Result},
    rdb::{
        constant::{QName, RedisKey, ToQName},
        scripts::ENQUEUE,
    },
    task::Task,
};

// -- `KEYS[1]` -> easy-mq:topics
// -- `KEYS[2]` -> easy-mq:{topic}:qname
// -- `KEYS[3]` -> easy-mq:{qname}:task:{task_id}
// -- `KEYS[4]` -> easy-mq:{qname}:stream
// -- `KEYS[5]` -> easy-mq:{qname}:deadline

// -- `ARGV[1]` -> topic
// -- `ARGV[2]` -> qname
// -- `ARGV[3]` -> task data
// -- `ARGV[4]` -> current timestamp (in millisecond)
// -- `ARGV[5]` -> timeout (in millisecond)
// -- `ARGV[6]` -> deadline timestamp (in millisecond)
// -- `ARGV[7]` -> max retries
// -- `ARGV[8]` -> retry interval (in millisecond)
// -- `ARGV[9]` -> retention (in millisecond)
pub async fn enqueue(conn: &mut impl ConnectionLike, task: &Task) -> Result<String> {
    let task_data = bincode::serde::encode_to_vec(task, config::standard())?;

    let qname = task.to_qname();

    let topic_key = RedisKey::Topics.to_string();
    let qname_key = RedisKey::QName { topic: &task.topic }.to_string();
    let task_key = RedisKey::Task {
        qname: &qname,
        task_id: &task.id,
    }
    .to_string();
    let stream_key = RedisKey::Stream { qname: &qname }.to_string();
    let deadline_key = RedisKey::Deadline { qname: &qname }.to_string();
    let current = chrono::Local::now().timestamp_millis();
    let timeout = task.options.timeout_ms.unwrap_or_default();
    let deadline = task.options.deadline_ms.unwrap_or_default();
    let (max_retries, retry_interval) = if let Some(retry) = &task.options.retry {
        (retry.max_retries, retry.interval_ms)
    } else {
        (0, 0)
    };
    let retention = task.options.retention_ms;

    let ret: String = ENQUEUE
        .key(topic_key)
        .key(qname_key)
        .key(task_key)
        .key(stream_key)
        .key(deadline_key)
        .arg(&task.topic)
        .arg(&qname)
        .arg(task_data)
        .arg(current)
        .arg(timeout)
        .arg(deadline)
        .arg(max_retries)
        .arg(retry_interval)
        .arg(retention)
        .invoke_async(conn)
        .await?;

    if ret == "0" {
        return Err(Error::TaskAlreadyExists);
    }
    Ok(ret)
}

// -- KEYS -> [stream1, stream2 ...] (stream: easy-mq:{qname}:stream)
// -- ARGV[1] -> consumer
// -- ARGV[2] -> current timestamp in milliseconds
pub async fn dequeue(conn: &mut impl ConnectionLike, qnames: &[QName]) -> Result<Task> {
    todo!();
}
