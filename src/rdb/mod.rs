pub mod constant;
pub mod scripts;

use bincode::config;
use deadpool_redis::redis::{self, aio::ConnectionLike};

use crate::{
    errors::{Error, Result},
    rdb::{
        constant::{QName, RedisKey, ToQName},
        scripts::{DEQUEUE, ENQUEUE},
    },
    task::Task,
};

// -- `KEYS[1]` -> easy-mq:{topic}:qname
// -- `KEYS[2]` -> easy-mq:{topic}:{priority}:task:{task_id}
// -- `KEYS[3]` -> easy-mq:{topic}:{priority}:stream
// -- `KEYS[4]` -> easy-mq:{topic}:{priority}:deadline
//
// -- `ARGV[1]` -> qname - ({topic}:{priority})
// -- `ARGV[2]` -> task data
// -- `ARGV[3]` -> current timestamp (in milliseconds)
// -- `ARGV[4]` -> timeout (in milliseconds)
// -- `ARGV[5]` -> deadline timestamp (in milliseconds)
// -- `ARGV[6]` -> max retries
// -- `ARGV[7]` -> retry interval (in milliseconds)
// -- `ARGV[8]` -> retention (in milliseconds)
pub async fn enqueue(conn: &mut impl ConnectionLike, task: &Task) -> Result<String> {
    let task_data = bincode::serde::encode_to_vec(task, config::standard())?;

    let current = chrono::Local::now().timestamp_millis();

    let qname = task.to_qname();
    let task_key = RedisKey::task(&qname, &task.id).to_string();
    let stream_key = RedisKey::stream(&qname).to_string();
    let deadline_key = RedisKey::deadline(&qname).to_string();

    let timeout = task.options.timeout_ms.unwrap_or_default();
    let deadline = task.options.deadline_ms.unwrap_or_default();
    let (max_retries, retry_interval) = if let Some(retry) = &task.options.retry {
        (retry.max_retries, retry.interval_ms)
    } else {
        (0, 0)
    };
    let retention = task.options.retention_ms;

    // 1. 任务入队
    let ret: String = ENQUEUE
        .key(task_key)
        .key(stream_key)
        .key(deadline_key)
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

    // 2. 更新topic时间
    redis::Cmd::zadd(RedisKey::topics().to_string(), &task.topic, current)
        .exec_async(conn)
        .await?;

    // 3. 更新topic下的qname
    redis::Cmd::hset(
        RedisKey::qname(&task.topic).to_string(),
        &task.topic,
        qname.0,
    )
    .exec_async(conn)
    .await?;

    Ok(ret)
}

// -- KEYS -> [stream1, stream2 ...] (stream: easy-mq:{qname}:stream)
// -- ARGV[1] -> consumer (worker)
// -- ARGV[2] -> current timestamp in milliseconds
pub async fn dequeue(
    conn: &mut impl ConnectionLike,
    qnames: &[QName],
    worker: Option<&str>,
) -> Result<Task> {
    let worker = worker.unwrap_or("default");
    let current = chrono::Local::now().timestamp_millis();

    let task = DEQUEUE
        .key(
            qnames
                .iter()
                .map(|qname| RedisKey::stream(qname).to_string())
                .collect::<Vec<_>>(),
        )
        .arg(worker)
        .arg(current)
        .invoke_async(conn)
        .await?;

    Ok(task)
}

#[cfg(test)]
mod test {
    use deadpool_redis::{Config, Connection};

    use crate::{
        rdb::{constant::ToQName, dequeue, enqueue},
        task::Task,
    };

    async fn new_redis_conn() -> Connection {
        let pool = Config::from_url("redis://127.0.0.1:6379")
            .builder()
            .unwrap()
            .build()
            .unwrap();

        pool.get().await.unwrap()
    }

    #[tokio::test]
    async fn test_enqueue() {
        let task = Task::new("test_topic", None);

        let mut conn = new_redis_conn().await;

        let stream_id = enqueue(&mut conn, &task).await.unwrap();

        println!("{} - {}", task.id, stream_id)
    }

    #[tokio::test]
    async fn test_dequeue() {
        let mut conn = new_redis_conn().await;

        let qname = ("test_topic", 0).to_qname();
        let task = dequeue(&mut conn, &[qname], None).await.unwrap();

        println!("{:?}", task)
    }
}
