use std::{borrow::Cow, collections::HashMap};

use bincode::config;
use deadpool_redis::redis::{
    ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value, from_redis_value,
};

use crate::task::{CompletedTask, ScheduledAt, Task, TaskState};

pub const DEFAULT_WORKER: &str = "default";

#[derive(Debug)]
pub struct QName(pub String);

pub enum RedisKey<'a> {
    Topics,
    QName { topic: &'a str },
    Task { qname: &'a QName, task_id: &'a str },
    Stream { qname: &'a QName },
    Scheduled { qname: &'a QName },
    Dependent { qname: &'a QName },
    DependentTask { qname: &'a QName, task_id: &'a str },
    Archive { qname: &'a QName },
    Deadline { qname: &'a QName },
}

impl<'a> RedisKey<'a> {
    pub fn topics() -> RedisKey<'a> {
        Self::Topics
    }

    pub fn qname(topic: &'a str) -> RedisKey<'a> {
        Self::QName { topic }
    }

    pub fn task(qname: &'a QName, task_id: &'a str) -> RedisKey<'a> {
        Self::Task { qname, task_id }
    }

    pub fn stream(qname: &'a QName) -> RedisKey<'a> {
        Self::Stream { qname }
    }

    pub fn scheduled(qname: &'a QName) -> RedisKey<'a> {
        Self::Scheduled { qname }
    }

    pub fn dependent(qname: &'a QName) -> RedisKey<'a> {
        Self::Dependent { qname }
    }

    pub fn dependent_task(qname: &'a QName, task_id: &'a str) -> RedisKey<'a> {
        Self::DependentTask { qname, task_id }
    }

    pub fn archive(qname: &'a QName) -> RedisKey<'a> {
        Self::Archive { qname }
    }

    pub fn deadline(qname: &'a QName) -> RedisKey<'a> {
        Self::Deadline { qname }
    }
}

impl<'a> std::fmt::Display for RedisKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisKey::Topics => write!(f, "easy-mq:topics"),
            RedisKey::QName { topic } => write!(f, "easy-mq:{{{}}}:qname", topic),
            RedisKey::Task { qname, task_id } => {
                write!(f, "easy-mq:{}:task:{{{}}}", qname, task_id)
            }
            RedisKey::Stream { qname } => write!(f, "easy-mq:{}:stream", qname),
            RedisKey::Scheduled { qname } => write!(f, "easy-mq:{}:scheduled", qname),
            RedisKey::Dependent { qname } => write!(f, "easy-mq:{}:dependent", qname),
            RedisKey::DependentTask { qname, task_id } => {
                write!(f, "easy-mq:{}:dependent:{{{}}}", qname, task_id)
            }
            RedisKey::Archive { qname } => write!(f, "easy-mq:{}:archive", qname),
            RedisKey::Deadline { qname } => write!(f, "easy-mq:{}:deadline", qname),
        }
    }
}

impl std::fmt::Display for QName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl QName {
    pub fn new(topic: impl AsRef<str>, priority: i8) -> QName {
        QName(format!("{{{}}}:{{{}}}", topic.as_ref(), priority))
    }

    pub fn new_with_slot(slot: impl AsRef<str>, topic: impl AsRef<str>, priority: i8) -> QName {
        QName(format!(
            "{{{}}}:{{{}}}:{{{}}}",
            slot.as_ref(),
            topic.as_ref(),
            priority
        ))
    }

    pub fn from_task(task: &Task) -> QName {
        match task.options.slot.as_ref() {
            Some(slot) => QName::new_with_slot(slot, &task.topic, task.options.priority),
            None => QName::new(&task.topic, task.options.priority),
        }
    }

    pub fn from_completed_task(task: &CompletedTask) -> QName {
        match task.slot.as_ref() {
            Some(slot) => QName::new_with_slot(slot, &task.topic, task.priority),
            None => QName::new(&task.topic, task.priority),
        }
    }
}

impl FromRedisValue for Task {
    fn from_redis_value(
        v: &deadpool_redis::redis::Value,
    ) -> deadpool_redis::redis::RedisResult<Self> {
        if let Some(m) = v.as_map_iter() {
            let map = m
                .into_iter()
                .map(|(k, v)| Ok((k.as_str()?, v)))
                .collect::<RedisResult<HashMap<Cow<'_, str>, &Value>>>()?;

            let data = map
                .get("data")
                .ok_or(FromRedisValueError::MissingTaskData)?
                .as_vec()?;

            let mut task: Task = bincode::serde::decode_from_slice(data, config::standard())
                .map_err(FromRedisValueError::DecodeTaskDataError)?
                .0;

            task.runtime.state = map
                .get("state")
                .ok_or(FromRedisValueError::MissingTaskState)?
                .as_str()?
                .as_ref()
                .try_into()
                .map_err(FromRedisValueError::DecodeTaskStateError)?;

            if let Some(stream_id) = map.get("stream_id") {
                task.runtime.stream_id = from_redis_value(stream_id)?
            }
            if let Some(retried) = map.get("retried") {
                task.runtime.retried = from_redis_value(retried)?;
            }
            if let Some(is_orphaned) = map.get("is_orphaned") {
                task.runtime.is_orphaned = from_redis_value(is_orphaned)?;
            }
            if let Some(created_at) = map.get("created_at") {
                task.runtime.created_at_ms = from_redis_value(created_at)?;
            }
            if let Some(next_process_at) = map.get("next_process_at") {
                task.runtime.next_process_at_ms = from_redis_value(next_process_at)?;
            }
            if let Some(last_pending_at) = map.get("last_pending_at") {
                task.runtime.last_pending_at_ms = from_redis_value(last_pending_at)?;
            }
            if let Some(last_active_at) = map.get("last_active_at") {
                task.runtime.last_active_at_ms = from_redis_value(last_active_at)?;
            }
            if let Some(last_worker) = map.get("last_worker") {
                task.runtime.last_worker = from_redis_value(last_worker)?;
            }
            if let Some(last_err) = map.get("last_err") {
                task.runtime.last_err = from_redis_value(last_err)?;
            }
            if let Some(last_err_at) = map.get("last_err_at") {
                task.runtime.last_err_at_ms = from_redis_value(last_err_at)?;
            }
            if let Some(completed_at) = map.get("completed_at") {
                task.runtime.completed_at_ms = from_redis_value(completed_at)?;
            }
            if let Some(result) = map.get("result") {
                task.runtime.result = from_redis_value(result)?;
            }
            return Ok(task);
        }
        Err(FromRedisValueError::TaskValueIsNotAMap.into())
    }
}

trait RedisValueExt {
    fn as_str(&self) -> RedisResult<Cow<'_, str>>;

    fn as_vec(&self) -> RedisResult<&[u8]>;
}

impl RedisValueExt for Value {
    fn as_str(&self) -> RedisResult<Cow<'_, str>> {
        match self {
            Value::BulkString(bytes) => Ok(Cow::Borrowed(str::from_utf8(bytes)?)),
            Value::Okay => Ok(Cow::Borrowed("OK")),
            Value::SimpleString(val) => Ok(Cow::Borrowed(val)),
            Value::VerbatimString { format: _, text } => Ok(Cow::Borrowed(text)),
            Value::Double(val) => Ok(Cow::Owned(val.to_string())),
            Value::Int(val) => Ok(Cow::Owned(val.to_string())),
            _ => Err(FromRedisValueError::NotAStr.into()),
        }
    }

    fn as_vec(&self) -> RedisResult<&[u8]> {
        match self {
            Value::BulkString(bytes) => Ok(bytes),
            _ => Err(FromRedisValueError::NotABulkString.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FromRedisValueError {
    #[error("missing task data")]
    MissingTaskData,

    #[error("missing task state")]
    MissingTaskState,

    #[error("decode task data failed: {0}")]
    DecodeTaskDataError(#[from] bincode::error::DecodeError),

    #[error("decode task state failed: {0}")]
    DecodeTaskStateError(#[from] crate::errors::Error),

    #[error("not a str")]
    NotAStr,

    #[error("not a bulk string")]
    NotABulkString,

    #[error("task value is not a map")]
    TaskValueIsNotAMap,
}

impl From<FromRedisValueError> for RedisError {
    fn from(value: FromRedisValueError) -> Self {
        match value {
            FromRedisValueError::MissingTaskData => {
                RedisError::from((ErrorKind::ParseError, "missing task data"))
            }
            FromRedisValueError::MissingTaskState => {
                RedisError::from((ErrorKind::ParseError, "missing task state"))
            }
            FromRedisValueError::DecodeTaskDataError(e) => RedisError::from((
                ErrorKind::ParseError,
                "decode task data failed",
                e.to_string(),
            )),
            FromRedisValueError::DecodeTaskStateError(e) => RedisError::from((
                ErrorKind::ParseError,
                "decode task state failed",
                e.to_string(),
            )),
            FromRedisValueError::NotAStr => RedisError::from((ErrorKind::TypeError, "not a str")),
            FromRedisValueError::NotABulkString => {
                RedisError::from((ErrorKind::TypeError, "not a bulk string"))
            }
            FromRedisValueError::TaskValueIsNotAMap => {
                RedisError::from((ErrorKind::TypeError, "task value is not a map"))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct RedisTaskArgs<'a> {
    pub task: &'a Task,

    pub current: i64,

    pub task_data: Option<Vec<u8>>,

    pub scheduled_at: Option<u64>,

    pub dependent: Option<Vec<(String, String)>>,
}

impl<'a> ToRedisArgs for RedisTaskArgs<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        match self.task.runtime.state {
            TaskState::Pending => {
                self.write_task_msg(out);
            }
            TaskState::Scheduled => {
                self.write_task_msg(out);
                // -- `ARGV[8]` -> scheduled (in milliseconds)
                self.scheduled_at.write_redis_args(out);
            }
            TaskState::Dependent => {
                self.write_task_msg(out);
                // -- `ARGV[8..]` -> field: task_key; value: task_state
                self.dependent.write_redis_args(out);
            }
            TaskState::Succeed => {
                self.write_task_complete(out);
                // -- optional - `ARGV[5]` -> result
                self.task.runtime.result.write_redis_args(out);
            }
            TaskState::Failed | TaskState::Canceled => {
                self.write_task_complete(out);
                // -- optional - `ARGV[5]` -> error message
                self.task.runtime.last_err.write_redis_args(out);
            }
            TaskState::Retry => {
                self.write_task_retry(out);
            }
            _ => {}
        }
    }
}

impl<'a> TryFrom<&'a Task> for RedisTaskArgs<'a> {
    type Error = crate::errors::Error;

    fn try_from(value: &'a Task) -> Result<Self, Self::Error> {
        let mut args = RedisTaskArgs {
            task: value,
            task_data: None,
            current: chrono::Local::now().timestamp_millis(),
            scheduled_at: None,
            dependent: None,
        };

        match value.runtime.state {
            TaskState::Pending => {
                args.task_data = Some(bincode::serde::encode_to_vec(value, config::standard())?)
            }
            TaskState::Scheduled => {
                args.task_data = Some(bincode::serde::encode_to_vec(value, config::standard())?);
                match value.options.scheduled_at.as_ref() {
                    Some(ScheduledAt::TimestampMs(t)) => args.scheduled_at = Some(*t),
                    _ => return Err(crate::errors::Error::ScheduledAtTimeNotSet),
                }
            }
            TaskState::Dependent => match value.options.scheduled_at.as_ref() {
                Some(ScheduledAt::DependsOn(tasks)) => {
                    args.task_data =
                        Some(bincode::serde::encode_to_vec(value, config::standard())?);
                    args.dependent = Some(
                        tasks
                            .iter()
                            .map(|task| {
                                let qname = QName::from_completed_task(task);
                                (
                                    RedisKey::task(&qname, &task.id).to_string(),
                                    task.state.to_string(),
                                )
                            })
                            .collect(),
                    );
                }
                _ => return Err(crate::errors::Error::DependentTasksNotSet),
            },
            TaskState::Retry => {
                if value.runtime.stream_id.is_none() {
                    return Err(crate::errors::Error::MissingStreamID);
                }
                match value.options.retry.as_ref() {
                    Some(retry) => {
                        if retry.max_retries <= value.runtime.retried {
                            return Err(crate::errors::Error::RetryHasExceeded);
                        }
                    }
                    None => return Err(crate::errors::Error::RetryNotSet),
                }
            }
            TaskState::Succeed | TaskState::Canceled | TaskState::Failed => {
                if value.runtime.stream_id.is_none() {
                    return Err(crate::errors::Error::MissingStreamID);
                }
            }
            _ => {}
        }

        Ok(args)
    }
}

/// -- `ARGV[1]` -> task data
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> timeout (in milliseconds)
/// -- `ARGV[4]` -> deadline timestamp (in milliseconds)
/// -- `ARGV[5]` -> max retries
/// -- `ARGV[6]` -> retry interval (in milliseconds)
/// -- `ARGV[7]` -> retention (in milliseconds)
impl<'a> RedisTaskArgs<'a> {
    fn write_task_msg<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        // -- `ARGV[1]` -> task data
        self.task_data.write_redis_args(out);
        // -- `ARGV[2]` -> current timestamp (in milliseconds)
        self.current.write_redis_args(out);
        // -- `ARGV[3]` -> timeout (in milliseconds)
        self.task
            .options
            .timeout_ms
            .unwrap_or_default()
            .write_redis_args(out);
        // -- `ARGV[4]` -> deadline timestamp (in milliseconds)
        self.task
            .options
            .deadline_ms
            .unwrap_or_default()
            .write_redis_args(out);
        // -- `ARGV[5]` -> max retries
        self.task
            .options
            .retry
            .as_ref()
            .map(|v| v.max_retries)
            .unwrap_or_default()
            .write_redis_args(out);
        // -- `ARGV[6]` -> retry interval (in milliseconds)
        self.task
            .options
            .retry
            .as_ref()
            .map(|v| v.interval_ms)
            .unwrap_or_default()
            .write_redis_args(out);
        // -- `ARGV[7]` -> retention (in milliseconds)
        self.task.options.retention_ms.write_redis_args(out);
    }

    /// -- `ARGV[1]` -> stream id
    /// -- `ARGV[2]` -> task completed state (succeed/failed/canceled)
    /// -- `ARGV[3]` -> current timestamp (in milliseconds)
    /// -- `ARGV[4]` -> archive expire timestamp (in milliseconds)
    fn write_task_complete<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        //  -- `ARGV[1]` -> stream id
        self.task.runtime.stream_id.write_redis_args(out);
        // -- `ARGV[2]` -> current timestamp (in milliseconds)
        self.current.write_redis_args(out);
        // -- `ARGV[3]` -> task completed state (succeed/failed/canceled)
        self.task.runtime.state.as_ref().write_redis_args(out);
        let archive_expired_at = self.current + self.task.options.retention_ms as i64;
        // -- `ARGV[4]` -> archive expire timestamp (in milliseconds)
        archive_expired_at.write_redis_args(out);
    }

    /// -- `ARGV[1]` -> stream_id
    /// -- `ARGV[2]` -> current timestamp (in milliseconds)
    /// -- `ARGV[3]` -> retried
    /// -- `ARGV[4]` -> next_process_at
    /// -- `ARGV[5]` -> timeout (in milliseconds)
    /// -- `ARGV[6]` -> max retries
    /// -- `ARGV[7]` -> retry interval (in milliseconds)
    /// -- `ARGV[8]` -> optional - error msg
    fn write_task_retry<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        // -- `ARGV[1]` -> stream_id
        self.task.runtime.stream_id.write_redis_args(out);
        // -- `ARGV[2]` -> current timestamp (in milliseconds)
        self.current.write_redis_args(out);
        // -- `ARGV[3]` -> retried
        (self.task.runtime.retried + 1).write_redis_args(out);
        // -- `ARGV[4]` -> next_process_at
        (self.current
            + self
                .task
                .options
                .retry
                .as_ref()
                .map(|v| v.interval_ms as i64)
                .unwrap_or_default())
        .write_redis_args(out);
        // -- `ARGV[5]` -> timeout (in milliseconds)
        self.task.options.timeout_ms.write_redis_args(out);
        // -- `ARGV[6]` -> max retries
        self.task
            .options
            .retry
            .as_ref()
            .map(|v| v.max_retries)
            .write_redis_args(out);
        // -- `ARGV[7]` -> retry interval (in milliseconds)
        self.task
            .options
            .retry
            .as_ref()
            .map(|v| v.interval_ms)
            .write_redis_args(out);
        // -- `ARGV[8]` -> optional - error msg
        self.task.runtime.last_err.write_redis_args(out);
    }
}
