use std::{borrow::Cow, collections::HashMap};

use bincode::config;
use deadpool_redis::redis::{
    ErrorKind, FromRedisValue, RedisError, RedisResult, Value, from_redis_value,
};

use crate::task::Task;

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

            task.runtime.stream_id = from_redis_value(
                map.get("stream_id")
                    .ok_or(FromRedisValueError::MissingStreamId)?,
            )?;

            if let Some(retried) = map.get("retried") {
                task.runtime.retried = from_redis_value(retried)?;
            }
            if let Some(next_process_at) = map.get("next_process_at") {
                task.runtime.next_process_at_ms = from_redis_value(next_process_at)?;
            }
            if let Some(is_orphaned) = map.get("is_orphaned") {
                task.runtime.is_orphaned = from_redis_value(is_orphaned)?;
            }
            if let Some(created_at) = map.get("created_at") {
                task.runtime.created_at_ms = from_redis_value(created_at)?;
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

    #[error("missing stream id")]
    MissingStreamId,

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
            FromRedisValueError::MissingStreamId => {
                RedisError::from((ErrorKind::ParseError, "missing stream id"))
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
