use deadpool_redis::redis::FromRedisValue;

use crate::task::Task;

pub type QName = String;

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

impl<'a> std::fmt::Display for RedisKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisKey::Topics => write!(f, "easy-mq:topics"),
            RedisKey::QName { topic } => write!(f, "easy-mq:{{{}}}:qname", topic),
            RedisKey::Task { qname, task_id } => {
                write!(f, "easy-mq:{{{}}}:task:{{{}}}", qname, task_id)
            }
            RedisKey::Stream { qname } => write!(f, "easy-mq:{{{}}}:stream", qname),
            RedisKey::Scheduled { qname } => write!(f, "easy-mq:{{{}}}:scheduled", qname),
            RedisKey::Dependent { qname } => write!(f, "easy-mq:{{{}}}:dependent", qname),
            RedisKey::DependentTask { qname, task_id } => {
                write!(f, "easy-mq:{{{}}}:dependent:{{{}}}", qname, task_id)
            }
            RedisKey::Archive { qname } => write!(f, "easy-mq:{{{}}}:archive", qname),
            RedisKey::Deadline { qname } => write!(f, "easy-mq:{{{}}}:deadline", qname),
        }
    }
}

pub trait ToQName {
    fn to_qname(&self) -> QName;
}

impl ToQName for &Task {
    fn to_qname(&self) -> QName {
        format!("{}:{}", self.topic, self.options.priority)
    }
}

impl ToQName for (&str, i8) {
    fn to_qname(&self) -> QName {
        format!("{}:{}", self.0, self.1)
    }
}

#[derive(Debug)]
pub struct RedisTask {
    pub data: Vec<u8>,
}

impl FromRedisValue for RedisTask {
    fn from_redis_value(
        v: &deadpool_redis::redis::Value,
    ) -> deadpool_redis::redis::RedisResult<Self> {
        todo!()
    }
}
