use std::{sync::Arc, time::Duration};

use crate::{
    broker::{Broker, redis::RedisBroker},
    errors::{Error, Result},
    model::{Queue, Stats},
    task::Task,
};

/// Instance 是 easy-mq 的主要入口，提供任务管理和消费功能
/// Instance is the main entry point for easy-mq, providing task management and consumption
#[derive(Debug)]
pub struct Instance<B: Broker> {
    broker: Arc<B>,
}

impl Instance<RedisBroker> {
    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_url(url: &str) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_url(url)?);
        Ok(Self { broker })
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_urls<T: Into<Vec<String>>>(urls: T) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_urls(urls)?);
        Ok(Self { broker })
    }

    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_config(config: deadpool_redis::Config) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self { broker })
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_cluster_config(config: deadpool_redis::cluster::Config) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self { broker })
    }
}

impl<B: Broker + Send + Sync + 'static> Instance<B> {
    /// 创建一个新的 Instance
    /// Create a new Instance
    pub fn new(broker: Arc<B>) -> Self {
        Self { broker }
    }

    /// 获取 broker 的引用
    /// Get a reference to the broker
    pub fn broker(&self) -> &Arc<B> {
        &self.broker
    }

    pub fn clone_broker(&self) -> Arc<B> {
        self.broker.clone()
    }

    /// 添加任务到队列
    /// Add a task to the queue
    pub async fn add_task(&self, task: &Task) -> Result<()> {
        self.broker.add_task(task).await
    }

    /// 无阻塞从指定队列列表中取出一个任务
    /// Non-blocking dequeue a task from the specified queue list
    pub async fn try_dequeue(&self, queues: &[Queue]) -> Result<Option<Task>> {
        self.broker.dequeue(queues).await
    }

    /// 阻塞从指定队列列表中消费一个任务
    /// Blocking dequeue a task from the specified queue list
    pub async fn dequeue(&self, queues: &[Queue], interval: Duration) -> Result<Task> {
        loop {
            match self.try_dequeue(queues).await {
                Ok(Some(task)) => return Ok(task),
                Ok(None) => {
                    tokio::time::sleep(interval).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 无阻塞从指定主题下的队列列表中取出一个任务
    /// Non-blocking dequeue a task from the specified topic's queue list
    pub async fn try_dequeue_topic(&self, topic: &str) -> Result<Option<Task>> {
        let queues = self
            .queues(topic)
            .await?
            .into_iter()
            .map(|q| q.queue)
            .collect::<Vec<_>>();

        self.try_dequeue(&queues).await
    }

    /// 阻塞从指定主题下的队列列表中消费一个任务
    /// Blocking dequeue a task from the specified topic's queue list
    pub async fn dequeue_topic(&self, topic: &str, interval: Duration) -> Result<Task> {
        loop {
            match self.try_dequeue_topic(topic).await {
                Ok(Some(task)) => return Ok(task),
                Ok(None) => {
                    tokio::time::sleep(interval).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 标记任务成功完成
    /// Mark a task as successfully completed
    pub async fn succeed(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        result: Option<&[u8]>,
    ) -> Result<()> {
        self.broker.succeed(queue, task_id, stream_id, result).await
    }

    /// 标记任务成功完成
    /// Mark a task as successfully completed
    pub async fn mark_as_succeed(&self, task: &Task, result: Option<&[u8]>) -> Result<()> {
        let stream_id = match task.stream_id() {
            Some(stream_id) => stream_id,
            None => return Err(Error::MissingStreamID),
        };

        let queue = Queue::from(task);
        self.broker
            .succeed(&queue, &task.id, stream_id, result)
            .await
    }

    /// 标记任务失败
    /// Mark a task as failed
    pub async fn fail(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        err_msg: &str,
    ) -> Result<()> {
        self.broker.fail(queue, task_id, stream_id, err_msg).await
    }

    /// 标记任务失败
    /// Mark a task as failed
    pub async fn mark_as_fail(&self, task: &Task, err_msg: &str) -> Result<()> {
        let stream_id = match task.stream_id() {
            Some(stream_id) => stream_id,
            None => return Err(Error::MissingStreamID),
        };

        let queue = Queue::from(task);
        self.broker.fail(&queue, &task.id, stream_id, err_msg).await
    }

    /// 取消任务
    /// Cancel a task
    pub async fn cancel(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        err_msg: Option<&str>,
    ) -> Result<()> {
        self.broker.cancel(queue, task_id, stream_id, err_msg).await
    }

    /// 标记任务取消
    /// Mark a task as canceled
    pub async fn mark_as_cancel(&self, task: &Task, err_msg: Option<&str>) -> Result<()> {
        let stream_id = match task.stream_id() {
            Some(stream_id) => stream_id,
            None => return Err(Error::MissingStreamID),
        };

        let queue = Queue::from(task);
        self.broker
            .cancel(&queue, &task.id, stream_id, err_msg)
            .await
    }

    /// 查看指定任务的详细信息
    /// View detailed information of a specific task
    pub async fn peek_task(&self, queue: &Queue, task_id: &str) -> Result<Task> {
        self.broker.peek_task(queue, task_id).await
    }

    /// 获取队列统计信息
    /// Get queue statistics
    pub async fn queue_stats(&self, queue: &Queue) -> Result<Stats> {
        self.broker.queue_stats(queue).await
    }

    /// 获取主题统计信息
    /// Get topic statistics
    pub async fn topic_stats(&self, topic: &str) -> Result<Stats> {
        self.broker.topic_stats(topic).await
    }

    /// 获取指定主题下的队列列表
    /// Get queues under a specific topic
    pub async fn queues(&self, topic: &str) -> Result<Vec<crate::model::QueueInfo>> {
        self.broker.queues(topic).await
    }

    /// 获取所有主题列表
    /// Get all topics
    pub async fn topics(&self) -> Result<Vec<crate::model::TopicInfo>> {
        self.broker.topics().await
    }

    /// 删除指定主题
    /// Delete a specific topic
    pub async fn delete_topic(&self, topic: &str) -> Result<()> {
        self.broker.delete_topic(topic).await
    }

    /// 删除指定队列
    /// Delete a specific queue
    pub async fn delete_queue(&self, queue: &Queue) -> Result<()> {
        self.broker.delete_queue(queue).await
    }
}
