use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::{
    broker::{Broker, redis::RedisBroker},
    errors::Result,
    model::{Queue, TopicQueuesInfo},
};

#[derive(Debug)]
pub struct Claimer<B: Broker> {
    broker: Arc<B>,

    queues: ArcSwap<Vec<TopicQueuesInfo>>,
}

impl Claimer<RedisBroker> {
    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_url(url: &str) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_url(url)?);
        Ok(Self::new(broker))
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_urls<T: Into<Vec<String>>>(urls: T) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_urls(urls)?);
        Ok(Self::new(broker))
    }

    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_config(config: deadpool_redis::Config) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self::new(broker))
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_cluster_config(config: deadpool_redis::cluster::Config) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self::new(broker))
    }
}

#[derive(Debug, Clone)]
pub struct ClaimConfig {
    /// 定时任务 claim 间隔 (秒) 默认5秒
    /// Scheduled task claim interval (seconds) default 5 seconds
    pub scheduled_claim_interval_sec: u64,

    /// 依赖任务 claim 间隔 (秒) 默认5秒
    /// Dependent task claim interval (seconds) default 5 seconds
    pub dependent_claim_interval_sec: u64,

    /// 超时任务 claim 间隔 (秒) 默认5秒
    /// Timeout task claim interval (seconds) default 5 seconds
    pub timeout_claim_interval_sec: u64,

    /// 超时任务的最小空闲时间 (秒) 默认5秒
    /// Minimum idle time for timeout tasks (seconds) default 5 seconds
    pub timeout_min_idle_sec: u64,

    /// 截止时间任务 claim 间隔 (秒) 默认60秒
    /// Deadline task claim interval (seconds) default 60 seconds
    pub deadline_claim_interval_sec: u64,

    /// 保留时间任务 claim 间隔 (秒) 默认60秒
    /// Retention task claim interval (seconds) default 60 seconds
    pub retention_claim_interval_sec: u64,

    /// pending 任务取消的最小等待时间 (秒) 默认关闭
    /// Minimum pending time for canceling pending tasks (seconds) default disabled
    pub cancel_pending_min_sec: u64,

    /// pending 任务取消的 claim 间隔 (秒) 默认关闭
    /// Pending task cancel claim interval (seconds) default disabled
    pub cancel_pending_claim_interval_sec: u64,

    /// 刷新队列列表的间隔 (秒) 默认60秒
    /// Refresh queue list interval (seconds) default 60 seconds
    pub refresh_queues_interval_sec: u64,

    /// 清理空闲超时的主题的间隔 (秒) 默认不设置
    /// Clean idle expired topics interval (seconds) default disabled
    pub clean_topics_interval_sec: u64,

    /// 清理空闲超时的主题的最小空闲时间 (秒) 默认不设置
    /// Minimum idle time for cleaning idle expired topics (seconds) default disabled
    pub clean_topics_min_idle_sec: u64,
}

impl<B: Broker + Send + Sync + 'static> Claimer<B> {
    pub fn new(broker: Arc<B>) -> Self {
        Self {
            broker,
            queues: ArcSwap::new(Arc::new(Vec::new())),
        }
    }

    /// 设置当前 claimer 托管的队列列表
    /// Set the queue list managed by the claimer
    pub fn set_queues(&self, queues: Vec<TopicQueuesInfo>) {
        self.queues.store(Arc::new(queues));
    }

    /// 获取当前 claimer 托管的队列列表
    /// Get the queue list managed by the claimer
    pub fn get_queues(&self) -> Arc<Vec<TopicQueuesInfo>> {
        self.queues.load().clone()
    }

    /// 刷新当前 claimer 托管的队列列表
    /// Refresh the queue list managed by the claimer
    pub async fn refresh_queues(&self) -> Result<usize> {
        let queues = self.broker.all_queues().await?;
        let n = queues.len();
        self.queues.store(Arc::new(queues));
        Ok(n)
    }

    /// 手动 claim 定时任务
    /// Manually claim scheduled tasks
    pub async fn claim_scheduled(&self, queue: &Queue) -> Result<usize> {
        self.broker.claim_scheduled(queue).await
    }

    /// 手动 claim 依赖任务
    /// Manually claim dependent tasks
    pub async fn claim_dependent(&self, queue: &Queue) -> Result<(usize, usize)> {
        self.broker.claim_dependent(queue).await
    }

    /// 手动 claim 超时任务
    /// Manually claim timeout tasks
    pub async fn claim_timeout(&self, queue: &Queue, min_idle_ms: u64) -> Result<(usize, usize)> {
        self.broker.claim_timeout(queue, min_idle_ms).await
    }

    /// 手动 claim 截止时间任务
    /// Manually claim deadline tasks
    pub async fn claim_deadline(&self, queue: &Queue) -> Result<usize> {
        self.broker.claim_deadline(queue).await
    }

    /// 手动 claim 保留时间任务
    /// Manually claim retention tasks
    pub async fn claim_retention(&self, queue: &Queue) -> Result<usize> {
        self.broker.claim_retention(queue).await
    }

    /// 手动取消超时的 pending 任务
    /// Manually cancel pending tasks that exceed timeout
    pub async fn cancel_pending(&self, queue: &Queue, min_pending_ms: u64) -> Result<usize> {
        self.broker.cancel_pending(queue, min_pending_ms).await
    }

    /// 清理空闲超时的主题
    /// Clean up idle expired topics
    pub async fn clean_topics(&self, min_idle_ms: u64) -> Result<usize> {
        self.refresh_queues().await?;
        let topic_queues_list = self.queues.load();

        let now = chrono::Local::now().timestamp_millis() as u64;
        let mut n = 0;
        for topic_queues in topic_queues_list.iter() {
            if topic_queues.topic.last_insert_at_ms < now - min_idle_ms {
                self.broker.delete_topic(&topic_queues.topic.topic).await?;
                n += 1;
            }
        }
        Ok(n)
    }

    pub fn start(self, config: ClaimConfig) -> BackgroundHandle {
        let claimer = Arc::new(self);

        let mut handlers = BackgroundHandle { handles: vec![] };
        if config.refresh_queues_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_refresh_queues(config.refresh_queues_interval_sec)
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.scheduled_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_scheduled_claim(config.scheduled_claim_interval_sec)
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.dependent_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_dependent_claim(config.dependent_claim_interval_sec)
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.deadline_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_deadline_claim(config.deadline_claim_interval_sec)
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.cancel_pending_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_cancel_pending_claim(
                        config.cancel_pending_claim_interval_sec,
                        config.cancel_pending_min_sec,
                    )
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.retention_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_retention_claim(config.retention_claim_interval_sec)
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.timeout_claim_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_timeout_claim(
                        config.timeout_claim_interval_sec,
                        config.timeout_min_idle_sec,
                    )
                    .await;
            });
            handlers.handles.push(handler);
        }
        if config.clean_topics_interval_sec > 0 {
            let claimer = claimer.clone();
            let handler = tokio::spawn(async move {
                claimer
                    .run_clean_topics(
                        config.clean_topics_interval_sec,
                        config.clean_topics_min_idle_sec,
                    )
                    .await;
            });
            handlers.handles.push(handler);
        }
        handlers
    }

    /// 定时刷新队列列表
    /// Run refresh queues periodically
    pub async fn run_refresh_queues(&self, interval_sec: u64) {
        loop {
            match self.refresh_queues().await {
                Ok(n) => debug!("refresh queues success, {} queues", n),
                Err(e) => warn!("refresh queues failed: {}", e),
            }
            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    /// 定时运行 claim `scheduled`
    /// Run claim `scheduled` periodically
    pub async fn run_scheduled_claim(&self, interval_sec: u64) {
        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.claim_scheduled(queue.as_ref()).await {
                        Ok(n) => debug!("claim scheduled tasks: {}", n),
                        Err(e) => warn!("claim scheduled tasks failed: {}", e),
                    }
                }

                tokio::time::sleep(Duration::from_secs(interval_sec)).await;
            }
        }
    }

    /// 定时运行 claim `dependent`
    /// Run claim `dependent` periodically
    pub async fn run_dependent_claim(&self, interval_sec: u64) {
        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.claim_dependent(queue.as_ref()).await {
                        Ok((pending, canceled)) => debug!(
                            "claim dependent tasks: pending - {}, canceled - {}",
                            pending, canceled
                        ),
                        Err(e) => warn!("claim dependent tasks failed: {}", e),
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    /// 定时运行 claim `pending` , 取消`pending`超过`min_pending_sec`的任务
    /// Run claim `cancel_pending` periodically, cancel `pending` tasks that exceed `min_pending_sec`
    pub async fn run_cancel_pending_claim(&self, interval_sec: u64, min_pending_sec: u64) {
        let min_pending_ms = min_pending_sec * 1000;
        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.cancel_pending(queue.as_ref(), min_pending_ms).await {
                        Ok(n) => debug!("cancel pending tasks: {}", n),
                        Err(e) => warn!("cancel pending tasks failed: {}", e),
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    /// 定时运行 claim `deadline`
    /// Run claim `deadline` periodically
    pub async fn run_deadline_claim(&self, interval_sec: u64) {
        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.claim_deadline(queue.as_ref()).await {
                        Ok(n) => debug!("claim deadline tasks: {}", n),
                        Err(e) => warn!("claim deadline tasks failed: {}", e),
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    /// 定时运行 claim `timeout`
    /// Run claim `timeout` periodically
    pub async fn run_timeout_claim(&self, interval_sec: u64, min_idle_sec: u64) {
        let min_idle_ms = min_idle_sec * 1000;

        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.claim_timeout(queue.as_ref(), min_idle_ms).await {
                        Ok((pending, canceled)) => debug!(
                            "claim timeout tasks: pending - {}, canceled - {}",
                            pending, canceled
                        ),
                        Err(e) => warn!("claim timeout tasks failed: {}", e),
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    /// 定时运行 claim `retention`
    /// Run claim `retention` periodically
    pub async fn run_retention_claim(&self, interval_sec: u64) {
        loop {
            let topic_queues = self.queues.load();

            for topic_queue in topic_queues.as_ref() {
                for queue in &topic_queue.queues {
                    match self.claim_retention(queue.as_ref()).await {
                        Ok(n) => debug!("claim retention tasks: {}", n),
                        Err(e) => warn!("claim retention tasks failed: {}", e),
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }

    pub async fn run_clean_topics(&self, interval_sec: u64, min_idle_sec: u64) {
        let min_idle_ms = min_idle_sec * 1000;
        loop {
            match self.clean_topics(min_idle_ms).await {
                Ok(n) => debug!("clean topics: {}", n),
                Err(e) => warn!("clean topics failed: {}", e),
            }

            tokio::time::sleep(Duration::from_secs(interval_sec)).await;
        }
    }
}

impl Default for ClaimConfig {
    fn default() -> Self {
        Self {
            scheduled_claim_interval_sec: 5,
            dependent_claim_interval_sec: 5,
            timeout_claim_interval_sec: 5,
            timeout_min_idle_sec: 5,
            deadline_claim_interval_sec: 60,
            retention_claim_interval_sec: 60,
            refresh_queues_interval_sec: 60,

            cancel_pending_min_sec: 0,
            cancel_pending_claim_interval_sec: 0,
            clean_topics_interval_sec: 0,
            clean_topics_min_idle_sec: 0,
        }
    }
}

/// 后台任务句柄，用于控制后台自动 claim 任务
/// Background task handle for controlling auto claim tasks
pub struct BackgroundHandle {
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundHandle {
    /// 停止所有后台任务
    /// Stop all background tasks
    pub fn stop(self) {
        for handle in self.handles {
            handle.abort();
        }
    }

    /// 等待所有后台任务完成
    /// Wait for all background tasks to complete
    pub async fn wait(self) {
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}
