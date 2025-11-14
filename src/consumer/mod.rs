pub mod config;
pub mod handler;

use std::{sync::Arc, time::Duration};

use tracing::{Level, instrument};

use crate::{
    broker::{Broker, redis::RedisBroker},
    claimer::{ClaimConfig, Claimer},
    consumer::{
        config::{ConsumerConfig, RefreshQueuesStrategy},
        handler::Handler,
    },
    errors::{Error, Result},
    instance::Instance,
    model::Queue,
    task::Task,
};

#[derive(Debug)]
pub struct Consumer<B: Broker> {
    instance: Instance<B>,

    claimer: Arc<Claimer<B>>,

    config: ConsumerConfig,
}

impl Consumer<RedisBroker> {
    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_url(url: &str, config: ConsumerConfig) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_url(url)?);
        Ok(Self::new(broker, config))
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_urls<T: Into<Vec<String>>>(urls: T, config: ConsumerConfig) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_urls(urls)?);
        Ok(Self::new(broker, config))
    }

    #[cfg(not(feature = "cluster"))]
    pub fn from_redis_config(config: deadpool_redis::Config, consumer_config: ConsumerConfig) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self::new(broker, consumer_config))
    }

    #[cfg(feature = "cluster")]
    pub fn from_redis_cluster_config(config: deadpool_redis::cluster::Config, consumer_config: ConsumerConfig) -> Result<Self> {
        let broker = Arc::new(RedisBroker::from_config(config)?);
        Ok(Self::new(broker, consumer_config))
    }
}

impl<B> Consumer<B>
where
    B: Broker + Send + Sync + 'static,
{
    pub fn new(broker: Arc<B>, config: ConsumerConfig) -> Self {
        let instance = Instance::new(broker.clone());
        let claimer = Arc::new(Claimer::new(broker));

        if let RefreshQueuesStrategy::FixedQueues(queues) = &config.refresh_queues_strategy {
            claimer.set_queues(queues.clone());
        }
        
        Self {
            instance,
            claimer,
            config,
        }
    }

    pub async fn run<H>(mut self, handler: H)
    where
        H: Handler + Send + Sync + 'static,
    {
        let claimer = self.claimer.clone();

        let handler = Arc::new(handler);

        claimer.start(ClaimConfig::from(&self.config));

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));

        loop {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            match self.try_dequeue().await {
                Ok(Some(task)) => {
                    let handler = handler.clone();
                    let instance = self.instance.clone();
  
                    tokio::spawn(Consumer::process_task(task, handler, instance, permit));

                    self.config.dequeue_backoff_strategy.reset();
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(message = "try dequeue failed", error=?e)
                }
            }
            let backoff = Duration::from_secs(self.config.dequeue_backoff_strategy.backoff());
            tokio::time::sleep(backoff).await
        }
    }

    async fn try_dequeue(&self) -> Result<Option<Task>> {
        match &self.config.refresh_queues_strategy {
            RefreshQueuesStrategy::AllOnEachDequeue => {
                _ = self.claimer.refresh_topic_queues().await?;
            }
            RefreshQueuesStrategy::SpecifiedTopicOnEachDequeue(topics) => {
                _ = self.claimer.refresh_topic_queues_by_topics(topics).await?;
            }
            _ => {}
        }

        if let RefreshQueuesStrategy::FixedQueues(queues) = &self.config.refresh_queues_strategy {
            self.instance.try_dequeue(queues).await
        } else {
            self.instance
                .try_dequeue(self.claimer.get_queues().as_ref())
                .await
        }
    }

    #[instrument(
        level = Level::DEBUG,
        skip(task, handler, instance, _permit),
        fields(
            topic = task.topic, 
            priority = task.options.priority,
            task_id = task.id,
            stream_id = task.stream_id(),
        ),
    )]
    async fn process_task<H>(
        task: Task,
        handler: Arc<H>,
        instance: Instance<B>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) where
        H: Handler + Send + Sync + 'static,
    {
        let stream_id = match task.stream_id() {
            Some(stream_id) => stream_id,
            None => {
                tracing::error!("dequeue task without stream_id. that should not happen!!!");
                return;
            }
        };

        let queue = Queue::from(&task);

        let ret = match task.current_deadline() {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), handler.process_task(&task))
                .await
                .map_err(|_| Error::ProcessTaskTimeout),
            None => Ok(handler.process_task(&task).await),
        }
        .flatten();

        for _ in 0..10 {
            match match ret.as_ref() {
                Ok(ret) => {
                    instance
                        .succeed(&queue, &task.id, stream_id, ret.as_deref())
                        .await
                }
                Err(e) => {
                    instance
                        .fail(&queue, &task.id, stream_id, e.to_string().as_str())
                        .await
                }
            } {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    tracing::warn!(message = "failed to mark task as done", error=?e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}
