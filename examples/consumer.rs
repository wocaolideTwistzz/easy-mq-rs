use std::env;

use bincode::config;
use easy_mq_rs::{
    consumer::{Consumer, config::ConsumerConfig, handler::Handler},
    errors::{Error, Result},
    task::Task,
};

use crate::common::payloads::Email;

#[path = "common/mod.rs"]
mod common;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let consumer = Consumer::from_redis_url(&redis_url, ConsumerConfig::default()).unwrap();

    let instance = consumer.clone_instance();

    tokio::spawn(async move {
        loop {
            let topic_queues = instance.all_queues().await.unwrap();

            for tqs in topic_queues {
                for queue in tqs.queues {
                    let queue = queue.as_ref();
                    let stats = instance.queue_stats(queue).await.unwrap();

                    println!(
                        "> [{}]-[{}] pending: {}, active: {}, scheduled: {}, dependent: {}, completed: {}",
                        queue.topic,
                        queue.priority,
                        stats.pending_count,
                        stats.active_count,
                        stats.scheduled_count,
                        stats.dependent_count,
                        stats.completed_count
                    );
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });

    println!("Connected to Redis broker: {redis_url}");

    consumer.run(EmailHandler).await
}

pub struct EmailHandler;

#[async_trait::async_trait]
impl Handler for EmailHandler {
    async fn process_task(&self, task: &Task) -> Result<Option<Vec<u8>>> {
        let start = std::time::Instant::now();

        println!("[{}]-[{}] start processing", task.topic, task.id);

        let ret = match task.topic.as_str() {
            common::topics::SEND_EMAIL => self.handle_send_email(task).await,
            common::topics::RECEIVE_EMAIL => self.handle_receive_email(task).await,
            _ => Err(Error::custom("Unknown topic")),
        };
        match ret.as_ref() {
            Ok(_) => {
                println!(
                    "[{}]-[{}] processed in {}ms",
                    task.topic,
                    task.id,
                    start.elapsed().as_millis()
                );
            }
            Err(e) => {
                println!(
                    "[{}]-[{}] failed in {}ms: {}",
                    task.topic,
                    task.id,
                    start.elapsed().as_millis(),
                    e
                );
            }
        }
        ret
    }
}
impl Default for EmailHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl EmailHandler {
    pub fn new() -> Self {
        Self
    }

    async fn handle_send_email(&self, task: &Task) -> Result<Option<Vec<u8>>> {
        if task.payload.is_none() {
            return Err(Error::custom("Empty email!"));
        }

        let (email, _): (Email, _) =
            bincode::serde::decode_from_slice(task.payload.as_ref().unwrap(), config::standard())?;

        println!(
            "[{}]-[{}] send email to {}",
            task.topic, task.id, email.send_to
        );

        self.maybe_error()?;
        self.maybe_wait().await;

        Ok(None)
    }

    async fn handle_receive_email(&self, task: &Task) -> Result<Option<Vec<u8>>> {
        if task.payload.is_none() {
            return Err(Error::custom("Empty email!"));
        }

        let (email, _): (Email, _) =
            bincode::serde::decode_from_slice(task.payload.as_ref().unwrap(), config::standard())?;

        println!(
            "[{}]-[{}] receive email from {}",
            task.topic, task.id, email.send_to
        );

        self.maybe_error()?;
        self.maybe_wait().await;

        Ok(Some("Receive data".as_bytes().to_vec()))
    }

    fn maybe_error(&self) -> Result<()> {
        match rand::random_bool(0.5) {
            true => Err(Error::custom("Something went wrong!")),
            false => Ok(()),
        }
    }

    async fn maybe_wait(&self) {
        let wait_sec = rand::random_range(0..30);
        tokio::time::sleep(std::time::Duration::from_secs(wait_sec)).await;
    }
}
