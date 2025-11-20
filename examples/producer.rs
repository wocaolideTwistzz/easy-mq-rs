use std::env;

use easy_mq_rs::{
    instance::Instance,
    task::{ScheduledAt, Task, TaskCompletedState},
};

#[path = "common/mod.rs"]
mod common;

use common::topics;
use inquire::{Select, Text};

use crate::common::payloads::Email;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let instance = Instance::from_redis_url(&redis_url).unwrap();

    println!("Connected to Redis broker: {redis_url}");

    loop {
        match Select::new("Please select your action: ", Actions::all()).prompt() {
            Ok(Actions::SendAll) => {
                println!(
                    "========================================================================================================="
                );
                let task = Task::try_new(topics::SEND_EMAIL, Email::random()).unwrap();
                instance.add_task(&task).await.unwrap();
                println!("Queued `topic` [{}] with `id` [{}]", task.topic, task.id);

                let priority_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_priority(-1);
                instance.add_task(&priority_task).await.unwrap();
                println!(
                    "Queued `topic` [{}] with `id` [{}] with `priority` [{}]",
                    priority_task.topic,
                    priority_task.id,
                    priority_task.options().priority
                );

                let retry_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_retry(5, 1000);
                instance.add_task(&retry_task).await.unwrap();
                println!(
                    "Queued `topic` [{}] with `id` [{}] with `max_retries` [{}] and `interval_ms` [{}]",
                    retry_task.topic,
                    retry_task.id,
                    retry_task.options().retry.as_ref().unwrap().max_retries,
                    retry_task.options().retry.as_ref().unwrap().interval_ms
                );

                let timeout_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_retry(5, 1000)
                    .with_timeout(5000);
                instance.add_task(&timeout_task).await.unwrap();
                println!(
                    "Queued `topic` [{}] with `id` [{}] with `max_retries` [{}] and `interval_ms` [{}] and `timeout_ms` [{}]",
                    timeout_task.topic,
                    timeout_task.id,
                    timeout_task.options().retry.as_ref().unwrap().max_retries,
                    timeout_task.options().retry.as_ref().unwrap().interval_ms,
                    timeout_task.options().timeout_ms.as_ref().unwrap()
                );

                let deadline_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_retry(5, 1000)
                    .with_deadline(chrono::Local::now().timestamp_millis() as u64 + 1000);
                instance.add_task(&deadline_task).await.unwrap();
                println!(
                    "Queued `topic` [{}] with `id` [{}] with `max_retries` [{}] and `interval_ms` [{}] and `deadline_ms` [{}]",
                    deadline_task.topic,
                    deadline_task.id,
                    deadline_task.options().retry.as_ref().unwrap().max_retries,
                    deadline_task.options().retry.as_ref().unwrap().interval_ms,
                    deadline_task.options().deadline_ms.as_ref().unwrap()
                );

                let dependent_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_scheduled(ScheduledAt::DependsOn(vec![
                        task.to_completed_task(TaskCompletedState::Any),
                        retry_task.to_completed_task(TaskCompletedState::Succeed),
                    ]));
                instance.add_task(&dependent_task).await.unwrap();

                let mut dep_str = format!(
                    "Queued `topic` [{}] with `id` [{}] with `dependent_task`\n",
                    dependent_task.topic, dependent_task.id
                );
                dependent_task
                    .options()
                    .scheduled_at
                    .as_ref()
                    .unwrap()
                    .depends_on()
                    .unwrap()
                    .iter()
                    .for_each(|t| {
                        dep_str.push_str(&format!(
                            "\twith dependent task [{}]-[{}]-[{}]\n",
                            t.topic, t.id, t.state
                        ));
                    });
                dep_str.pop();
                println!("{}", dep_str);

                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::SendEmail) => {
                let task = Task::try_new(topics::SEND_EMAIL, Email::random()).unwrap();
                instance.add_task(&task).await.unwrap();
                println!(
                    "========================================================================================================="
                );
                println!("Queued `topic` [{}] with `id` [{}]", task.topic, task.id);
                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::SendDependentEmail) => {
                let task = Task::try_new(topics::SEND_EMAIL, Email::random()).unwrap();
                let depend_on = task.to_completed_task(TaskCompletedState::Any);
                let dependent_task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_scheduled(ScheduledAt::DependsOn(vec![depend_on.clone()]));

                instance.add_task(&task).await.unwrap();
                instance.add_task(&dependent_task).await.unwrap();
                println!(
                    "========================================================================================================="
                );
                println!("Queued `topic` [{}] with `id` [{}]", task.topic, task.id);
                println!(
                    "Queued `topic` [{}] with `id` [{}] with `dependent_task` [{}]-[{}]-[{}]",
                    dependent_task.topic,
                    dependent_task.id,
                    depend_on.topic,
                    depend_on.id,
                    depend_on.state
                );
                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::SendDelayEmail) => {
                let scheduled_at = chrono::Local::now().timestamp_millis() as u64 + 10_000;
                let task = Task::try_new(topics::SEND_EMAIL, Email::random())
                    .unwrap()
                    .with_scheduled(ScheduledAt::TimestampMs(scheduled_at));

                instance.add_task(&task).await.unwrap();
                println!(
                    "========================================================================================================="
                );
                println!(
                    "Queued `topic` [{}] with `id` [{}] `scheduled_at`: [{}] ",
                    task.topic, task.id, scheduled_at
                );
                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::SendAdvancedEmail) => {
                let receiver = Text::new("Send to: ").prompt().unwrap();
                let message = Text::new("Message: ").prompt().unwrap();
                let mut task =
                    Task::try_new(topics::SEND_EMAIL, Email::new(receiver, message)).unwrap();
                let priority =
                    Select::new("Set priority: ", vec!["High (-1)", "Medium (0)", "Low (1)"])
                        .with_starting_cursor(1)
                        .prompt()
                        .unwrap();
                task = task.with_priority(match priority {
                    "High (-1)" => -1,
                    "Medium (0)" => 0,
                    "Low (1)" => 1,
                    _ => 0,
                });
                let retry_enable = Select::new("Enable retry: ", vec!["Yes", "No"])
                    .with_starting_cursor(1)
                    .prompt()
                    .unwrap();
                if retry_enable == "Yes" {
                    let retry_count = Text::new("Retry count: ").prompt().unwrap();
                    let retry_interval = Text::new("Retry interval (ms): ").prompt().unwrap();
                    task = task.with_retry(
                        retry_count.parse().unwrap(),
                        retry_interval.parse().unwrap(),
                    );
                }

                let timeout_enable = Select::new("Enable timeout: ", vec!["Yes", "No"])
                    .with_starting_cursor(1)
                    .prompt()
                    .unwrap();
                if timeout_enable == "Yes" {
                    let timeout = Text::new("Timeout (ms): ").prompt().unwrap();
                    task = task.with_timeout(timeout.parse().unwrap());
                }

                let deadline_enable = Select::new("Enable deadline: ", vec!["Yes", "No"])
                    .with_starting_cursor(1)
                    .prompt()
                    .unwrap();
                if deadline_enable == "Yes" {
                    let deadline = Text::new("Deadline after (ms): ").prompt().unwrap();
                    task = task.with_deadline(
                        chrono::Local::now().timestamp_millis() as u64
                            + deadline.parse::<u64>().unwrap(),
                    );
                }

                let slot = Text::new("Set slot: ").prompt().unwrap();
                if !slot.trim().is_empty() {
                    task = task.with_slot(slot.trim());
                }

                let scheduled_at = Select::new("Set scheduled at: ", vec!["Now", "Later"])
                    .prompt()
                    .unwrap();
                if scheduled_at == "Later" {
                    let scheduled_at = Text::new("Scheduled after (ms): ").prompt().unwrap();
                    task = task.with_scheduled(ScheduledAt::TimestampMs(
                        chrono::Local::now().timestamp_millis() as u64
                            + scheduled_at.parse::<u64>().unwrap(),
                    ));
                }

                let retention = Text::new("Set retention (ms): ").prompt().unwrap();
                if !retention.trim().is_empty() {
                    task = task.with_retention(retention.parse::<u64>().unwrap());
                }

                instance.add_task(&task).await.unwrap();

                println!(
                    "========================================================================================================="
                );
                println!(
                    "Queued `topic` [{}] with `id` [{}], `priority` [{}]",
                    task.topic,
                    task.id,
                    task.options().priority
                );
                if let Some(retry) = task.options().retry.as_ref() {
                    println!(
                        "With `max_retries` [{}], `retry_interval_ms` [{}],",
                        retry.max_retries, retry.interval_ms
                    );
                }
                if let Some(timeout) = task.options().timeout_ms {
                    println!("With `timeout_ms` [{}]", timeout);
                }
                if let Some(deadline) = task.options().deadline_ms {
                    println!("With `deadline_ms` [{}]", deadline);
                }
                if let Some(ScheduledAt::TimestampMs(scheduled_at)) = task.options().scheduled_at {
                    println!("With `scheduled_at` [{}]", scheduled_at);
                }
                println!("With `retention_ms` [{}]", task.options().retention_ms);
                if let Some(slot) = task.options().slot.as_ref() {
                    println!("With `slot` [{}]", slot);
                }
                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::ReceiveEmail) => {
                let task = Task::try_new(topics::RECEIVE_EMAIL, Email::random()).unwrap();
                instance.add_task(&task).await.unwrap();

                println!(
                    "========================================================================================================="
                );
                println!("Queued `topic` [{}] with `id` [{}]", task.topic, task.id);
                println!(
                    "========================================================================================================="
                );
            }
            Ok(Actions::ReceiveEmptyEmail) => {
                let task = Task::new(topics::RECEIVE_EMAIL, None);
                instance.add_task(&task).await.unwrap();

                println!(
                    "========================================================================================================="
                );
                println!("Queued `topic` [{}] with `id` [{}]", task.topic, task.id);
                println!(
                    "========================================================================================================="
                );
            }
            Err(e) => {
                println!("User interrupt: {e}");
                break;
            }
        }
    }
}

#[derive(Debug)]
pub enum Actions {
    SendAll,
    SendEmail,
    SendDelayEmail,
    SendDependentEmail,
    SendAdvancedEmail,
    ReceiveEmail,
    ReceiveEmptyEmail,
}

impl Actions {
    pub fn all() -> Vec<Actions> {
        vec![
            Actions::SendAll,
            Actions::SendEmail,
            Actions::SendDelayEmail,
            Actions::SendDependentEmail,
            Actions::SendAdvancedEmail,
            Actions::ReceiveEmail,
            Actions::ReceiveEmptyEmail,
        ]
    }
}

impl std::fmt::Display for Actions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Actions::SendAll => "Send all emails (with different options)",
                Actions::SendEmail => "Send email",
                Actions::SendDelayEmail => "Send delay email (after 10 seconds)",
                Actions::SendDependentEmail => "Send two emails, one depends on the other",
                Actions::SendAdvancedEmail => "Send advanced email",
                Actions::ReceiveEmail => "Receive email",
                Actions::ReceiveEmptyEmail =>
                    "Receive empty email (will be failed in consumer side)",
            }
        )
    }
}
