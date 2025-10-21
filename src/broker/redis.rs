use crate::{
    broker::Broker,
    errors::Result,
    task::{Task, TaskState},
};

pub struct RedisBroker {}

#[async_trait::async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, task: Task) -> Result<()> {
        match task.runtime.state {
            TaskState::Pending => {
                todo!()
            }
            TaskState::Scheduled => {
                todo!()
            }
            _ => {}
        }
        if !matches!(
            task.runtime.state,
            TaskState::Pending | TaskState::Scheduled
        ) {
            panic!("");
        }
        todo!()
    }

    async fn try_dequeue<I, S>(&self, topics: I) -> Result<Option<Task>>
    where
        I: IntoIterator<Item = S> + Send + Sync,
        S: AsRef<str>,
    {
        todo!()
    }
}
