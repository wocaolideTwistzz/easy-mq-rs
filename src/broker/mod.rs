use crate::{errors::Result, task::Task};

pub mod redis;

#[async_trait::async_trait]
pub trait Broker {
    /// 将任务放入队列，会自动根据当前State选择将任务放入`Pending`(可以直接被消费)
    /// 或者进入`Scheduled`(会在依赖条件(某个时间或某些任务的完成)满足后被消费)
    /// 任务状态必须是 `Pending` 或者 `Scheduled`.
    ///
    /// Putting a task into the queue will automatically select the task to be put into
    /// `Pending` (can be consumed directly) or `Scheduled` (will be consumed after the
    /// dependent conditions(a certain time or completion of certain tasks) are met)
    /// according to the current state.
    /// The task state must be `Pending` or `Scheduled`.
    async fn enqueue(&self, task: Task) -> Result<()>;

    /// 尝试从指定话题/话题列表中取出任务，若队列中存在任务，直接返回优先级最高的任务，若不存在，直接返回None.
    ///
    /// Try to dequeue task from the specified topic/topic list. Return the task with highest
    /// priority, return None if there is no task in topics.
    async fn try_dequeue<I, S>(&self, topics: I) -> Result<Option<Task>>
    where
        I: IntoIterator<Item = S> + Send + Sync,
        S: AsRef<str>;
}

fn xd() {}
