use crate::{
    errors::Result,
    model::{Queue, QueueInfo, Stats, TopicInfo, TopicQueuesInfo},
    task::Task,
};

pub mod redis;

#[async_trait::async_trait]
pub trait Broker {
    /// 添加任务到消息队列中, 会自动根据任务状态选择合适的队列, 只支持`pending`, `scheduled`, `dependent`三种状态
    /// `scheduled` -> 定时任务,会在指定的时间到达后正式进入消息队列
    /// `dependent` -> 依赖任务,会在指定的依赖任务完成后再正式进入消息队列
    /// `pending` -> 普通任务,会直接进入消息队列
    ///
    /// Add task to message queue, will automatically select the appropriate queue based on the task state,
    /// only supports `pending`, `scheduled`, `dependent` three states.
    /// `scheduled` -> scheduled task, will be officially entered into the message queue after the specified time arrived.
    /// `dependent` -> dependent task, will be officially entered into the message queue after the specified dependent tasks has completed.
    /// `pending` -> pending task, will be officially entered into the message queue directly.
    async fn add_task(&self, task: &Task) -> Result<()>;

    /// 从指定的消息队列列表中取出一个任务, 如果没有任务则返回None
    ///
    /// Dequeue a task from the specified message queue list, if there is no task then return None.
    async fn dequeue(&self, queue_list: &[Queue]) -> Result<Option<Task>>;

    /// 标记任务成功
    ///
    /// Mark the task as success.
    async fn succeed(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        result: Option<&[u8]>,
    ) -> Result<()>;

    /// 标记任务失败, 若任务配置了重试, 会自动进行重试
    ///
    /// Mark the task as failed. If the task has retry configured, it will automatically retry.
    async fn fail(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        err_msg: &str,
    ) -> Result<()>;

    /// 标记任务取消
    ///
    /// Mark the task as canceled.
    async fn cancel(
        &self,
        queue: &Queue,
        task_id: &str,
        stream_id: &str,
        err_msg: Option<&str>,
    ) -> Result<()>;

    /// 认领到期的定时任务,放入可以直接被消费的`stream`消息队列.
    /// 返回认领的个数.
    ///
    /// Claim the due date of the scheduled task and add it to the `stream` message queue.
    /// Returns the number of items claimed.
    async fn claim_scheduled(&self, queue: &Queue) -> Result<usize>;

    /// 认领满足依赖条件的依赖任务,放入可以直接被消费的`stream`消息队列, 或者依赖条件永远无法满足,取消该任务.
    /// 返回满足依赖条件的个数和被取消的任务
    ///
    /// Claim the dependent task that satisfies the dependency condition and add it to the `stream` message queue,
    /// or cancel the task if the dependency condition can never be satisfied.
    /// Returns the number of items claimed and the number of items canceled.
    async fn claim_dependent(&self, queue: &Queue) -> Result<(usize, usize)>;

    /// 认领到期的死信任务. 将其标记为取消. 返回认领的个数
    ///
    /// Claim the due date of the dead letter task and mark it as canceled. Returns the number of items claimed.
    async fn claim_deadline(&self, queue: &Queue) -> Result<usize>;

    /// 认领执行超时的任务. 根据是否配置了重试, 将其标记为取消或者重新入队. 返回重试的个数和取消的个数
    ///
    /// Claim the execution timeout task. According to whether retry is configured, mark it as canceled or re-queue.
    /// Returns the number of retried tasks and the number of canceled tasks.
    async fn claim_timeout(&self, queue: &Queue, min_idle_ms: u64) -> Result<(usize, usize)>;

    /// 认领保留到期的任务. 将保留过期的任务删除. 返回删除的个数
    ///
    /// Claim the retention expired task. Delete the retention expired task. Returns the number of items deleted.
    async fn claim_retention(&self, queue: &Queue) -> Result<usize>;

    /// 取消`pending`状态下等待时间超过指定 `min_pending_ms` 的任务. 返回取消的个数
    ///
    /// Cancel the `pending` state task that has been waiting for more than the specified `min_pending_ms`.
    /// Returns the number of items canceled.
    async fn cancel_pending(&self, queue: &Queue, min_pending_ms: u64) -> Result<usize>;

    /// 获取队列的统计信息
    ///
    /// Get the queue statistics.
    async fn queue_stats(&self, queue: &Queue) -> Result<Stats>;

    /// 获取主题的统计信息
    ///
    /// Get the topic statistics.
    async fn topic_stats(&self, topic: &str) -> Result<Stats>;

    /// 查看指定任务的详细信息
    ///
    /// View the detailed information of the specified task.
    async fn peek_task(&self, queue: &Queue, task_id: &str) -> Result<Task>;

    /// 获取主题列表
    ///
    /// Get the topic list.
    async fn topics(&self) -> Result<Vec<TopicInfo>>;

    /// 获取指定主题下的队列列表
    ///
    /// Get the queue list under the specified topic.
    async fn queues(&self, topic: &str) -> Result<Vec<QueueInfo>>;

    /// 获取所有队列列表
    ///
    /// Get the list of all queues.
    async fn all_queues(&self) -> Result<Vec<TopicQueuesInfo>>;

    /// 删除指定主题
    ///
    /// Delete the specified topic.
    async fn delete_topic(&self, topic: &str) -> Result<()>;

    /// 删除指定队列
    ///
    /// Delete the specified queue.
    async fn delete_queue(&self, queue: &Queue) -> Result<()>;
}
