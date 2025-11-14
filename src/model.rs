use crate::{
    rdb::constant::QName,
    task::{Task, TaskOptions},
};

/// 队列/话题 的统计数据
/// Queue/Topic 's stats
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Stats {
    /// 处于`pending`状态下的任务数 - 进入等待队列,但还未开始执行
    /// The number of task that in `pending` state. - Enter the waiting queue, but have not started execution.
    pub pending_count: usize,

    /// 处于`active`状态下的任务数 - 正在执行
    /// The number of task that in `active` state. - Currently executing.
    pub active_count: usize,

    /// 处于`scheduled`状态下的任务数 - 等到指定时间后才会进入等待队列
    /// The number of task that in `scheduled` state. - Enter the waiting queue only after the specified time.
    pub scheduled_count: usize,

    /// 处于`dependent`状态下的任务数 - 等待依赖任务完成后才会进入等待队列
    /// The number of task that in `dependent` state. - Enter the waiting queue only after the dependent task is completed.
    pub dependent_count: usize,

    /// 处于`completed`状态下的任务数 - 已经完成
    /// The number of task that in `completed` state. - Already completed.
    pub completed_count: usize,
}

/// 任务队列
/// Task queue
#[derive(Debug, Clone, PartialEq)]
pub struct Queue {
    /// 话题
    /// Topic
    pub topic: String,

    /// 槽位
    /// Slot
    pub slot: Option<String>,

    /// 优先级
    /// Priority
    pub priority: i8,
}

/// 话题信息
/// Topic info
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// 话题名称
    /// Topic name
    pub topic: String,

    /// 话题最后产生任务的时间 (单位: 毫秒)
    /// The last time a task was generated in the topic (unit: milliseconds)
    pub last_insert_at_ms: u64,
}

/// 队列信息
/// Queue info
#[derive(Debug, Clone)]
pub struct QueueInfo {
    /// 队列
    /// Queue
    pub queue: Queue,

    /// 话题最后产生任务的时间 (单位: 毫秒)
    /// The last time a task was generated in the topic (unit: milliseconds)
    pub last_insert_at_ms: u64,
}

/// 话题队列信息
/// Topic queues info
#[derive(Debug, Clone)]
pub struct TopicQueuesInfo {
    /// 话题信息
    /// Topic info
    pub topic: TopicInfo,

    /// 队列信息
    /// Queue info
    pub queues: Vec<QueueInfo>,
}

impl From<&Task> for Queue {
    fn from(value: &Task) -> Self {
        Self {
            topic: value.topic.clone(),
            slot: value.options.slot.clone(),
            priority: value.options.priority,
        }
    }
}

impl QueueInfo {
    pub fn from_queue(queue: Queue, last_insert_at_ms: u64) -> Self {
        Self {
            queue,
            last_insert_at_ms,
        }
    }
}

impl Queue {
    pub fn new(topic: impl AsRef<str>, priority: i8) -> Self {
        Self {
            topic: topic.as_ref().to_string(),
            slot: None,
            priority,
        }
    }

    pub fn new_with_slot(slot: impl AsRef<str>, topic: impl AsRef<str>, priority: i8) -> Self {
        Self {
            topic: topic.as_ref().to_string(),
            slot: Some(slot.as_ref().to_string()),
            priority,
        }
    }

    pub fn generate_task(&self, payload: Option<Vec<u8>>) -> Task {
        let mut task = Task::new(self.topic.clone(), payload).with_priority(self.priority);
        if let Some(slot) = self.slot.as_ref() {
            task = task.with_slot(slot.clone());
        }
        task
    }

    pub fn generate_task_with_options(
        &self,
        payload: Option<Vec<u8>>,
        options: TaskOptions,
    ) -> Task {
        let mut task = Task::new_with_options(self.topic.clone(), payload, options)
            .with_priority(self.priority);
        if let Some(slot) = self.slot.as_ref() {
            task = task.with_slot(slot.clone());
        }
        task
    }
}

impl AsRef<Queue> for QueueInfo {
    fn as_ref(&self) -> &Queue {
        &self.queue
    }
}

impl AsRef<TopicInfo> for TopicQueuesInfo {
    fn as_ref(&self) -> &TopicInfo {
        &self.topic
    }
}

impl AsRef<Vec<QueueInfo>> for TopicQueuesInfo {
    fn as_ref(&self) -> &Vec<QueueInfo> {
        &self.queues
    }
}

impl std::fmt::Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        QName::from(self).fmt(f)
    }
}
