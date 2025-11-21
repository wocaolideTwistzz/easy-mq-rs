use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use bincode::config::{self};
use serde::{Deserialize, Serialize};

use crate::errors::Result;

/// 定义任务结构体
/// Define the Task struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    /// 任务主题，用于标识任务的类别或分组
    /// Task topic, used to identify the category or group of the task
    pub topic: String,

    /// 任务的唯一标识符
    /// Unique identifier for the task
    pub id: String,

    /// 任务负载，包含任务的具体数据
    /// Task payload, contains the actual data of the task
    pub payload: Option<Vec<u8>>,

    /// 任务选项，包含任务的配置信息，如优先级、重试策略等
    /// Task options, contains configuration information such as priority, retry strategy, etc.
    pub(crate) options: TaskOptions,

    /// 任务运行时数据，包含当前状态，已重试次数，执行结果等
    /// Task runtimes, contains current state, retried, results, etc.
    pub(crate) runtime: TaskRuntime,
}

/// 定义任务选项结构体，包含任务的配置参数
/// Define the TaskOptions struct, contains configuration parameters for the task
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskOptions {
    /// 任务优先级，值越小优先级越高 默认为0, 可以使用负数来表达更高的优先级
    /// Task priority, lower value indicates higher priority.
    /// The default value is 0，and negative number can be used to express higher priority.
    pub priority: i8,

    /// 任务保留时间（单位：毫秒），用于指定任务在完成后保留的时间 默认保留7天
    /// Task retention time (in milliseconds), specifies how long the task stays after completed.
    /// The default retention period is 7 days.
    pub retention_ms: u64,

    /// 重试策略，可选，定义任务失败后的重试行为 默认不做重试
    /// Retry strategy, optional, defines the behavior for retrying failed tasks
    /// Default is not retry
    pub retry: Option<Retry>,

    /// 任务超时时间（单位：毫秒），可选，指定任务执行的最大时长 默认不设置超时时间
    /// Task timeout (in milliseconds), optional, specifies the maximum duration for task execution.
    /// No timeout is set by default
    pub timeout_ms: Option<u64>,

    /// 任务截止时间（单位： 毫秒级时间戳），可选，指定任务必须完成的时间点 默认不设置截止时间
    /// Task deadline (in milliseconds), optional, specifies the time by which the task must be completed
    /// No deadline is set by default
    pub deadline_ms: Option<u64>,

    /// 任务在 dependent 队列中等待依赖的截止时间，可选，默认不设置
    /// Task dependent deadline (in milliseconds), optional, specifies the time by which the task waiting for
    /// dependencies in `dependent` queue.
    /// No dependent deadline is set by default.
    pub dependent_deadline_ms: Option<u64>,

    /// 任务调度时间，可选，指定任务的执行时间或依赖条件
    /// Task scheduled time, optional, specifies when or under what conditions the task should be executed
    pub scheduled_at: Option<ScheduledAt>,

    /// 定义任务所属的slot,在redis集群模式下,相同slot的任务才可以同时被消费,才可以互相依赖.
    /// 默认slot为当前的topic,redis非集群模式下无需设置.
    /// redis 集群模式下,任务出于不同topic但却互相依赖需要设置该值
    /// Defines the slot which the task belongs. Tasks in the same slot can be consumed at the same time and
    /// can depend each other in redis cluster mode.
    /// The default slot is the concurrent topic and does not need to be set in redis non-cluster mode.
    /// In cluster mode, task from different topics but dependent on each other need to set this value.
    pub slot: Option<String>,
}

/// 定义重试策略结构体，包含重试相关参数
/// Define the Retry struct, contains parameters related to retry behavior
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Retry {
    /// 最大重试次数
    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// 重试间隔时间（单位：毫秒）
    /// Retry interval time (in milliseconds)
    pub interval_ms: u32,
}

/// 定义任务调度时间的枚举类型
/// Define the ScheduledAt enum for specifying task scheduling time or conditions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ScheduledAt {
    /// 指定任务在某个时间戳（单位: 毫秒）执行
    /// Specifies that the task should be executed at a certain timestamp (in milliseconds)
    TimestampMs(u64),

    /// 指定任务在某些其他任务完成后执行
    /// Specifies that the task should be executed after certain other tasks are completed
    DependsOn(Vec<CompletedTask>),
}

/// 定义依赖完成的任务.
/// Define the task to be completed by the dependency.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompletedTask {
    /// 任务主题，用于标识任务的类别或分组
    /// Task topic, used to identify the category or group of the task
    pub topic: String,

    /// 任务的唯一标识符
    /// Unique identifier for the task
    pub id: String,

    /// 任务优先级
    /// Task priority
    pub priority: i8,

    /// 定义任务所属的slot,在redis集群模式下,相同slot的任务才可以同时被消费,才可以互相依赖.
    /// Defines the slot which the task belongs. Tasks in the same slot can be consumed at the same time and
    /// can depend each other in redis cluster mode.
    pub slot: Option<String>,

    /// 任务完成的状态.
    /// Task completion state.
    pub state: TaskCompletedState,
}

/// 定义任务运行时结构体，包含任务的运行时状态信息
/// Define the TaskRuntime struct, contains runtime state information for a task
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TaskRuntime {
    /// 任务当前状态，描述任务的生命周期阶段
    /// Current state of the task, describes the lifecycle stage of the task
    pub state: TaskState,

    /// 已重试次数，记录任务失败后尝试重试的次数
    /// Number of retry attempts, records how many times the task has been retried after failure
    pub retried: u32,

    /// 当前任务是否处于活动状态，且没有工作线程对其进行处理。
    /// 一个孤立的任务表示该工作进程已崩溃或遭遇网络故障，无法继续延长租期。
    ///
    /// 任务所在消费者可以恢复此任务
    ///
    /// IsOrphaned describes whether the task is left in active state with no worker processing it.
    /// An orphaned task indicates that the worker has crashed or experienced network failures and was not able to
    /// extend its lease on the task.
    ///
    /// This task will be recovered by running a server against the queue the task is in.
    /// This field is only applicable to tasks with TaskStateActive.
    pub is_orphaned: bool,

    /// 任务创建时间(单位: 毫秒)
    /// Task creation time (in milliseconds)
    pub created_at_ms: Option<u64>,

    /// 下次处理时间（单位：毫秒），表示任务计划执行的时间戳
    /// Next process time (in milliseconds), indicates the timestamp when the task is scheduled to be processed
    pub next_process_at_ms: u64,

    /// 最后一次处于`pending`状态, 等待消费者消费(单位：毫秒)
    /// The last time it entered `pending` state, awaiting a consumer's purchase (in milliseconds)
    pub last_pending_at_ms: Option<u64>,

    /// 最后一次进入`active`状态, 被某个消费者取出的时间(单位：毫秒)
    /// The last time it entered `active` state and was retrieved by a  consumer. (in milliseconds)
    pub last_active_at_ms: Option<u64>,

    /// 最后一次执行该任务的worker.
    /// The worker that last executed the task.
    pub last_worker: Option<String>,

    /// 最后一次失败错误信息
    /// Last failed error message.
    pub last_err: Option<String>,

    /// 最后一次失败时间（单位：毫秒），记录错误发生的时间戳
    /// Last failure time (in milliseconds), records the timestamp when the error occurred
    pub last_err_at_ms: Option<u64>,

    /// 任务完成时间（单位：毫秒），记录任务完成的时间戳
    /// Task completion time (in milliseconds), records the timestamp when the task was completed
    pub completed_at_ms: Option<u64>,

    /// 任务在stream中自动生成的ID,为生成的时间戳+序号
    /// The ID of the task is automatically generated in the stream, which is the generated timestamp + sequence number.
    pub stream_id: Option<String>,

    /// 任务执行结果，可选，存储任务的输出数据（以字节形式）
    /// Task execution result, optional, stores the output data of the task (in bytes)
    pub result: Option<Vec<u8>>,
}

/// 定义任务状态枚举，描述任务的各种可能状态，支持序列化、反序列化和比较
/// Define the TaskState enum, describes the possible states of a task, supports serialization/deserialization and comparison
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default, Clone, Copy)]
pub enum TaskState {
    /// 默认状态，任务等待处理
    /// Default state, task is waiting to be processed
    #[default]
    Pending,

    /// 任务为定时任务，等待指定时间再进入Pending状态
    /// The Task is a scheduled task, waiting for the specified time before entering the Pending state.
    Scheduled,

    /// 任务依赖于其他任务执行完毕，等待指定任务执行完毕后再进入Pending状态
    /// The task depends on the completion of other tasks and wait for the specified task to complete before entering the Pending state.
    Dependent,

    /// 任务正在执行
    /// Task is currently being executed
    Active,

    /// 任务进入重试状态
    /// Task is in retry state
    Retry,

    /// 任务成功完成
    /// Task completed successfully
    Succeed,

    /// 任务失败
    /// Task failed
    Failed,

    /// 任务被取消
    /// Task was canceled
    Canceled,
}

impl Task {
    /// 创建一个新的 Task 实例，生成一个 UUID 作为 id，并且允许传入一个可选的 payload
    /// Creates a new Task instance with a generated UUID as id, and an optional payload
    pub fn new(topic: impl Into<String>, payload: Option<Vec<u8>>) -> Task {
        Self::new_with(
            topic,
            uuid::Uuid::new_v4().to_string(),
            payload,
            TaskOptions::default(),
        )
    }

    /// 创建一个带有指定 id 和可选 payload 的 Task 实例
    /// Creates a Task instance with a specified id and optional payload
    pub fn new_with_id(
        topic: impl Into<String>,
        id: impl Into<String>,
        payload: Option<Vec<u8>>,
    ) -> Task {
        Self::new_with(topic, id, payload, TaskOptions::default())
    }

    pub fn new_with_options(
        topic: impl Into<String>,
        payload: Option<Vec<u8>>,
        options: TaskOptions,
    ) -> Task {
        Self::new_with(topic, uuid::Uuid::new_v4().to_string(), payload, options)
    }

    /// 创建一个带有指定 id、payload 和选项的 Task 实例
    /// Creates a Task instance with a specified id, payload, and options
    pub fn new_with(
        topic: impl Into<String>,
        id: impl Into<String>,
        payload: Option<Vec<u8>>,
        options: TaskOptions,
    ) -> Task {
        let runtime = match options.scheduled_at.as_ref() {
            Some(ScheduledAt::DependsOn(_)) => TaskRuntime {
                state: TaskState::Dependent,
                ..Default::default()
            },
            Some(ScheduledAt::TimestampMs(_)) => TaskRuntime {
                state: TaskState::Scheduled,
                ..Default::default()
            },
            None => TaskRuntime::default(),
        };
        Self {
            topic: topic.into(),
            id: id.into(),
            payload,
            options,
            runtime,
        }
    }

    /// 创建一个新的 Task，并通过bincode序列化 payload 来填充 payload 字段
    /// Creates a new Task instance by bincode serializing the payload and filling the payload field
    pub fn try_new<S>(topic: impl Into<String>, payload: S) -> Result<Task>
    where
        S: Serialize,
    {
        let payload_buf = bincode::serde::encode_to_vec(payload, config::standard())?;
        Ok(Self::new_with(
            topic,
            uuid::Uuid::new_v4().to_string(),
            Some(payload_buf),
            TaskOptions::default(),
        ))
    }

    /// 创建一个新的 Task，并通过bincode序列化 payload 来填充 payload 字段
    /// Creates a new Task instance by bincode serializing the payload and filling the payload field
    pub fn try_new_with_serde<S>(
        topic: impl Into<String>,
        id: impl Into<String>,
        payload: S,
        options: TaskOptions,
    ) -> Result<Task>
    where
        S: Serialize,
    {
        let payload_buf = bincode::serde::encode_to_vec(payload, config::standard())?;
        Ok(Self::new_with(topic, id, Some(payload_buf), options))
    }

    /// 设置任务的 id
    /// Sets the id for the task
    pub fn with_id(mut self, id: impl Into<String>) -> Task {
        self.id = id.into();
        self
    }

    /// 设置任务的优先级 值越小优先级越高，可以使用负数来表达更高的优先级
    /// Sets the priority for the task, lower value indicates higher priority.
    pub fn with_priority(mut self, priority: i8) -> Task {
        self.options.priority = priority;
        self
    }

    /// 设置任务完成后的保留时间（retention），单位是毫秒
    /// Specifies how long the task stays after completed in milliseconds
    pub fn with_retention(mut self, retention_ms: u64) -> Task {
        self.options.retention_ms = retention_ms;
        self
    }

    /// 设置任务的重试策略，指定最大重试次数和重试间隔（单位：毫秒）
    /// Sets the retry strategy for the task, with max retries and retry interval in milliseconds the behavior for retrying failed tasks
    pub fn with_retry(mut self, max_retries: u32, interval_ms: u32) -> Task {
        self.options.retry = Some(Retry {
            max_retries,
            interval_ms,
        });
        self
    }

    /// 设置任务的超时时间（timeout），单位是毫秒
    /// Sets the timeout for the task, in milliseconds
    pub fn with_timeout(mut self, timeout_ms: u64) -> Task {
        self.options.timeout_ms = Some(timeout_ms);
        self
    }

    /// 设置任务的截止时间（deadline），单位是毫秒
    /// Sets the deadline for the task, in milliseconds
    pub fn with_deadline(mut self, deadline_ms: u64) -> Task {
        self.options.deadline_ms = Some(deadline_ms);
        self
    }

    /// 设置任务的执行时间或依赖条件
    /// Sets the scheduled time or dependency condition of the task
    pub fn with_scheduled(mut self, scheduled_at: ScheduledAt) -> Task {
        match scheduled_at {
            ScheduledAt::DependsOn(_) => self.runtime.state = TaskState::Dependent,
            ScheduledAt::TimestampMs(_) => self.runtime.state = TaskState::Scheduled,
        }
        self.options.scheduled_at = Some(scheduled_at);
        self
    }

    /// 定义任务所属的slot,在redis集群模式下,相同slot的任务才可以同时被消费,才可以互相依赖.
    /// 默认slot为当前的topic,redis非集群模式下无需设置.
    /// redis 集群模式下,任务出于不同topic但却互相依赖需要设置该值
    /// Defines the slot which the task belongs. Tasks in the same slot can be consumed at the same time and
    /// can depend each other in redis cluster mode.
    /// The default slot is the concurrent topic and does not need to be set in redis non-cluster mode.
    /// In cluster mode, task from different topics but dependent on each other need to set this value.
    pub fn with_slot(mut self, slot: impl Into<String>) -> Task {
        self.options.slot = Some(slot.into());
        self
    }

    pub fn to_completed_task(&self, state: TaskCompletedState) -> CompletedTask {
        CompletedTask {
            topic: self.topic.clone(),
            id: self.id.clone(),
            priority: self.options.priority,
            slot: self.options.slot.clone(),
            state,
        }
    }

    pub fn state(&self) -> TaskState {
        self.runtime.state
    }

    /// 获取任务的 stream_id
    /// Get the stream_id of the task
    pub fn stream_id(&self) -> Option<&str> {
        self.runtime.stream_id.as_deref()
    }

    /// 获取任务的运行时信息
    /// Get the runtime information of the task
    pub fn runtime(&self) -> &TaskRuntime {
        &self.runtime
    }

    /// 获取任务配置选项
    /// Get the task options
    pub fn options(&self) -> &TaskOptions {
        &self.options
    }

    /// 获取任务的当前截止时间
    /// Get the current deadline of the task
    pub fn current_deadline(&self) -> Option<Instant> {
        match self.options.deadline_ms {
            Some(deadline) => {
                if deadline > START.1 {
                    START
                        .0
                        .checked_add(Duration::from_millis(deadline - START.1))
                } else {
                    START
                        .0
                        .checked_sub(Duration::from_millis(START.1 - deadline))
                }
            }
            None => match self.options.timeout_ms {
                Some(timeout) => Instant::now().checked_add(Duration::from_millis(timeout)),
                None => None,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskCompletedState {
    /// 任务完成的任意状态 succeed/failed/canceled
    /// Any state of task completion: succeed/failed/canceled.
    Any,

    /// 任务成功完成
    /// Task completed successfully
    Succeed,

    /// 任务失败
    /// Task failed
    Failed,

    /// 任务被取消
    /// Task was canceled
    Canceled,

    /// 任务完成, 但是不是被取消的
    /// Task completed but not because of canceled.
    NotCanceled,
}

impl Default for TaskOptions {
    fn default() -> Self {
        Self {
            priority: 0,
            retention_ms: 1000 * 60 * 60 * 24 * 7,
            retry: None,
            timeout_ms: None,
            deadline_ms: None,
            dependent_deadline_ms: None,
            scheduled_at: None,
            slot: None,
        }
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl TryFrom<&str> for TaskState {
    type Error = crate::errors::Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            "pending" => TaskState::Pending,
            "active" => TaskState::Active,
            "scheduled" => TaskState::Scheduled,
            "dependent" => TaskState::Dependent,
            "retry" => TaskState::Retry,
            "succeed" => TaskState::Succeed,
            "failed" => TaskState::Failed,
            "canceled" => TaskState::Canceled,
            other => return Err(crate::errors::Error::UnknownTaskState(other.to_string())),
        })
    }
}

impl AsRef<str> for TaskState {
    fn as_ref(&self) -> &str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Active => "active",
            TaskState::Canceled => "canceled",
            TaskState::Dependent => "dependent",
            TaskState::Failed => "failed",
            TaskState::Succeed => "succeed",
            TaskState::Retry => "retry",
            TaskState::Scheduled => "scheduled",
        }
    }
}

impl AsRef<str> for TaskCompletedState {
    fn as_ref(&self) -> &str {
        match self {
            TaskCompletedState::Any => "any",
            TaskCompletedState::Canceled => "canceled",
            TaskCompletedState::Failed => "failed",
            TaskCompletedState::Succeed => "succeed",
            TaskCompletedState::NotCanceled => "not_canceled",
        }
    }
}

impl std::fmt::Display for TaskCompletedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl TaskState {
    pub fn is_completed(&self) -> bool {
        matches!(
            self,
            TaskState::Succeed | TaskState::Failed | TaskState::Canceled
        )
    }
}

static START: LazyLock<(Instant, u64)> = LazyLock::new(|| {
    let now = Instant::now();
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    (now, timestamp_ms)
});

impl ScheduledAt {
    pub fn depends_on(&self) -> crate::errors::Result<&[CompletedTask]> {
        match self {
            ScheduledAt::TimestampMs(_) => Err(crate::errors::Error::DependentTasksNotFound),
            ScheduledAt::DependsOn(tasks) => Ok(tasks),
        }
    }
}

impl CompletedTask {
    pub fn new(topic: impl Into<String>, id: impl Into<String>, state: TaskCompletedState) -> Self {
        CompletedTask {
            topic: topic.into(),
            id: id.into(),
            priority: 0,
            slot: None,
            state,
        }
    }
}
