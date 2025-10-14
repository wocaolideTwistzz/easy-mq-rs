use serde::{Deserialize, Serialize};

/// 定义任务结构体
/// Define the Task struct
#[derive(Debug, Serialize, Deserialize)]
pub struct Task<T> {
    /// 任务的唯一标识符
    /// Unique identifier for the task
    pub id: String,

    /// 任务负载，包含任务的具体数据
    /// Task payload, contains the actual data of the task
    pub payload: T,

    /// 任务选项，包含任务的配置信息，如优先级、重试策略等
    /// Task options, contains configuration information such as priority, retry strategy, etc.
    pub options: TaskOptions,

    /// 任务运行时数据，包含当前状态，已重试次数，执行结果等
    /// Task runtimes, contains current state, retried, results, etc.
    pub(crate) runtime: TaskRuntime,
}

/// 定义任务选项结构体，包含任务的配置参数
/// Define the TaskOptions struct, contains configuration parameters for the task
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TaskOptions {
    /// 任务主题，可选，用于标识任务的类别或分组
    /// Task topic, optional, used to identify the category or group of the task
    pub topic: Option<String>,

    /// 任务优先级，值越小优先级越高
    /// Task priority, lower value indicates higher priority
    pub priority: i8,

    /// 任务保留时间（单位：秒），用于指定任务在完成后保留的时间
    /// Task retention time (in seconds), specifies how long the task stays after completed.
    pub retention: u64,

    /// 重试策略，可选，定义任务失败后的重试行为
    /// Retry strategy, optional, defines the behavior for retrying failed tasks
    pub retry: Option<Retry>,

    /// 任务超时时间（单位：秒），可选，指定任务执行的最大时长
    /// Task timeout (in seconds), optional, specifies the maximum duration for task execution
    pub timeout: Option<u64>,

    /// 任务截止时间（单位： Unix秒级时间戳），可选，指定任务必须完成的时间点
    /// Task deadline (unix timestamp), optional, specifies the time by which the task must be completed
    pub deadline: Option<u64>,

    /// 任务调度时间，可选，指定任务的执行时间或依赖条件
    /// Task scheduled time, optional, specifies when or under what conditions the task should be executed
    pub scheduled_at: Option<ScheduledAt>,
}

/// 定义重试策略结构体，包含重试相关参数
/// Define the Retry struct, contains parameters related to retry behavior
#[derive(Debug, Serialize, Deserialize)]
pub struct Retry {
    /// 最大重试次数
    /// Maximum number of retry attempts
    pub max_retry: u32,

    /// 重试间隔时间（单位：秒）
    /// Retry interval time (in seconds)
    pub interval: u32,
}

/// 定义任务调度时间的枚举类型
/// Define the ScheduledAt enum for specifying task scheduling time or conditions
#[derive(Debug, Serialize, Deserialize)]
pub enum ScheduledAt {
    /// 指定任务在某个时间戳（单位：秒）执行
    /// Specifies that the task should be executed at a certain unix timestamp
    Timestamp(u64),

    /// 指定任务在某些其他任务完成后执行
    /// Specifies that the task should be executed after certain other tasks are completed
    TasksCompleted(Vec<TaskCompleted>),
}

/// 定义任务完成状态的枚举类型
/// Define the TaskCompleted enum for representing the completion status of tasks
#[derive(Debug, Serialize, Deserialize)]
pub enum TaskCompleted {
    /// 任务完成（不区分成功或失败）- 任务ID
    /// Task completed (regardless of success or failure) - with task id
    Completed(String),

    /// 任务成功完成 - 任务ID
    /// Task completed successfully - with task id
    Success(String),

    /// 任务失败 - 任务ID
    /// Task failed - with task id
    Failed(String),

    /// 任务被取消 - 任务ID
    /// Task was canceled - with task id
    Canceled(String),
}

/// 定义任务运行时结构体，包含任务的运行时状态信息
/// Define the TaskRuntime struct, contains runtime state information for a task
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TaskRuntime {
    /// 任务当前状态，描述任务的生命周期阶段
    /// Current state of the task, describes the lifecycle stage of the task
    pub state: TaskState,

    /// 已重试次数，记录任务失败后尝试重试的次数
    /// Number of retry attempts, records how many times the task has been retried after failure
    pub retried: u32,

    /// 下次处理时间（单位：秒），表示任务计划执行的时间戳
    /// Next process time (in seconds), indicates the timestamp when the task is scheduled to be processed
    pub next_process_at: u64,

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

    /// 错误信息列表，可选，记录任务执行过程中的错误
    /// List of error messages, optional, records errors encountered during task execution
    pub errors: Option<Vec<ErrorMessage>>,

    /// 任务完成时间（单位：秒），记录任务完成的时间戳
    /// Task completion time (in seconds), records the timestamp when the task was completed
    pub completed_at: u64,

    /// 任务执行结果，可选，存储任务的输出数据（以字节形式）
    /// Task execution result, optional, stores the output data of the task (in bytes)
    pub result: Option<Vec<u8>>,
}

/// 定义错误消息结构体，记录任务失败时的错误信息
/// Define the ErrorMessage struct, records error information when a task fails
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// 错误消息内容，描述失败的具体原因
    /// Error message content, describes the specific reason for the failure
    pub error_msg: String,

    /// 失败时间（单位：秒），记录错误发生的时间戳
    /// Failure time (in seconds), records the timestamp when the error occurred
    pub failed_at: u64,
}

/// 定义任务状态枚举，描述任务的各种可能状态，支持序列化、反序列化和比较
/// Define the TaskState enum, describes the possible states of a task, supports serialization/deserialization and comparison
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum TaskState {
    /// 默认状态，任务等待处理
    /// Default state, task is waiting to be processed
    #[default]
    Pending,

    /// 任务已调度，等待执行
    /// Task is scheduled, waiting for execution
    Scheduled,

    /// 任务正在执行
    /// Task is currently being executed
    Active,

    /// 任务进入重试状态
    /// Task is in retry state
    Retry,

    /// 任务成功完成
    /// Task completed successfully
    Success,

    /// 任务失败
    /// Task failed
    Failed,

    /// 任务被取消
    /// Task was canceled
    Canceled,
}

impl<T> Task<T> {
    pub fn new(payload: T) -> Task<T> {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            payload,
            options: TaskOptions::default(),
            runtime: TaskRuntime::default(),
        }
    }

    pub fn new_with_id(id: impl Into<String>, payload: T) -> Task<T> {
        Self {
            id: id.into(),
            payload,
            options: TaskOptions::default(),
            runtime: TaskRuntime::default(),
        }
    }

    pub fn new_with_topic_id(
        topic: impl Into<String>,
        id: impl Into<String>,
        payload: T,
    ) -> Task<T> {
        Self {
            id: id.into(),
            payload,
            options: TaskOptions {
                topic: Some(topic.into()),
                ..Default::default()
            },
            runtime: TaskRuntime::default(),
        }
    }
}
