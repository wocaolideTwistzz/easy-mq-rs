use crate::{claimer::ClaimConfig, model::Queue};

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// 刷新队列列表的策略
    /// Refresh queue list strategy
    pub refresh_queues_strategy: RefreshQueuesStrategy,

    /// 出队列失败后的退避策略
    /// Dequeue backoff strategy
    pub dequeue_backoff_strategy: DequeueBackoffStrategy,

    /// 最大并发数
    /// Maximum concurrent tasks
    pub max_concurrent: usize,

    // ===== Claimer config =====
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

    /// 清理空闲超时的主题的间隔 (秒) 默认不设置
    /// Clean idle expired topics interval (seconds) default disabled
    pub clean_topics_interval_sec: u64,

    /// 清理空闲超时的主题的最小空闲时间 (秒) 默认不设置
    /// Minimum idle time for cleaning idle expired topics (seconds) default disabled
    pub clean_topics_min_idle_sec: u64,
}

impl ConsumerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置刷新队列列表的策略
    /// Set refresh queue list strategy
    pub fn with_refresh_queues_strategy(mut self, strategy: RefreshQueuesStrategy) -> Self {
        self.refresh_queues_strategy = strategy;
        self
    }

    /// 设置出队列失败后的退避策略
    /// Set dequeue backoff strategy
    pub fn with_dequeue_backoff_strategy(mut self, strategy: DequeueBackoffStrategy) -> Self {
        self.dequeue_backoff_strategy = strategy;
        self
    }
}

#[derive(Debug, Clone)]
pub enum RefreshQueuesStrategy {
    /// 全部的队列, 只在初始化的时候刷新一次
    /// All queues, only refresh once when initialized
    AllOnStart,
    /// 全部的队列, 每次出队列的时候刷新
    /// All queues, refresh on each dequeue
    AllOnEachDequeue,
    /// 全部的队列, 每隔一段时间刷新一次 单位: 秒
    /// All queues, refresh on each interval (in seconds)
    AllOnIntervalSec(u64),

    /// 指定话题的队列, 只在初始化的时候刷新一次
    /// Specified topic queues, only refresh once when initialized
    SpecifiedTopicOnStart(Vec<String>),
    /// 指定话题的队列, 每次出队列的时候刷新
    /// Specified topic queues, refresh on each dequeue
    SpecifiedTopicOnEachDequeue(Vec<String>),
    /// 指定话题的队列, 每隔一段时间刷新一次 单位: 秒
    /// Specified topic queues, refresh on each interval (in seconds)
    SpecifiedTopicOnIntervalSec(Vec<String>, u64),

    /// 固定队列列表
    /// Fixed queues
    FixedQueues(Vec<Queue>),
}

#[derive(Debug, Clone)]
pub enum DequeueBackoffStrategy {
    /// 固定退避时间 单位: 秒
    /// Fixed backoff duration (in seconds)
    Fixed(u64),

    /// 指数退避 - 每次失败后退避时间翻倍
    /// Exponential backoff - backoff time doubles after each failure
    Exponential(Exponential),

    /// 斐波那契退避 - 每次失败后退避时间按照斐波那契数列增长
    /// Fibonacci backoff - backoff time grows according to Fibonacci sequence after each failure
    Fibonacci(Fibonacci),
}

#[derive(Debug, Clone)]
pub struct Exponential {
    /// 基础退避时间 单位: 秒
    /// Base backoff duration (in seconds)
    pub base: u64,

    /// 当前退避时间 单位: 秒
    /// Current backoff duration (in seconds)
    pub current: u64,

    /// 最大退避时间 单位: 秒
    /// Maximum backoff duration (in seconds)
    pub max: u64,
}

#[derive(Debug, Clone)]
pub struct Fibonacci {
    /// 基础退避时间 单位: 秒
    /// Base backoff duration (in seconds)
    pub base: u64,

    /// 当前斐波那契数 单位: 秒
    /// Current Fibonacci number (in seconds)
    pub current: u64,

    /// 下一个斐波那契数 单位: 秒
    /// Next Fibonacci number (in seconds)
    pub next: u64,

    /// 最大退避时间 单位: 秒
    /// Maximum backoff duration (in seconds)
    pub max: u64,
}

impl DequeueBackoffStrategy {
    /// 重置退避策略到初始状态
    /// Reset backoff strategy to initial state
    pub fn reset(&mut self) {
        match self {
            DequeueBackoffStrategy::Fixed(_) => {}
            DequeueBackoffStrategy::Exponential(exponential) => {
                exponential.current = exponential.base;
            }
            DequeueBackoffStrategy::Fibonacci(fibonacci) => {
                fibonacci.current = fibonacci.base;
                fibonacci.next = fibonacci.base;
            }
        }
    }

    /// 获取下一次退避时间
    /// Get next backoff duration
    pub fn backoff(&mut self) -> u64 {
        match self {
            DequeueBackoffStrategy::Fixed(duration) => *duration,
            DequeueBackoffStrategy::Exponential(exponential) => {
                let ret = exponential.current;
                exponential.current *= 2;
                if exponential.current > exponential.max {
                    exponential.current = exponential.max;
                }
                ret
            }
            DequeueBackoffStrategy::Fibonacci(fibonacci) => {
                let ret = fibonacci.current;
                let next = fibonacci.current + fibonacci.next;
                fibonacci.current = fibonacci.next;
                fibonacci.next = next;
                if fibonacci.current > fibonacci.max {
                    fibonacci.current = fibonacci.max;
                }
                ret
            }
        }
    }

    /// 创建固定退避策略 (单位: 秒)
    /// Create fixed backoff strategy (in seconds)
    pub fn fixed_sec(duration: u64) -> Self {
        DequeueBackoffStrategy::Fixed(duration)
    }

    /// 创建指数退避策略 (单位: 秒)
    /// Create exponential backoff strategy (in seconds)
    pub fn exponential(base: u64, max: u64) -> Self {
        DequeueBackoffStrategy::Exponential(Exponential {
            base,
            current: base,
            max,
        })
    }

    /// 创建斐波那契退避策略 (单位: 秒)
    /// Create fibonacci backoff strategy (in seconds)
    pub fn fibonacci(base: u64, max: u64) -> Self {
        DequeueBackoffStrategy::Fibonacci(Fibonacci {
            base,
            current: base,
            next: base,
            max,
        })
    }
}

impl From<&ConsumerConfig> for ClaimConfig {
    fn from(val: &ConsumerConfig) -> Self {
        let (specified_topics, refresh_queues_interval_sec) = match &val.refresh_queues_strategy {
            RefreshQueuesStrategy::AllOnStart | RefreshQueuesStrategy::AllOnEachDequeue => {
                (None, 0)
            }
            RefreshQueuesStrategy::AllOnIntervalSec(interval) => (None, *interval),
            RefreshQueuesStrategy::SpecifiedTopicOnStart(topics) => (Some(topics.clone()), 0),
            RefreshQueuesStrategy::SpecifiedTopicOnEachDequeue(topics) => (Some(topics.clone()), 0),
            RefreshQueuesStrategy::SpecifiedTopicOnIntervalSec(topics, interval) => {
                (Some(topics.clone()), *interval)
            }
            RefreshQueuesStrategy::FixedQueues(_) => (None, 0),
        };

        ClaimConfig {
            specified_topics,
            refresh_queues_interval_sec,

            scheduled_claim_interval_sec: val.scheduled_claim_interval_sec,
            dependent_claim_interval_sec: val.dependent_claim_interval_sec,
            timeout_claim_interval_sec: val.timeout_claim_interval_sec,
            timeout_min_idle_sec: val.timeout_min_idle_sec,
            deadline_claim_interval_sec: val.deadline_claim_interval_sec,
            retention_claim_interval_sec: val.retention_claim_interval_sec,
            cancel_pending_min_sec: val.cancel_pending_min_sec,
            cancel_pending_claim_interval_sec: val.cancel_pending_claim_interval_sec,
            clean_topics_interval_sec: val.clean_topics_interval_sec,
            clean_topics_min_idle_sec: val.clean_topics_min_idle_sec,
        }
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            refresh_queues_strategy: RefreshQueuesStrategy::AllOnEachDequeue,
            dequeue_backoff_strategy: DequeueBackoffStrategy::Fixed(1),
            max_concurrent: 20,

            scheduled_claim_interval_sec: 5,
            dependent_claim_interval_sec: 5,
            timeout_claim_interval_sec: 5,
            timeout_min_idle_sec: 5,
            deadline_claim_interval_sec: 60,
            retention_claim_interval_sec: 60,

            cancel_pending_min_sec: 0,
            cancel_pending_claim_interval_sec: 0,
            clean_topics_interval_sec: 0,
            clean_topics_min_idle_sec: 0,
        }
    }
}
