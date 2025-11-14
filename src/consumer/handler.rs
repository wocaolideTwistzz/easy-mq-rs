use crate::errors::Result;
use crate::task::Task;

/// 任务处理器 trait
/// Task handler trait
#[async_trait::async_trait]
pub trait Handler {
    async fn process_task(&self, task: &Task) -> Result<Option<Vec<u8>>>;
}

#[async_trait::async_trait]
impl<F> Handler for F
where
    F: Fn(&Task) -> Result<Option<Vec<u8>>> + Sync + Send,
{
    async fn process_task(&self, task: &Task) -> Result<Option<Vec<u8>>> {
        self(task)
    }
}

/// 异步任务处理器
/// Async task handler
pub struct AsyncHandler<F, Fut> {
    func: F,

    _phantom: std::marker::PhantomData<Fut>,
}

impl<F, Fut> AsyncHandler<F, Fut>
where
    F: Fn(&Task) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<Option<Vec<u8>>>> + Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<F, Fut> Handler for AsyncHandler<F, Fut>
where
    F: Fn(&Task) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<Option<Vec<u8>>>> + Send + Sync,
{
    async fn process_task(&self, task: &Task) -> Result<Option<Vec<u8>>> {
        (self.func)(task).await
    }
}
