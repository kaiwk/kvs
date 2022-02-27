mod naive_thread_pool;
mod shared_queue_thread_pool;

pub use naive_thread_pool::NaiveThreadPool;
pub use shared_queue_thread_pool::SharedQueueThreadPool;

use anyhow::Result;

/// thread pool
pub trait ThreadPool {
    /// create a thread pool
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// spawn a job
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
