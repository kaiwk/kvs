use std::thread;

use anyhow::Result;

use crate::thread_pool::ThreadPool;

pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> Result<SharedQueueThreadPool> {
        Ok(SharedQueueThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
