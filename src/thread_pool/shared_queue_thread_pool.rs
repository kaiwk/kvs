use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use anyhow::Result;
use log::warn;

use crate::thread_pool::ThreadPool;

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

pub struct SharedQueueThreadPool {
    tx: Sender<ThreadPoolMessage>,
    thread_handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let (tx, rx) = channel();
        let mut thread_handles = Vec::new();
        let rx_ = Arc::new(Mutex::new(rx));
        for _ in 0..threads {
            let rx_ = rx_.clone();
            let handle = thread::spawn(move || loop {
                let rx = rx_.lock().unwrap();
                if let Ok(msg) = rx.recv() {
                    drop(rx);
                    match msg {
                        ThreadPoolMessage::RunJob(job) => job(),
                        ThreadPoolMessage::Shutdown => break,
                    }
                } else {
                    warn!("rx recv failed");
                }
            });

            thread_handles.push(handle);
        }

        Ok(SharedQueueThreadPool { tx, thread_handles })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(e) = self.tx.send(ThreadPoolMessage::RunJob(Box::new(job))) {
            warn!("send RunJob failed: {:?}", e);
        }
    }
}
