use crossbeam::channel::{self, Receiver, Sender};
use std::thread;

use anyhow::Result;
use log::debug;
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

#[derive(Clone)]
struct TaskReceiver(Receiver<ThreadPoolMessage>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_task(rx)) {
                warn!("run task failed, {:?}", e);
            }
        }
    }
}

fn run_task(receiver: TaskReceiver) {
    loop {
        match receiver.0.recv() {
            Ok(msg) => match msg {
                ThreadPoolMessage::RunJob(job) => job(),
                ThreadPoolMessage::Shutdown => return,
            },
            Err(e) => debug!("thread pool exits: {:?}", e),
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let (tx, rx) = channel::unbounded();
        let mut thread_handles = Vec::new();
        for _ in 0..threads {
            let task_receiver = TaskReceiver(rx.clone());
            let handle = thread::spawn(move || {
                run_task(task_receiver);
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
