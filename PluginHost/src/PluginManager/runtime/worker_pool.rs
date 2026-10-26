//! Bounded persistent request worker pool used by each plugin runtime.
use std::sync::{ Arc, Mutex, atomic::{ AtomicUsize, Ordering }, mpsc };
use std::thread;

pub(super) type PluginJob = Box<dyn FnOnce() + Send + 'static>;

pub(super) enum PluginWorkerSubmitError {
    QueueFull(PluginJob),
    Closed(PluginJob),
}

pub(super) struct PluginWorkerPool {
    pub(super) sender: Mutex<Option<mpsc::SyncSender<PluginJob>>>,
    pub(super) workers: Mutex<Option<Vec<thread::JoinHandle<()>>>>,
    pub(super) queue_capacity: usize,
    pub(super) queued_jobs: Arc<AtomicUsize>,
    pub(super) running_workers: Arc<AtomicUsize>,
}

struct WorkerActivityGuard(Arc<AtomicUsize>);

impl WorkerActivityGuard {
    fn enter(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for WorkerActivityGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl PluginWorkerPool {
    pub(super) fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, String> {
        let worker_count = worker_count.max(1);
        let queue_capacity = queue_capacity.max(1);
        let (sender, receiver) = mpsc::sync_channel::<PluginJob>(queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let queued_jobs = Arc::new(AtomicUsize::new(0));
        let running_workers = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let worker_queue_depth = Arc::clone(&queued_jobs);
            let worker_running_count = Arc::clone(&running_workers);
            let thread_name = format!("ezf-plugin-worker-{index}");
            match
                thread::Builder
                    ::new()
                    .name(thread_name)
                    .spawn(move || {
                        loop {
                            let job = {
                                let receiver = receiver
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                receiver.recv()
                            };

                            match job {
                                Ok(job) => {
                                    worker_queue_depth.fetch_sub(1, Ordering::Relaxed);
                                    let _activity = WorkerActivityGuard::enter(
                                        Arc::clone(&worker_running_count)
                                    );
                                    job();
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    })
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(format!("failed to spawn plugin worker {index}: {error}"));
                }
            }
        }

        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(Some(workers)),
            queue_capacity,
            queued_jobs,
            running_workers,
        })
    }

    pub(super) fn submit(&self, job: PluginJob) -> Result<(), PluginWorkerSubmitError> {
        let sender = self.sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(sender) = sender.as_ref() else {
            return Err(PluginWorkerSubmitError::Closed(job));
        };

        // Increment before publishing the job so a worker cannot dequeue it before it is counted.
        self.queued_jobs.fetch_add(1, Ordering::Relaxed);
        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job)) => {
                self.queued_jobs.fetch_sub(1, Ordering::Relaxed);
                Err(PluginWorkerSubmitError::QueueFull(job))
            }
            Err(mpsc::TrySendError::Disconnected(job)) => {
                self.queued_jobs.fetch_sub(1, Ordering::Relaxed);
                Err(PluginWorkerSubmitError::Closed(job))
            }
        }
    }

    pub(super) fn shutdown(&self) {
        let sender = self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(sender);

        let workers = self.workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(workers) = workers else {
            return;
        };

        let current_thread_id = thread::current().id();
        for worker in workers {
            // A last Arc<PluginRuntime> can theoretically be released by the worker executing a
            // timed-out job. Never attempt to join the current worker thread from itself; dropping
            // that JoinHandle detaches it and it will exit as soon as the queue observes closure.
            if worker.thread().id() == current_thread_id {
                continue;
            }
            let _ = worker.join();
        }
    }
}

impl Drop for PluginWorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}
