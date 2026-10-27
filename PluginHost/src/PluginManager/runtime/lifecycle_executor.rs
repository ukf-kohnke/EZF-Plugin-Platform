//! Serialized lifecycle callback executor with timeout-safe retry semantics.
use super::PluginLifecycleStatus;
use super::super::executor::PluginCancellationSource;
use std::sync::{ Mutex, mpsc };
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginLifecycleOperation {
    OnUnload,
    Destroy,
}

pub(super) struct PendingPluginLifecycle {
    pub(super) operation: PluginLifecycleOperation,
    pub(super) receiver: mpsc::Receiver<Result<PluginLifecycleStatus, String>>,
    pub(super) cancellation: PluginCancellationSource,
}

pub(super) type PluginLifecycleJob = Box<
    dyn (FnOnce() -> Result<PluginLifecycleStatus, String>) + Send + 'static
>;

/// Dedicated single-worker executor for lifecycle callbacks. Lifecycle operations do not consume
/// normal request-worker capacity. A timed-out callback is intentionally allowed to finish on this
/// persistent worker; the runtime remains registered and will only proceed to the next lifecycle
/// stage after the callback's actual result has been observed.
pub(super) struct PluginLifecycleExecutor {
    sender: Mutex<Option<mpsc::SyncSender<PluginLifecycleJob>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl PluginLifecycleExecutor {
    pub(super) fn new() -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel::<PluginLifecycleJob>(1);
        let worker = thread::Builder
            ::new()
            .name("ezf-plugin-lifecycle-worker".to_owned())
            .spawn(move || {
                loop {
                    let job = match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => {
                            break;
                        }
                    };
                    let _ = job();
                }
            })
            .map_err(|error| format!("failed to spawn plugin lifecycle worker: {error}"))?;

        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
        })
    }

    pub(super) fn submit(
        &self,
        job: PluginLifecycleJob
    ) -> Result<mpsc::Receiver<Result<PluginLifecycleStatus, String>>, String> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let wrapped: PluginLifecycleJob = Box::new(move || {
            let result = std::panic
                ::catch_unwind(std::panic::AssertUnwindSafe(job))
                .unwrap_or_else(|_| Err("plugin lifecycle callback panicked".to_owned()));
            let _ = sender.send(result.clone());
            result
        });

        let sender = self.sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(sender) = sender.as_ref() else {
            return Err("plugin lifecycle worker is shut down".to_owned());
        };
        sender
            .try_send(wrapped)
            .map_err(|_| "plugin lifecycle worker is busy or disconnected".to_owned())?;
        Ok(receiver)
    }

    pub(super) fn shutdown(&self) {
        let sender = self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(sender);

        let worker = self.worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(worker) = worker else {
            return;
        };
        if worker.thread().id() != thread::current().id() {
            let _ = worker.join();
        }
    }
}

impl Drop for PluginLifecycleExecutor {
    fn drop(&mut self) {
        self.shutdown();
    }
}
