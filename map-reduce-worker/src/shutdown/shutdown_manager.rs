use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

type ShutdownTask = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct ShutdownManager<A> {
  pub shutdown: Arc<Shutdown<A>>,
  receiver: oneshot::Receiver<A>,
}

impl<A> ShutdownManager<A> {
  pub fn new() -> Self {
    let (sender, receiver) = oneshot::channel();
    let shutdown = Arc::new(Shutdown {
      sender: Mutex::new(Some(sender)),
      tasks: Mutex::new(Vec::new()),
    });

    Self {
      shutdown: shutdown.clone(),
      receiver,
    }
  }

  pub async fn await_shutdown(self) -> Result<A, oneshot::error::RecvError> {
    self.receiver.await
  }
}

pub struct Shutdown<A> {
  sender: Mutex<Option<oneshot::Sender<A>>>,
  tasks: Mutex<Vec<(String, ShutdownTask)>>,
}

impl<A> Shutdown<A> {
  pub async fn register_shutdown_task<F>(&self, task: F, description: String)
  where
    F: FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
  {
    self.tasks.lock().await.push((description, Box::new(task)));
  }

  pub async fn trigger(&self, value: A) {
    info!("Triggering shutdown signal");
    let mut sender_guard = self.sender.lock().await;
    if let Some(sender) = sender_guard.take() {
      let tasks = {
        let mut tasks_guard = self.tasks.lock().await;
        std::mem::take(&mut *tasks_guard)
      };

      let total_tasks = tasks.len();

      /*
       * this is bad. I should probably spawn a fiber for each task.
       * I also need some sort of deadline to ensure that the shutdown signal is sent
       * even if the tasks take too long to complete.
       * For now its good enough tho.
       */

      for (index, (description, task)) in tasks.into_iter().rev().enumerate() {
        info!(
          "[{}/{}] Running shutdown task: {}",
          index + 1,
          total_tasks,
          description
        );
        task().await;
        info!(
          "[{}/{}] Shutdown task completed: {}",
          index + 1,
          total_tasks,
          description
        );
      }

      let success = sender.send(value).is_ok();

      if success {
        info!("Shutdown signal sent successfully");
      } else {
        warn!("Failed to send shutdown signal");
      }
    }
  }

  pub async fn is_triggered(&self) -> bool {
    self.sender.lock().await.is_none()
  }
}
