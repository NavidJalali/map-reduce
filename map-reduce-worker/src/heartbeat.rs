use std::{sync::Arc, time::Duration};

use map_reduce_core::grpc::{self, master_client::MasterClient};
use tokio::{
  sync::{Mutex, RwLock},
  time::sleep,
};
use tonic::transport::Channel;
use tracing::{error, info};

use crate::{
  error_tracker::ErrorTracker,
  file_system::FileSystem,
  shutdown::{shutdown_manager::Shutdown, shutdown_reason::ShutdownReason},
};

pub async fn start_heartbeat_fiber<FS: FileSystem + Send + Sync + 'static>(
  file_system: Arc<FS>,
  client: Arc<Mutex<MasterClient<Channel>>>,
  heartbeat_interval: Duration,
  max_error_tolerance: usize,
  error_tracker: Arc<RwLock<ErrorTracker>>,
  shutdown: Arc<Shutdown<ShutdownReason>>,
) {
  let finalizer_shutdown = shutdown.clone();
  let fiber = tokio::spawn(async move {
    loop {
      sleep(heartbeat_interval).await;

      let fs_info = file_system
        .file_system_information()
        .await
        .expect("Failed to get file system information");

      let result = client
        .lock()
        .await
        .send_heartbeat(tonic::Request::new(grpc::SendHeartbeatRequest {
          file_system_information: Some(fs_info),
        }))
        .await;

      match result {
        Ok(_) => {
          info!("Successfully sent heartbeat");
          let mut err = error_tracker.write().await;
          err.heartbeat.clear();
        }
        Err(status) => {
          error!("Failed to send heartbeat: {:?}", status);

          let should_shutdown = {
            let mut err = error_tracker.write().await;
            err.heartbeat.push(status);

            // Check condition and clone errors inside the scope
            if err.heartbeat.len() > max_error_tolerance {
              Some(err.clone())
            } else {
              None
            }
          };

          if let Some(errors) = should_shutdown {
            error!("Reached max error tolerance, shutting down");

            tokio::spawn(async move {
              shutdown.trigger(ShutdownReason::WorkerError(errors)).await;
            });

            break;
          }
        }
      }
    }
  });

  finalizer_shutdown
    .register_shutdown_task(
      || {
        Box::pin(async {
          fiber.abort();
          info!("Aborted heartbeat fiber");
          let exit = fiber.await;
          info!("heartbeat exited: {:?}", exit);
        })
      },
      "heartbeat".to_string(),
    )
    .await;
}
