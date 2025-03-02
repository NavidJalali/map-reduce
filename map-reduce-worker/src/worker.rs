use std::sync::Arc;

use map_reduce_core::grpc::{self, master_client::MasterClient};
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};
use tracing::info;

use crate::{
  error_tracker::ErrorTracker,
  heartbeat::start_heartbeat_fiber,
  shutdown::{shutdown_manager::ShutdownManager, shutdown_reason::ShutdownReason},
  task_puller::start_task_puller_fiber,
  worker_config::WorkerConfig,
};

pub struct WorkerImpl {
  client: Arc<Mutex<MasterClient<Channel>>>,
  config: WorkerConfig,
  shutdown: ShutdownManager<ShutdownReason>,
  error_tracker: Arc<RwLock<ErrorTracker>>,
}

impl WorkerImpl {
  async fn make_client(
    config: &WorkerConfig,
  ) -> Result<MasterClient<Channel>, Box<dyn std::error::Error>> {
    let url = format!("http://{}", config.master_address.0);
    let client = MasterClient::connect(Endpoint::from_shared(url)?.tcp_nodelay(true)).await?;
    Ok(client)
  }

  async fn register(
    client: &Arc<Mutex<MasterClient<Channel>>>,
    config: &WorkerConfig,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let files = config.list_files_in_directory().await?;

    client
      .lock()
      .await
      .register_worker(tonic::Request::new(grpc::RegisterWorkerRequest {
        locally_stored_chunks: files.iter().map(|(id, _)| *id as u32).collect(),
      }))
      .await?;

    Ok(())
  }

  pub async fn new(config: WorkerConfig) -> Result<Self, Box<dyn std::error::Error>> {
    info!("Starting worker with config: {:?}", config);

    let client = Arc::new(Mutex::new(Self::make_client(&config).await?));

    // Register this worker with the master
    Self::register(&client, &config).await?;

    let shutdown_manager = ShutdownManager::new();

    let error_tracker = Arc::new(RwLock::new(ErrorTracker::default()));

    start_heartbeat_fiber(
      client.clone(),
      config.heartbeat_interval,
      config.max_error_tolerance,
      error_tracker.clone(),
      shutdown_manager.shutdown.clone(),
    )
    .await;

    start_task_puller_fiber(
      client.clone(),
      config.max_error_tolerance,
      error_tracker.clone(),
      shutdown_manager.shutdown.clone(),
    )
    .await;

    Ok(WorkerImpl {
      client,
      config,
      shutdown: shutdown_manager,
      error_tracker,
    })
  }

  pub async fn await_shutdown(self) -> ShutdownReason {
    self
      .shutdown
      .await_shutdown()
      .await
      .expect("Failed to await shutdown")
  }
}
