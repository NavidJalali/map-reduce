use std::sync::Arc;

use map_reduce_core::grpc::{self, master_client::MasterClient};
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint, Server};
use tracing::info;

use crate::{
  error_tracker::ErrorTracker,
  file_system::{FileSystem, LocalFileSystem},
  heartbeat::start_heartbeat_fiber,
  shutdown::{shutdown_manager::ShutdownManager, shutdown_reason::ShutdownReason},
  task_puller::start_task_puller_fiber,
  worker_config::WorkerConfig,
  worker_server::WorkerServerImpl,
};

pub struct WorkerImpl {
  client: Arc<Mutex<MasterClient<Channel>>>,
  config: WorkerConfig,
  shutdown: ShutdownManager<ShutdownReason>,
  error_tracker: Arc<RwLock<ErrorTracker>>,
  pub file_system: Arc<LocalFileSystem>,
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
    file_system: &LocalFileSystem,
  ) -> Result<(), Box<dyn std::error::Error>> {
    client
      .lock()
      .await
      .register_worker(tonic::Request::new(grpc::RegisterWorkerRequest {
        file_system_information: Some(file_system.file_system_information().await?),
      }))
      .await?;

    Ok(())
  }

  pub async fn new(config: WorkerConfig) -> Result<Self, Box<dyn std::error::Error>> {
    info!("Starting worker with config: {:?}", config);

    let client = Arc::new(Mutex::new(Self::make_client(&config).await?));
    let file_system = Arc::new(LocalFileSystem::new(
      &config.input_directory,
      &config.output_directory,
    ));

    // Register this worker with the master
    Self::register(&client, file_system.as_ref()).await?;

    let shutdown_manager = ShutdownManager::new();

    let error_tracker = Arc::new(RwLock::new(ErrorTracker::default()));

    start_heartbeat_fiber(
      file_system.clone(),
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

    let grpc_server = WorkerServerImpl::new(file_system.clone());

    let address = "0.0.0.0:50052".parse()?;

    let router = Server::builder().add_service(grpc::worker_server::WorkerServer::new(grpc_server));

    let server_handle = tokio::spawn(async move {
      router.serve(address).await.unwrap();
    });

    shutdown_manager
      .shutdown
      .register_shutdown_task(
        || {
          Box::pin(async move {
            server_handle.abort();
          })
        },
        "grpc server",
      )
      .await;

    Ok(WorkerImpl {
      client,
      config,
      shutdown: shutdown_manager,
      error_tracker,
      file_system,
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
