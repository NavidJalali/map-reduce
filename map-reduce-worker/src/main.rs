use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    vec,
};

use shutdown_signal::ShutdownSignal;
use tokio::{
    sync::Mutex,
    time::{sleep, Duration},
};

use map_reduce_core::{
    grpc::{self, master_client::MasterClient},
    Address,
};
use tonic::transport::{Channel, Endpoint};
use tracing::{error, info};

mod shutdown_signal;

#[derive(Debug)]
pub struct WorkerConfig {
    pub master_address: Address,
    pub heartbeat_interval: Duration,
    pub input_directory: String,
    pub max_error_tolerance: usize,
}

impl WorkerConfig {
    pub async fn list_files_in_directory(&self) -> Result<Vec<(usize, PathBuf)>, std::io::Error> {
        let mut read_dir = tokio::fs::read_dir(&self.input_directory).await?;

        let mut files = vec![];

        while let Some(entry) = read_dir.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let name = entry.file_name();
                // name should be in the format "chunk-<number>.txt"
                let name = name.to_str().unwrap();
                let chunk_number = name
                    .split('-')
                    .nth(1)
                    .iter()
                    .flat_map(|s| s.split('.').next())
                    .flat_map(|s| s.parse::<usize>().ok())
                    .next()
                    .expect("Invalid chunk file name");

                files.push((chunk_number, entry.path()));
            }
        }

        Ok(files)
    }
}

struct WorkerImpl {
    client: Arc<Mutex<MasterClient<Channel>>>,
    config: WorkerConfig,
    shutdown: tokio::sync::oneshot::Receiver<ShutdownReason>,
    accumulated_errors: Arc<RwLock<AccumulatedErrors>>,
}

#[derive(Debug, Clone)]
struct AccumulatedErrors {
    heartbeat: Vec<tonic::Status>,
}

#[derive(Debug)]
enum ShutdownReason {
    Done,
    WorkerError(AccumulatedErrors),
}

impl WorkerImpl {
    pub async fn new(config: WorkerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let url = format!("http://{}", config.master_address.0);

        let client = Arc::new(Mutex::new(
            MasterClient::connect(Endpoint::from_shared(url)?.tcp_nodelay(true)).await?,
        ));

        let accumulated_errors = Arc::new(RwLock::new(AccumulatedErrors { heartbeat: vec![] }));

        info!("Starting worker with config: {:?}", config);

        let heartbeat_client = client.clone();
        let files = config.list_files_in_directory().await?;

        let (shutdown, shutdown_receiver) = ShutdownSignal::new();

        // Register with the master
        heartbeat_client
            .lock()
            .await
            .register_worker(tonic::Request::new(grpc::RegisterWorkerRequest {
                locally_stored_chunks: files.iter().map(|(id, _)| *id as u32).collect(),
            }))
            .await?;

        // Start the heartbeat loop
        let heartbeat_shutdown = shutdown.clone();
        let heartbeat_errors = accumulated_errors.clone();
        let heartbeat_fiber = tokio::spawn(async move {
            loop {
                sleep(config.heartbeat_interval).await;
                let result = heartbeat_client
                    .lock()
                    .await
                    .send_heartbeat(tonic::Request::new(grpc::SendHeartbeatRequest {}))
                    .await;

                match result {
                    Ok(_) => {
                        info!("Successfully sent heartbeat");
                        let mut err = heartbeat_errors
                            .write()
                            .expect("Failed to acquire lock for errors");

                        err.heartbeat.clear();
                    }
                    Err(e) => {
                        error!("Failed to send heartbeat: {:?}", e);

                        let should_shutdown = {
                            let mut err = heartbeat_errors
                                .write()
                                .expect("Failed to acquire lock for errors");

                            err.heartbeat.push(e);

                            // Check condition and clone errors inside the scope
                            if err.heartbeat.len() > config.max_error_tolerance {
                                Some(err.clone())
                            } else {
                                None
                            }
                        };

                        if let Some(errors) = should_shutdown {
                            error!("Reached max error tolerance, shutting down");

                            let die_now = heartbeat_shutdown.clone();
                            tokio::spawn(async move {
                                die_now.trigger(ShutdownReason::WorkerError(errors)).await;
                            });

                            break;
                        }
                    }
                }
            }
        });

        shutdown
            .register_shutdown_task(
                || {
                    Box::pin(async {
                        heartbeat_fiber.abort();
                        info!("Aborted heartbeat fiber");
                        let exit = heartbeat_fiber.await;
                        info!("heartbeat exited: {:?}", exit);
                    })
                },
                "heartbeat".to_string(),
            )
            .await;

        let task_client = client.clone();
        let task_pull_fiber = tokio::spawn(async move {
            loop {
                //  let foo = task_client.lock().await.get_task(tonic::Request::new(grpc::GetTaskRequest {})).await;
                sleep(Duration::from_secs(5)).await;
            }
        });

        shutdown
            .register_shutdown_task(
                || {
                    Box::pin(async {
                        task_pull_fiber.abort();
                        info!("Aborted task puller fiber");
                        let exit = task_pull_fiber.await;
                        info!("task puller exited: {:?}", exit);
                    })
                },
                "task puller".to_string(),
            )
            .await;

        Ok(WorkerImpl {
            client,
            config,
            shutdown: shutdown_receiver,
            accumulated_errors,
        })
    }

    pub async fn await_shutdown(self) -> ShutdownReason {
        self.shutdown.await.expect("Failed to await shutdown")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig {
        master_address: Address("0.0.0.0:50051".parse()?),
        heartbeat_interval: Duration::from_secs(3).into(),
        input_directory: std::env::var("INPUT_DIRECTORY").expect("INPUT_DIRECTORY not set"),
        max_error_tolerance: 3,
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let worker = WorkerImpl::new(config).await?;

    let shutdown_reason = worker.await_shutdown().await;

    info!("Shutting down due to: {:?}", shutdown_reason);

    Ok(())
}
