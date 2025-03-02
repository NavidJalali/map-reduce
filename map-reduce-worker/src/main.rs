use std::{path::PathBuf, sync::Arc};

use error_tracker::ErrorTracker;
use shutdown_manager::{Shutdown, ShutdownManager};
use tokio::{
    sync::{Mutex, RwLock},
    time::{sleep, Duration},
};

use map_reduce_core::{
    grpc::{self, master_client::MasterClient},
    Address,
};
use tonic::transport::{Channel, Endpoint};
use tracing::{error, info, warn};
use worker_config::WorkerConfig;

mod error_tracker;
mod shutdown_manager;
mod worker_config;

struct WorkerImpl {
    client: Arc<Mutex<MasterClient<Channel>>>,
    config: WorkerConfig,
    shutdown: ShutdownManager<ShutdownReason>,
    error_tracker: Arc<RwLock<ErrorTracker>>,
}

#[derive(Debug)]
enum ShutdownReason {
    Done,
    MasterError(Option<String>),
    WorkerError(ErrorTracker),
}

impl WorkerImpl {
    async fn make_client(
        config: &WorkerConfig,
    ) -> Result<MasterClient<Channel>, Box<dyn std::error::Error>> {
        let url = format!("http://{}", config.master_address.0);
        let client = MasterClient::connect(Endpoint::from_shared(url)?.tcp_nodelay(true)).await?;
        Ok(client)
    }

    async fn error_threshold_exceeded(
        shutdown: Arc<Shutdown<ShutdownReason>>,
        reason: ShutdownReason,
    ) {
        error!("Reached max error tolerance, shutting down");

        tokio::spawn(async move {
            shutdown.trigger(reason).await;
        });
    }

    async fn master_signalled_shutdown(
        shutdown: Arc<Shutdown<ShutdownReason>>,
        reason: ShutdownReason,
    ) {
        error!("Master signalled shutdown, shutting down");

        tokio::spawn(async move {
            shutdown.trigger(reason).await;
        });
    }

    async fn start_heartbeat_fiber(
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
                let result = client
                    .lock()
                    .await
                    .send_heartbeat(tonic::Request::new(grpc::SendHeartbeatRequest {}))
                    .await;

                match result {
                    Ok(_) => {
                        info!("Successfully sent heartbeat");
                        let mut err = error_tracker.write().await;
                        err.heartbeat.clear();
                    }
                    Err(e) => {
                        error!("Failed to send heartbeat: {:?}", e);

                        let should_shutdown = {
                            let mut err = error_tracker.write().await;
                            err.heartbeat.push(e);

                            // Check condition and clone errors inside the scope
                            if err.heartbeat.len() > max_error_tolerance {
                                Some(err.clone())
                            } else {
                                None
                            }
                        };

                        if let Some(errors) = should_shutdown {
                            Self::error_threshold_exceeded(
                                shutdown.clone(),
                                ShutdownReason::WorkerError(errors),
                            )
                            .await;

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

    async fn start_task_puller_fiber(
        client: Arc<Mutex<MasterClient<Channel>>>,
        max_error_tolerance: usize,
        error_tracker: Arc<RwLock<ErrorTracker>>,
        shutdown: Arc<Shutdown<ShutdownReason>>,
    ) {
        let finalizer_shutdown = shutdown.clone();
        let fiber = tokio::spawn(async move {
            loop {
                info!("Requesting task from master");

                let task = client
                    .lock()
                    .await
                    .get_task(tonic::Request::new(grpc::GetTaskRequest {}))
                    .await;

                match task {
                    Ok(task) => {
                        let mut err = error_tracker.write().await;
                        err.task_puller.clear();

                        let task = task.into_inner();

                        info!("Received task from master: {:?}", task);
                        match task.instruction {
                            Some(instruction) => {
                                match instruction {
                                    grpc::get_task_response::Instruction::Shutdown(
                                        shutdown_task,
                                    ) => {
                                        if let Some(reason) = shutdown_task.reason {
                                            match reason {
                                            grpc::get_task_response::shutdown::Reason::Done(done) => {
                                                Self::master_signalled_shutdown(
                                                    shutdown.clone(),
                                                    ShutdownReason::Done,
                                                )
                                                .await;
                                                break;
                                            }
                                            grpc::get_task_response::shutdown::Reason::Error(error) => {
                                                Self::master_signalled_shutdown(
                                                    shutdown.clone(),
                                                    ShutdownReason::MasterError(Some(error.message)),
                                                )
                                                .await;
                                                break;
                                            }
                                        }
                                        } else {
                                            warn!("Received shutdown instruction without reason");

                                            Self::master_signalled_shutdown(
                                                shutdown.clone(),
                                                ShutdownReason::MasterError(None),
                                            )
                                            .await;

                                            break;
                                        }
                                    }
                                    grpc::get_task_response::Instruction::Backoff(backoff) => {
                                        if let Some(duration) = backoff.duration {
                                            let duration = Duration::new(
                                                duration.seconds as u64,
                                                duration.nanos as u32,
                                            );

                                            sleep(duration).await;
                                        } else {
                                            warn!("Received backoff instruction without duration");
                                        }
                                    }
                                    grpc::get_task_response::Instruction::Task(task) => {
                                        unimplemented!()
                                    }
                                }
                            }
                            None => warn!("Received empty task"),
                        }
                    }
                    Err(e) => {
                        error!("Failed to get task: {:?}", e);

                        let should_shutdown = {
                            let mut err = error_tracker.write().await;
                            err.task_puller.push(e);

                            // Check condition and clone errors inside the scope
                            if err.task_puller.len() > max_error_tolerance {
                                Some(err.clone())
                            } else {
                                None
                            }
                        };

                        if let Some(errors) = should_shutdown {
                            Self::error_threshold_exceeded(
                                shutdown.clone(),
                                ShutdownReason::WorkerError(errors),
                            )
                            .await;

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
                        info!("Aborted task puller fiber");
                        let exit = fiber.await;
                        info!("task puller exited: {:?}", exit);
                    })
                },
                "task puller".to_string(),
            )
            .await;
    }

    async fn register(
        client: &Arc<Mutex<MasterClient<Channel>>>,
        config: &WorkerConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let files: Vec<(usize, PathBuf)> = config.list_files_in_directory().await?;

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

        Self::start_heartbeat_fiber(
            client.clone(),
            config.heartbeat_interval,
            config.max_error_tolerance,
            error_tracker.clone(),
            shutdown_manager.shutdown.clone(),
        )
        .await;

        Self::start_task_puller_fiber(
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
        self.shutdown
            .await_shutdown()
            .await
            .expect("Failed to await shutdown")
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
