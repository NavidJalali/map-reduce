use std::{sync::Arc, time::Duration};

use map_reduce_core::grpc::{self, master_client::MasterClient};
use tokio::{
    sync::{Mutex, RwLock},
    time::sleep,
};
use tonic::transport::Channel;
use tracing::{error, info, warn};

use crate::{
    error_tracker::ErrorTracker,
    shutdown::{shutdown_manager::Shutdown, shutdown_reason::ShutdownReason},
};

async fn master_signalled_shutdown(
    shutdown: Arc<Shutdown<ShutdownReason>>,
    reason: ShutdownReason,
) {
    error!("Master signalled shutdown, shutting down");

    tokio::spawn(async move {
        shutdown.trigger(reason).await;
    });
}

pub async fn start_task_puller_fiber(
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
                        Some(instruction) => match instruction {
                            grpc::get_task_response::Instruction::Shutdown(shutdown_task) => {
                                if let Some(reason) = shutdown_task.reason {
                                    match reason {
                                        grpc::get_task_response::shutdown::Reason::Done(_) => {
                                            master_signalled_shutdown(
                                                shutdown.clone(),
                                                ShutdownReason::Done,
                                            )
                                            .await;
                                            break;
                                        }
                                        grpc::get_task_response::shutdown::Reason::Error(error) => {
                                            master_signalled_shutdown(
                                                shutdown.clone(),
                                                ShutdownReason::MasterError(Some(error.message)),
                                            )
                                            .await;
                                            break;
                                        }
                                    }
                                } else {
                                    warn!("Received shutdown instruction without reason");

                                    master_signalled_shutdown(
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
                        },
                        None => warn!("Received empty task"),
                    }
                }
                Err(status) => {
                    error!("Failed to get task: {:?}", status);

                    let should_shutdown = {
                        let mut err = error_tracker.write().await;
                        err.task_puller.push(status);

                        // Check condition and clone errors inside the scope
                        if err.task_puller.len() > max_error_tolerance {
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
                    info!("Aborted task puller fiber");
                    let exit = fiber.await;
                    info!("task puller exited: {:?}", exit);
                })
            },
            "task puller".to_string(),
        )
        .await;
}
