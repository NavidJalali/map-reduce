use crate::{
    config::MasterConfig,
    dfs::DFS,
    input_file_chunk::InputFileChunk,
    state::{State, WorkerInfo},
};
use map_reduce_core::{grpc::master_server::Master, *};
use std::ops::Sub;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Debug)]
pub struct MasterImpl<FileSystem> {
    config: MasterConfig,
    state: Arc<RwLock<State>>,
    file_system: FileSystem,
}

impl<FileSystem: DFS> MasterImpl<FileSystem> {
    pub async fn new(config: MasterConfig, file_system: FileSystem) -> Self {
        let chunks = file_system
            .get_chunks()
            .await
            .expect("Failed to get chunks");

        let state = Arc::new(RwLock::new(State::AwaitingWorkers {
            workers: HashMap::new(),
            files: chunks,
        }));

        let moved_state = state.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(config.cleanup_interval).await;
                let mut state = moved_state.write().await;
                match &mut *state {
                    State::AwaitingWorkers { workers, .. } => {
                        let last_allowed_hearbeat =
                            SystemTime::now().sub(config.max_heartbeat_delay);

                        workers.retain(|_, info| {
                            let keep = info.last_heartbeat > last_allowed_hearbeat;
                            if !keep {
                                info!("Removing worker {:?} due to inactivity", info);
                            }
                            keep
                        });
                    }
                }
            }
        });

        Self {
            config,
            state,
            file_system,
        }
    }
}

#[tonic::async_trait]
impl<FileSystem: DFS + Send + Sync + 'static> Master for MasterImpl<FileSystem> {
    async fn register_worker(
        &self,
        request: Request<grpc::RegisterWorkerRequest>,
    ) -> Result<Response<grpc::RegisterWorkerResponse>, Status> {
        info!(
            "Received worker registration request {:?} from {:?}",
            request,
            request.remote_addr()
        );
        let address = Address(request.remote_addr().unwrap());
        let locally_stored_chunks = request
            .into_inner()
            .locally_stored_chunks
            .iter()
            .map(|&id| InputFileChunk { id })
            .collect::<Vec<_>>();

        let locally_stored_chunks = locally_stored_chunks.as_slice();

        let locally_stored_chunks = Arc::from(locally_stored_chunks);

        let info = WorkerInfo {
            address: address,
            last_heartbeat: SystemTime::now(),
            locally_stored_chunks: locally_stored_chunks,
        };

        let mut state = self.state.write().await;

        info!("Current state: {:?}", state);

        match &mut *state {
            State::AwaitingWorkers { workers, .. } => {
                workers.insert(address, info);
                if workers.len() == self.config.num_workers {
                    // Transition to the next state
                    info!("All workers have registered. Starting the process.");
                }
            }
        }

        info!("Current state: {:?}", state);

        Ok(Response::new(grpc::RegisterWorkerResponse {}))
    }

    async fn send_heartbeat(
        &self,
        request: Request<grpc::SendHeartbeatRequest>,
    ) -> Result<Response<grpc::SendHeartbeatResponse>, Status> {
        info!(
            "Received heartbeat request {:?} from {:?}",
            request,
            request.remote_addr()
        );
        let address = Address(request.remote_addr().unwrap());
        let mut state = self.state.write().await;

        match &mut *state {
            State::AwaitingWorkers { workers, .. } => {
                if let Some(info) = workers.get_mut(&address) {
                    info.last_heartbeat = SystemTime::now();
                }
            }
        }

        Ok(Response::new(grpc::SendHeartbeatResponse {}))
    }

    async fn get_task(
        &self,
        request: Request<grpc::GetTaskRequest>,
    ) -> Result<Response<grpc::GetTaskResponse>, Status> {
        info!(
            "Received task request {:?} from {:?}",
            request,
            request.remote_addr()
        );
        Ok(Response::new(grpc::GetTaskResponse {
            instruction: Some(grpc::get_task_response::Instruction::Backoff(
                grpc::get_task_response::Backoff {
                    duration: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                },
            )),
        }))
    }

    async fn report_task_completion(
        &self,
        request: Request<grpc::ReportTaskCompletionRequest>,
    ) -> Result<Response<grpc::ReportTaskCompletionResponse>, Status> {
        info!(
            "Received task completion report {:?} from {:?}",
            request,
            request.remote_addr()
        );
        Ok(Response::new(grpc::ReportTaskCompletionResponse {}))
    }

    async fn debug(
        &self,
        request: Request<grpc::DebugRequest>,
    ) -> Result<Response<grpc::DebugResponse>, Status> {
        info!(
            "Received debug request {:?} from {:?}",
            request,
            request.remote_addr()
        );
        let state = self.state.read().await;

        let proto_state = match &*state {
            State::AwaitingWorkers { workers, .. } => grpc::debug_response::State::AwaitingWorkers(
                grpc::debug_response::AwaitingWorkers {
                    workers: workers
                        .iter()
                        .map(|(address, info)| {
                            let duration = info
                                .last_heartbeat
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards");
                            (
                                address.0.to_string(),
                                grpc::debug_response::Worker {
                                    address: address.0.to_string(),
                                    last_heartbeat: Some(prost_types::Timestamp {
                                        seconds: duration.as_secs() as i64,
                                        nanos: duration.subsec_nanos() as i32,
                                    }),
                                    locally_stored_chunks: info
                                        .locally_stored_chunks
                                        .iter()
                                        .map(|chunk| chunk.id)
                                        .collect::<Vec<_>>(),
                                },
                            )
                        })
                        .collect(),
                },
            ),
        };

        let response = grpc::DebugResponse {
            state: Some(proto_state),
        };

        Ok(Response::new(response))
    }
}
