use std::{collections::HashMap, path::PathBuf, sync::Arc, vec};

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

#[derive(Debug)]
pub struct WorkerConfig {
    pub master_address: Address,
    pub heartbeat_interval: Duration,
    pub input_directory: String,
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
    pub client: Arc<Mutex<MasterClient<Channel>>>,
}

impl WorkerImpl {
    pub async fn new(config: WorkerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let url = format!("http://{}", config.master_address.0);
        let endpoint = Endpoint::from_shared(url)?.tcp_nodelay(true);
        let client = Arc::new(Mutex::new(MasterClient::connect(endpoint).await?));

        println!("Starting worker with config: {:?}", config);

        let heartbeat_client = client.clone();

        // List all files in the input directory
        let files = config.list_files_in_directory().await?;
        println!("Files in input directory: {:?}", files);

        // Register with the master
        let mut guard = heartbeat_client.lock().await;

        guard
            .register_worker(tonic::Request::new(grpc::RegisterWorkerRequest {
                locally_stored_chunks: files.iter().map(|(id, _)| *id as u32).collect(),
            }))
            .await?;

        drop(guard);

        // Start the heartbeat loop
        tokio::spawn(async move {
            loop {
                sleep(config.heartbeat_interval).await;
                let result = heartbeat_client
                    .lock()
                    .await
                    .send_heartbeat(tonic::Request::new(grpc::SendHeartbeatRequest {}))
                    .await;

                match result {
                    Ok(_) => info!("Successfully sent heartbeat"),
                    Err(e) => error!("Failed to send heartbeat: {:?}", e),
                }
            }
        });

        Ok(WorkerImpl { client })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig {
        master_address: Address("0.0.0.0:50051".parse()?),
        heartbeat_interval: Duration::from_secs(10).into(),
        input_directory: std::env::var("INPUT_DIRECTORY").expect("INPUT_DIRECTORY not set"),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let worker = WorkerImpl::new(config).await?;

    // prevent shut down until input from stdin

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;

    Ok(())
}
