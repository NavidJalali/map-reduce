use tokio::time::{sleep, Duration};

use map_reduce_core::{
    grpc::{self, master_client::MasterClient},
    Address,
};
use tonic::transport::{Channel, Endpoint};

#[derive(Debug)]
pub struct WorkerConfig {
    pub master_address: Address,
    pub heartbeat_interval: Duration,
    pub input_directory: String,
}

struct WorkerImpl {
    pub client: MasterClient<Channel>,
}

impl WorkerImpl {
    pub async fn new(config: WorkerConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let url = format!("http://{}", config.master_address.0);
        let endpoint = Endpoint::from_shared(url)?.tcp_nodelay(true);
        let mut client = MasterClient::connect(endpoint).await?;

        println!("Starting worker with config: {:?}", config);

        // Register with the master
        client
            .register_worker(tonic::Request::new(grpc::RegisterWorkerRequest {}))
            .await?;

        // List all files in the input directory
        let files = tokio::fs::read_dir(&config.input_directory).await?;

        println!("Files in input directory: {:?}", files);

        // Start the heartbeat loop
        // let self_id = config.self_id.clone();
        // tokio::spawn(async move {
        //     loop {
        //         sleep(config.heartbeat_interval).await;
        //         client
        //             .send_heartbeat(tonic::Request::new(grpc::SendHeartbeatRequest {}))
        //             .await
        //             .unwrap();
        //     }
        // });

        Ok(WorkerImpl { client })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig {
        master_address: Address("0.0.0.0:50051".parse()?),
        heartbeat_interval: Duration::from_secs(1).into(),
        input_directory: std::env::var("INPUT_DIRECTORY").unwrap_or_else(|_| "input".to_string()),
    };

    let worker = WorkerImpl::new(config).await?;

    Ok(())
}
