use config::MasterConfig;
use map_reduce_core::grpc;
use map_reduce_core::*;
use master::MasterImpl;
use tokio::time::Duration;
use tonic::transport::Server;

use grpc::master_server::MasterServer;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

mod config;
mod master;
mod state;
mod task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = MasterConfig {
        num_workers: 3,
        max_heartbeat_delay: Duration::from_secs(30),
        cleanup_interval: Duration::from_secs(10),
        address: Address("0.0.0.0:50051".parse()?),
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let address = config.address.clone();

    let master = MasterImpl::new(config);

    info!("Starting master server on {:?}", address);

    Server::builder()
        .add_service(MasterServer::new(master))
        .serve(address.0)
        .await?;
    Ok(())
}
