use map_reduce_core::Address;
use tokio::time::Duration;
use tracing::info;
use worker::WorkerImpl;
use worker_config::WorkerConfig;

mod error_tracker;
mod heartbeat;
mod shutdown;
mod task_puller;
mod worker;
mod worker_config;

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
