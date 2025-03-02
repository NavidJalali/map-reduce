use map_reduce_core::Address;
use tokio::time::Duration;

#[derive(Debug)]
pub struct MasterConfig {
  // Number of worker registrations the master waits for before starting the process.
  pub num_workers: usize,
  // how long the master waits for a heartbeat before considering a worker dead.
  pub max_heartbeat_delay: Duration,
  // Interval at which the master cleans up dead workers.
  pub cleanup_interval: Duration,
  // Address to bind the master server to.
  pub address: Address,
}
