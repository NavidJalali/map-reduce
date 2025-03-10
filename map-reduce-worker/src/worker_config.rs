use std::time::Duration;

use map_reduce_core::Address;

#[derive(Debug)]
pub struct WorkerConfig {
  pub master_address: Address,
  pub heartbeat_interval: Duration,
  pub input_directory: String,
  pub output_directory: String,
  pub max_error_tolerance: usize,
}
