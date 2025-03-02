use std::{collections::HashMap, sync::Arc, time::SystemTime};

use crate::input_file_chunk::InputFileChunk;
use map_reduce_core::Address;

#[derive(Debug, Clone)]
pub struct WorkerInfo {
  pub last_heartbeat: SystemTime,
  pub address: Address,
  pub locally_stored_chunks: Arc<[InputFileChunk]>,
}

#[derive(Debug)]
pub enum State {
  // When the master starts, it waits for a certain number of workers to register.
  AwaitingWorkers {
    workers: HashMap<Address, WorkerInfo>,
    files: Vec<InputFileChunk>,
  },
}
