use std::{collections::HashMap, sync::Arc, time::SystemTime};

use map_reduce_core::Address;

#[derive(Debug, Clone, Copy)]
pub struct InputFileChunk {
    pub id: u32,
}

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
    },
}
