use std::{collections::HashMap, time::SystemTime};

use map_reduce_core::Address;

#[derive(Debug, Clone, Copy)]
pub struct WorkerInfo {
    pub last_heartbeat: SystemTime,
    pub address: Address,
}

#[derive(Debug)]
pub enum State {
    // When the master starts, it waits for a certain number of workers to register.
    AwaitingWorkers {
        workers: HashMap<Address, WorkerInfo>,
    },
}
