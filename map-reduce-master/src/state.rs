use std::{collections::HashMap, sync::Arc, time::SystemTime};

use crate::{input_file_chunk::InputFileChunk, output_file_chunk::OutputFileChunk};
use map_reduce_core::{
  grpc::{FileSystemInformation, InputFileId, OutputFileId},
  Address,
};

#[derive(Debug, Clone)]
pub struct WorkerInfo {
  pub last_heartbeat: SystemTime,
  pub address: Address,
  pub input_files: Arc<[InputFileChunk]>,
  pub output_files: Arc<[OutputFileChunk]>,
}

impl WorkerInfo {
  pub fn file_system_information(&self) -> FileSystemInformation {
    FileSystemInformation {
      input_files: self
        .input_files
        .iter()
        .map(|f| InputFileId { id: f.id })
        .collect(),
      output_files: self
        .output_files
        .iter()
        .map(|f| OutputFileId {
          id: f.id,
          partition: f.partition,
        })
        .collect(),
    }
  }
}

#[derive(Debug)]
pub enum State {
  // When the master starts, it waits for a certain number of workers to register.
  AwaitingWorkers {
    workers: HashMap<Address, WorkerInfo>,
    files: Vec<InputFileChunk>,
  },
}

// TODO: Figure this out
// struct WorkInstance {
//   worker: Address,
//   started_at: SystemTime,
// }

// enum TaskState {
//   Unassigned,
//   InProgress { chunk: InputFileChunk },
// }
