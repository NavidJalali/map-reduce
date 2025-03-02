use std::{path::PathBuf, time::Duration};

use map_reduce_core::Address;

#[derive(Debug)]
pub struct WorkerConfig {
  pub master_address: Address,
  pub heartbeat_interval: Duration,
  pub input_directory: String,
  pub max_error_tolerance: usize,
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
