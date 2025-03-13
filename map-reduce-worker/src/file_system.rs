use std::{future::Future, io, path::Path, pin::Pin, sync::Arc};

use map_reduce_core::grpc::{FileSystemInformation, InputFileId, OutputFileId};
use tokio::{
  fs::File,
  io::{AsyncSeekExt, BufReader},
};
use tokio_stream::Stream;
use tokio_util::io::ReaderStream;

pub struct InputFileChunk {
  pub id: u32,
}

impl InputFileChunk {
  pub fn new(id: u32) -> Self {
    Self { id }
  }

  pub fn from_file_name(name: &str) -> Self {
    let id = name
      .split('-')
      .nth(1)
      .expect("Invalid file name: missing chunk number")
      .split('.')
      .next()
      .expect("Invalid file name: missing chunk number")
      .parse::<u32>()
      .expect("Invalid chunk number");
    Self { id }
  }
}

pub struct OutputFileChunk {
  id: u32,
  partition: u32,
}

impl OutputFileChunk {
  pub fn new(id: u32, partition: u32) -> Self {
    Self { id, partition }
  }

  pub fn from_file_name(name: &str) -> Self {
    let mut parts = name.split('-');
    let id = parts
      .nth(1)
      .expect("Invalid file name: missing chunk number")
      .parse::<u32>()
      .expect("Invalid chunk number");
    let partition = parts
      .next()
      .expect("Invalid file name: missing partition number")
      .split('.')
      .next()
      .expect("Invalid file name: missing partition number")
      .parse::<u32>()
      .expect("Invalid partition number");
    Self { id, partition }
  }
}

pub trait FileSystem {
  fn list_input_files(&self) -> impl Future<Output = io::Result<Vec<InputFileChunk>>> + Send;

  fn list_output_files(&self) -> impl Future<Output = io::Result<Vec<OutputFileChunk>>> + Send;

  fn file_system_information(
    &self,
  ) -> impl Future<Output = io::Result<FileSystemInformation>> + Send;

  fn read_input_file(
    &self,
    input_file_id: InputFileId,
    offset: usize,
  ) -> impl Future<
    Output = tokio::io::Result<Pin<Box<dyn Stream<Item = tokio::io::Result<bytes::Bytes>> + Send>>>,
  > + Send;
  
}

pub struct LocalFileSystem {
  input_directory: Arc<str>,
  output_directory: Arc<str>,
}

impl LocalFileSystem {
  pub fn new(input_directory: &str, output_directory: &str) -> LocalFileSystem {
    LocalFileSystem {
      input_directory: Arc::from(input_directory),
      output_directory: Arc::from(output_directory),
    }
  }

  pub async fn read_input_file<P: AsRef<Path>>(
    file_path: P,
    offset: u64,
  ) -> tokio::io::Result<impl Stream<Item = tokio::io::Result<bytes::Bytes>>> {
    let mut file = File::open(file_path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let reader = BufReader::new(file);
    let stream = ReaderStream::new(reader);

    Ok(stream)
  }
}

impl FileSystem for LocalFileSystem {
  async fn list_input_files(&self) -> Result<Vec<InputFileChunk>, std::io::Error> {
    let mut read_dir = tokio::fs::read_dir(&self.input_directory.as_ref()).await?;

    let mut files = vec![];

    while let Some(entry) = read_dir.next_entry().await? {
      if entry.file_type().await?.is_file() {
        let name = entry.file_name();
        // name should be in the format "chunk-<number>.txt"
        let name = name.to_str().expect("Invalid file name: not UTF-8");
        let input_file_chunk = InputFileChunk::from_file_name(name);
        files.push(input_file_chunk);
      }
    }

    Ok(files)
  }

  async fn list_output_files(&self) -> Result<Vec<OutputFileChunk>, std::io::Error> {
    let mut read_dir = tokio::fs::read_dir(&self.output_directory.as_ref()).await?;

    let mut files = vec![];

    while let Some(entry) = read_dir.next_entry().await? {
      if entry.file_type().await?.is_file() {
        let name = entry.file_name();
        // name should be in the format "chunk-<number>-<partition>.txt"
        let name = name.to_str().expect("Invalid file name: not UTF-8");
        let output_file_chunk = OutputFileChunk::from_file_name(name);
        files.push(output_file_chunk);
      }
    }

    Ok(files)
  }

  async fn file_system_information(&self) -> Result<FileSystemInformation, std::io::Error> {
    let input = self.list_input_files().await?;
    let output = self.list_output_files().await?;
    let result = FileSystemInformation {
      input_files: input
        .iter()
        .map(|chunk| InputFileId { id: chunk.id })
        .collect(),
      output_files: output
        .iter()
        .map(|chunk| OutputFileId {
          id: chunk.id,
          partition: chunk.partition,
        })
        .collect(),
    };

    Ok(result)
  }

  async fn read_input_file(
    &self,
    input_file_id: InputFileId,
    offset: usize,
  ) -> tokio::io::Result<Pin<Box<dyn Stream<Item = tokio::io::Result<bytes::Bytes>> + Send>>> {
    let file_path = format!("{}/chunk-{}.txt", self.input_directory, input_file_id.id);
    let offset = offset as u64;
    let stream = LocalFileSystem::read_input_file(file_path, offset).await?;
    Ok(Box::pin(stream))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_input_file_chunk_from_file_name() {
    let file_name = "chunk-1.txt";
    let input_file_chunk = InputFileChunk::from_file_name(file_name);
    assert_eq!(input_file_chunk.id, 1);
  }

  #[test]
  fn test_output_file_chunk_from_file_name() {
    let file_name = "chunk-1-2.txt";
    let output_file_chunk = OutputFileChunk::from_file_name(file_name);
    assert_eq!(output_file_chunk.id, 1);
    assert_eq!(output_file_chunk.partition, 2);
  }
}
