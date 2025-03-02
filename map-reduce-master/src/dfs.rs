use core::str;
use std::io::Error;

use crate::input_file_chunk::InputFileChunk;

pub trait DFS {
    async fn get_chunks(&self) -> Result<Vec<InputFileChunk>, Error>;
}

pub struct LocalFileSystem<'a> {
    root: &'a str,
}

impl LocalFileSystem<'_> {
    pub fn new(root: &str) -> LocalFileSystem {
        LocalFileSystem { root }
    }
}

impl DFS for LocalFileSystem<'_> {
    async fn get_chunks(&self) -> Result<Vec<InputFileChunk>, Error> {
        let mut dir = tokio::fs::read_dir(&self.root).await?;
        let mut chunks = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            let file_name = path
                .file_name()
                .and_then(|os_str| os_str.to_str())
                .and_then(|s| s.strip_prefix("chunk-"))
                .and_then(|s| s.strip_suffix(".txt"))
                .and_then(|s| s.parse::<u32>().ok())
                .map(|chunk_number| InputFileChunk { id: chunk_number })
                .ok_or(Error::from(std::io::ErrorKind::InvalidData))?;

            chunks.push(file_name);
        }
        Ok(chunks)
    }
}
