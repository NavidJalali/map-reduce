use futures_util::StreamExt;
use map_reduce_core::grpc::{
  worker_server::Worker, ListFilesRequest, ListFilesResponse, StreamFileRequest, StreamFileResponse,
};
use std::{pin::Pin, sync::Arc};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::file_system::FileSystem;

pub struct WorkerServerImpl<FS> {
  file_system: Arc<FS>,
}

impl<FS: FileSystem + Sync + Send + 'static> WorkerServerImpl<FS> {
  pub fn new(file_system: Arc<FS>) -> Self {
    Self { file_system }
  }
}

#[tonic::async_trait]
impl<FS: FileSystem + Sync + Send + 'static> Worker for WorkerServerImpl<FS> {
  async fn list_files(
    &self,
    _: Request<ListFilesRequest>,
  ) -> Result<Response<ListFilesResponse>, Status> {
    let file_system_information = self.file_system.file_system_information().await?;
    let list_files_response = ListFilesResponse {
      file_system_information: Some(file_system_information),
    };
    Ok(Response::new(list_files_response))
  }

  type StreamFileStream = Pin<Box<dyn Stream<Item = Result<StreamFileResponse, Status>> + Send>>;

  async fn stream_file(
    &self,
    request: Request<StreamFileRequest>,
  ) -> Result<Response<Self::StreamFileStream>, Status> {
    let StreamFileRequest { file, offset } = request.into_inner();
    let file = file.ok_or(Status::invalid_argument("file is required"))?;

    match file {
      map_reduce_core::grpc::stream_file_request::File::InputFile(input_file_id) => {
        let stream: Self::StreamFileStream = self
          .file_system
          .read_input_file(input_file_id, offset as usize)
          .await
          .map_err(|error| Status::internal(error.to_string()))?
          .map(|result| {
            result
              .map(|bytes| StreamFileResponse {
                chunk: bytes.into(),
                offset: 0,
              })
              .map_err(|error| Status::internal(error.to_string()))
          })
          .boxed();

        Ok(Response::new(stream))
      }
      map_reduce_core::grpc::stream_file_request::File::OutputFile(output_file_id) => todo!(),
    }
  }
}
