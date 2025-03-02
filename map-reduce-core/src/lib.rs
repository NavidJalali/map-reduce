use std::{fmt::Debug, net::SocketAddr};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct Address(pub SocketAddr);

impl Debug for Address {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.0.ip(), self.0.port())
  }
}

pub mod grpc {
  tonic::include_proto!("io.reverie.mapreduce");
}
