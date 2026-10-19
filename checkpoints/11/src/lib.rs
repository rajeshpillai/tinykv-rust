//! A small, in-memory teaching cache. This is not a Redis-compatible server.
#![forbid(unsafe_code)]

mod connection;
pub mod database;
mod framing;
mod pool;
pub mod protocol;
mod server;
pub mod shutdown;
pub mod store;

pub use connection::ConnectionLimits;
pub use server::{ServerConfig, ServerReport, serve};
pub use shutdown::Shutdown;
pub use store::Store;
