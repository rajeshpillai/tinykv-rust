#![forbid(unsafe_code)]
pub mod database;
pub mod protocol;
pub mod store;
pub use store::Store;
pub mod shutdown;
pub use shutdown::Shutdown;
mod connection;
mod framing;
mod pool;

/// Temporary one-session demo. Removed when the full supervisor is added.
pub fn serve_one(listener: std::net::TcpListener) -> std::io::Result<()> {
    let store = std::sync::Arc::new(
        Store::new(database::StoreLimits::default()).map_err(std::io::Error::other)?,
    );
    let (stream, _) = listener.accept()?;
    connection::handle_connection(
        stream,
        std::time::Instant::now(),
        store,
        Shutdown::default(),
        connection::ConnectionLimits::default(),
    )
}
