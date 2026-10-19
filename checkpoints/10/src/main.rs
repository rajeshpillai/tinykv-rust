use std::{
    error::Error,
    io,
    net::{SocketAddr, TcpListener},
    process::ExitCode,
};
use tinykv_lab::{ServerConfig, Shutdown, serve};

fn start() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let address: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:7878".into())
        .parse()?;
    if args.next().is_some() || !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: tinykv_lab [loopback-ip:port]",
        )
        .into());
    }
    let listener = TcpListener::bind(address)?;
    let stop = Shutdown::default();
    let signal_stop = stop.clone();
    // The crate invokes this closure on its signal-handling thread.
    // The termination feature also handles SIGTERM and SIGHUP on Unix.
    ctrlc::set_handler(move || signal_stop.request())?;
    println!(
        "Listening on {}. Press Ctrl+C to stop.",
        listener.local_addr()?
    );
    let report = serve(listener, ServerConfig::default(), stop)?;
    println!(
        "Stopped. Accepted: {}; rejected: {}; connection errors: {}.",
        report.accepted_connections, report.rejected_connections, report.connection_errors
    );
    Ok(())
}

fn main() -> ExitCode {
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tinykv_lab: {error}");
            ExitCode::FAILURE
        }
    }
}
