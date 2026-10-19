use std::{
    error::Error,
    time::{Duration, Instant},
};
use tinykv_lab::database::{Database, StoreLimits};

fn main() -> Result<(), Box<dyn Error>> {
    let mut database = Database::new(StoreLimits::default())?;
    let start = Instant::now();
    print!("{}", database.apply("PSETEX lesson 1000 Rust", start));
    print!(
        "{}",
        database.apply("GET lesson", start + Duration::from_millis(999))
    );
    print!(
        "{}",
        database.apply("GET lesson", start + Duration::from_millis(1000))
    );
    Ok(())
}
