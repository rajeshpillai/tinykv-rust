fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = tinykv_lab::protocol::parse("SETEX greeting 5 hello Rust")?;
    println!("{command:?}");
    Ok(())
}
