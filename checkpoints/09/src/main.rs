fn main() -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:7878")?;
    println!("One session on 127.0.0.1:7878. Send QUIT to finish.");
    tinykv_lab::serve_one(listener)
}
