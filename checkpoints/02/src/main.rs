use tinykv_lab::memory::BasicStore;
fn main() {
    let mut store = BasicStore::default();
    store.set("name".to_owned(), "Rajesh".to_owned());
    println!("{}", store.get("name").unwrap_or("missing"));
    println!("{}", store.delete("name"));
}
