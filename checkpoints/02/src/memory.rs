//! The first in-memory store. This file is retired at checkpoint 04.
use std::collections::HashMap;

#[derive(Default)]
pub struct BasicStore {
    entries: HashMap<String, String>,
}
impl BasicStore {
    pub fn set(&mut self, key: String, value: String) {
        self.entries.insert(key, value);
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries.get(key).map(String::as_str)
    }
    pub fn delete(&mut self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn set_get_replace_and_delete() {
        let mut store = BasicStore::default();
        assert_eq!(store.get("name"), None);
        store.set("name".into(), "Rajesh".into());
        assert_eq!(store.get("name"), Some("Rajesh"));
        store.set("name".into(), "Radhika".into());
        assert_eq!(store.get("name"), Some("Radhika"));
        assert!(store.delete("name"));
        assert!(!store.delete("name"));
    }
}
