//! A thread-safe boundary around the single-threaded database.
use crate::{
    database::{Database, StoreError, StoreLimits},
    protocol::{self, Reply},
};
use std::{sync::Mutex, time::Instant};

#[derive(Debug)]
pub struct Store {
    inner: Mutex<Database>,
}

impl Store {
    pub fn new(limits: StoreLimits) -> Result<Self, StoreError> {
        Ok(Self {
            inner: Mutex::new(Database::new(limits)?),
        })
    }

    pub fn execute_line(&self, line: &str) -> Result<Reply, StoreError> {
        // Validate before taking the lock. Syntax errors do not touch the map.
        let command = match protocol::parse(line) {
            Ok(command) => command,
            Err(error) => return Ok(Reply::Error(error.to_string())),
        };
        let reply = {
            let mut database = self.inner.lock().map_err(|_| StoreError::Poisoned)?;
            // Sample time AFTER acquiring the lock, not before waiting for it.
            database.execute(command, Instant::now())
        }; // The guard is dropped here. The reply owns all of its data.
        Ok(reply)
    }

    pub fn purge_expired(&self) -> Result<usize, StoreError> {
        let mut database = self.inner.lock().map_err(|_| StoreError::Poisoned)?;
        Ok(database.purge_expired(Instant::now()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread};

    #[test]
    fn threads_share_one_store() {
        let store = Arc::new(Store::new(StoreLimits::default()).unwrap());
        let workers: Vec<_> = (0..8)
            .map(|id| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    store
                        .execute_line(&format!("SET key:{id} value:{id}"))
                        .unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        for id in 0..8 {
            assert_eq!(
                store.execute_line(&format!("GET key:{id}")).unwrap(),
                Reply::Value(format!("value:{id}"))
            );
        }
    }

    #[test]
    fn poison_is_reported_not_silently_recovered() {
        let store = Arc::new(Store::new(StoreLimits::default()).unwrap());
        let other = Arc::clone(&store);
        let failed = thread::spawn(move || {
            let _guard = other.inner.lock().unwrap();
            panic!("intentional test panic while holding the store lock");
        });
        assert!(failed.join().is_err());
        assert_eq!(store.execute_line("GET a"), Err(StoreError::Poisoned));
    }
}
