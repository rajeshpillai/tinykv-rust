//! Storage rules without sockets, threads, or a real-time clock dependency.
use crate::protocol::{self, Command, Reply};
use std::{collections::HashMap, error::Error, fmt, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct StoreLimits {
    pub max_keys: usize,
    /// Sum of key and value lengths. This is NOT the process memory size.
    pub max_payload_bytes: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_keys: 10_000,
            max_payload_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    InvalidLimits,
    Poisoned,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "store limits must be positive",
            Self::Poisoned => "store lock is poisoned; stop and inspect the server",
        })
    }
}
impl Error for StoreError {}

#[derive(Debug)]
struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|deadline| now >= deadline)
    }
}

#[derive(Debug)]
pub struct Database {
    entries: HashMap<String, Entry>,
    payload_bytes: usize,
    limits: StoreLimits,
}

impl Database {
    pub fn new(limits: StoreLimits) -> Result<Self, StoreError> {
        if limits.max_keys == 0 || limits.max_payload_bytes == 0 {
            return Err(StoreError::InvalidLimits);
        }
        Ok(Self {
            entries: HashMap::new(),
            payload_bytes: 0,
            limits,
        })
    }

    /// A clock value is an input. Tests can use exact times without sleep.
    /// A caller must supply nondecreasing `now` values for this database.
    pub fn apply(&mut self, line: &str, now: Instant) -> Reply {
        match protocol::parse(line) {
            Ok(command) => self.execute(command, now),
            Err(error) => Reply::Error(error.to_string()),
        }
    }

    // Only parsed commands enter here. Do not expose an unchecked write API.
    pub(crate) fn execute(&mut self, command: Command<'_>, now: Instant) -> Reply {
        match command {
            Command::Ping => Reply::Pong,
            Command::Quit => Reply::Bye,
            Command::Set { key, value, ttl } => {
                let expires_at = match ttl {
                    Some(ttl) => match now.checked_add(ttl) {
                        Some(deadline) => Some(deadline),
                        None => return Reply::Error("INVALID_TTL".into()),
                    },
                    None => None,
                };
                self.remove_if_expired(key, now);
                // Reclaim expired entries only on pressure or periodic cleanup.
                if self.proposed_size(key, value).is_none() {
                    self.purge_expired(now);
                }
                let Some(new_size) = self.proposed_size(key, value) else {
                    return Reply::Error("CAPACITY".into());
                };
                // Replacing the whole Entry also replaces or clears its TTL.
                self.entries.insert(
                    key.to_owned(),
                    Entry {
                        value: value.to_owned(),
                        expires_at,
                    },
                );
                self.payload_bytes = new_size;
                Reply::Ok
            }
            Command::Get(key) => {
                self.remove_if_expired(key, now);
                self.entries
                    .get(key)
                    .map_or(Reply::Nil, |entry| Reply::Value(entry.value.clone()))
            }
            Command::Delete(key) => {
                self.remove_if_expired(key, now);
                Reply::Integer(i64::from(self.remove(key)))
            }
            Command::Exists(key) => {
                self.remove_if_expired(key, now);
                Reply::Integer(i64::from(self.entries.contains_key(key)))
            }
            Command::Expire { key, ttl } => {
                self.remove_if_expired(key, now);
                if ttl.is_zero() {
                    return Reply::Integer(i64::from(self.remove(key)));
                }
                let Some(deadline) = now.checked_add(ttl) else {
                    return Reply::Error("INVALID_TTL".into());
                };
                match self.entries.get_mut(key) {
                    Some(entry) => {
                        entry.expires_at = Some(deadline);
                        Reply::Integer(1)
                    }
                    None => Reply::Integer(0),
                }
            }
            Command::Ttl { key, milliseconds } => {
                self.remove_if_expired(key, now);
                let Some(entry) = self.entries.get(key) else {
                    return Reply::Integer(-2);
                };
                let Some(deadline) = entry.expires_at else {
                    return Reply::Integer(-1);
                };
                let remaining = deadline.saturating_duration_since(now);
                let amount = if milliseconds {
                    remaining.as_millis()
                } else {
                    u128::from(remaining.as_secs())
                };
                // Parser limits make overflow impossible; the conversion remains checked.
                match i64::try_from(amount) {
                    Ok(amount) => Reply::Integer(amount),
                    Err(_) => Reply::Error("INVALID_TTL".into()),
                }
            }
            Command::Persist(key) => {
                self.remove_if_expired(key, now);
                let changed = self
                    .entries
                    .get_mut(key)
                    .is_some_and(|entry| entry.expires_at.take().is_some());
                Reply::Integer(i64::from(changed))
            }
            Command::Stats => {
                self.purge_expired(now);
                Reply::Stats {
                    keys: self.entries.len(),
                    payload_bytes: self.payload_bytes,
                }
            }
        }
    }

    fn proposed_size(&self, key: &str, value: &str) -> Option<usize> {
        if !self.entries.contains_key(key) && self.entries.len() >= self.limits.max_keys {
            return None;
        }
        let previous = self
            .entries
            .get(key)
            .map_or(0, |entry| key.len() + entry.value.len());
        let next = self
            .payload_bytes
            .checked_sub(previous)?
            .checked_add(key.len())?
            .checked_add(value.len())?;
        (next <= self.limits.max_payload_bytes).then_some(next)
    }

    fn remove(&mut self, key: &str) -> bool {
        if let Some((stored_key, entry)) = self.entries.remove_entry(key) {
            // Invariant: the counter is the sum of all stored key/value lengths.
            self.payload_bytes -= stored_key.len() + entry.value.len();
            true
        } else {
            false
        }
    }

    fn remove_if_expired(&mut self, key: &str, now: Instant) {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired(now))
        {
            self.remove(key);
        }
    }

    /// Full-table sweep. Cost follows HashMap capacity, not just live entries.
    pub fn purge_expired(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        let payload_bytes = &mut self.payload_bytes;
        self.entries.retain(|key, entry| {
            if entry.is_expired(now) {
                *payload_bytes -= key.len() + entry.value.len();
                false
            } else {
                true
            }
        });
        before - self.entries.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn db() -> Database {
        Database::new(StoreLimits::default()).unwrap()
    }
    fn at(t: Instant, ms: u64) -> Instant {
        t + Duration::from_millis(ms)
    }

    #[test]
    fn basic_commands_and_missing_keys() {
        let (mut db, t) = (db(), Instant::now());
        assert_eq!(db.apply("GET name", t), Reply::Nil);
        assert_eq!(db.apply("SET name Rajesh", t), Reply::Ok);
        assert_eq!(db.apply("GET name", t), Reply::Value("Rajesh".into()));
        assert_eq!(db.apply("EXISTS name", t), Reply::Integer(1));
        assert_eq!(db.apply("DELETE name", t), Reply::Integer(1));
        assert_eq!(db.apply("DELETE name", t), Reply::Integer(0));
    }

    #[test]
    fn expiry_is_exclusive_at_the_deadline() {
        let (mut db, t) = (db(), Instant::now());
        db.apply("PSETEX a 1000 value", t);
        assert_eq!(db.apply("GET a", at(t, 999)), Reply::Value("value".into()));
        assert_eq!(db.apply("GET a", at(t, 1000)), Reply::Nil);
        assert_eq!(db.apply("GET a", at(t, 1001)), Reply::Nil);
    }

    #[test]
    fn ttl_has_three_states_and_rounds_down() {
        let (mut db, t) = (db(), Instant::now());
        assert_eq!(db.apply("TTL a", t), Reply::Integer(-2));
        db.apply("SET a v", t);
        assert_eq!(db.apply("TTL a", t), Reply::Integer(-1));
        db.apply("PEXPIRE a 1500", t);
        assert_eq!(db.apply("TTL a", t), Reply::Integer(1));
        assert_eq!(db.apply("TTL a", at(t, 501)), Reply::Integer(0));
        assert_eq!(db.apply("PTTL a", at(t, 501)), Reply::Integer(999));
        assert_eq!(db.apply("TTL a", at(t, 1500)), Reply::Integer(-2));
    }

    #[test]
    fn ordinary_set_clears_the_old_expiry() {
        let (mut db, t) = (db(), Instant::now());
        db.apply("PSETEX a 100 old", t);
        db.apply("SET a new", at(t, 50));
        assert_eq!(db.apply("GET a", at(t, 500)), Reply::Value("new".into()));
        assert_eq!(db.apply("TTL a", at(t, 500)), Reply::Integer(-1));
    }

    #[test]
    fn replacement_gets_a_new_deadline() {
        let (mut db, t) = (db(), Instant::now());
        db.apply("PSETEX a 100 old", t);
        db.apply("PSETEX a 1000 new", at(t, 50));
        assert_eq!(db.purge_expired(at(t, 100)), 0);
        assert_eq!(db.apply("GET a", at(t, 100)), Reply::Value("new".into()));
        assert_eq!(db.apply("GET a", at(t, 1050)), Reply::Nil);
    }

    #[test]
    fn zero_expire_deletes_but_zero_setex_is_invalid() {
        let (mut db, t) = (db(), Instant::now());
        db.apply("SET a v", t);
        assert_eq!(
            db.apply("SETEX a 0 changed", t),
            Reply::Error("INVALID_TTL".into())
        );
        assert_eq!(db.apply("GET a", t), Reply::Value("v".into()));
        assert_eq!(db.apply("EXPIRE a 0", t), Reply::Integer(1));
        assert_eq!(db.apply("EXPIRE a 0", t), Reply::Integer(0));
    }

    #[test]
    fn persist_removes_only_an_existing_expiry() {
        let (mut db, t) = (db(), Instant::now());
        assert_eq!(db.apply("PERSIST a", t), Reply::Integer(0));
        db.apply("PSETEX a 100 v", t);
        assert_eq!(db.apply("PERSIST a", at(t, 50)), Reply::Integer(1));
        assert_eq!(db.apply("PERSIST a", at(t, 50)), Reply::Integer(0));
        assert_eq!(db.apply("GET a", at(t, 500)), Reply::Value("v".into()));
    }

    #[test]
    fn expired_keys_are_missing_for_every_key_command() {
        for command in ["EXISTS a", "DELETE a", "EXPIRE a 10", "PERSIST a"] {
            let (mut db, t) = (db(), Instant::now());
            db.apply("PSETEX a 1 v", t);
            assert_eq!(db.apply(command, at(t, 1)), Reply::Integer(0), "{command}");
        }
    }

    #[test]
    fn sweep_reclaims_expired_payload_without_get() {
        let (mut db, t) = (db(), Instant::now());
        db.apply("PSETEX a 100 v", t);
        assert_eq!(db.entries.len(), 1);
        assert_eq!(db.purge_expired(at(t, 100)), 1);
        assert_eq!(db.payload_bytes, 0);
    }

    #[test]
    fn capacity_rejection_preserves_the_live_old_value() {
        let mut db = Database::new(StoreLimits {
            max_keys: 1,
            max_payload_bytes: 5,
        })
        .unwrap();
        let t = Instant::now();
        assert_eq!(db.apply("SET a 1234", t), Reply::Ok);
        assert_eq!(db.apply("SET a 12345", t), Reply::Error("CAPACITY".into()));
        assert_eq!(db.apply("SET b v", t), Reply::Error("CAPACITY".into()));
        assert_eq!(db.apply("GET a", t), Reply::Value("1234".into()));
        assert_eq!(db.apply("SET a x", t), Reply::Ok);
        assert_eq!(db.payload_bytes, 2);
    }

    #[test]
    fn pressure_reclaims_expired_keys_before_rejecting() {
        let mut db = Database::new(StoreLimits {
            max_keys: 1,
            max_payload_bytes: 10,
        })
        .unwrap();
        let t = Instant::now();
        db.apply("PSETEX a 1 v", t);
        assert_eq!(db.apply("SET b v", at(t, 1)), Reply::Ok);
        assert_eq!(
            db.apply("STATS", at(t, 1)),
            Reply::Stats {
                keys: 1,
                payload_bytes: 2
            }
        );
    }

    #[test]
    fn byte_accounting_matches_the_map_after_changes() {
        let (mut db, t) = (db(), Instant::now());
        for command in [
            "SET a नमस्ते",
            "SET b xyz",
            "SET a z",
            "DELETE b",
            "PSETEX c 1 v",
        ] {
            db.apply(command, t);
            let sum: usize = db
                .entries
                .iter()
                .map(|(k, e)| k.len() + e.value.len())
                .sum();
            assert_eq!(db.payload_bytes, sum);
        }
        db.purge_expired(at(t, 1));
        assert_eq!(db.payload_bytes, 2);
    }

    #[test]
    fn invalid_limits_fail_before_startup() {
        assert!(
            Database::new(StoreLimits {
                max_keys: 0,
                max_payload_bytes: 10
            })
            .is_err()
        );
    }
}
