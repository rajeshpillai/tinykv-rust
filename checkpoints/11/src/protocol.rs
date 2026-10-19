//! Parse complete UTF-8 lines. Network framing is a separate operation.
use std::{error::Error, fmt, time::Duration};

pub const MAX_KEY_BYTES: usize = 128;
pub const MAX_VALUE_BYTES: usize = 4096;
pub const MAX_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Ping,
    Set {
        key: &'a str,
        value: &'a str,
        ttl: Option<Duration>,
    },
    Get(&'a str),
    Delete(&'a str),
    Exists(&'a str),
    Expire {
        key: &'a str,
        ttl: Duration,
    },
    Ttl {
        key: &'a str,
        milliseconds: bool,
    },
    Persist(&'a str),
    Stats,
    Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    Syntax,
    UnknownCommand,
    InvalidKey,
    InvalidValue,
    InvalidTtl,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Syntax => "SYNTAX",
            Self::UnknownCommand => "UNKNOWN_COMMAND",
            Self::InvalidKey => "INVALID_KEY",
            Self::InvalidValue => "INVALID_VALUE",
            Self::InvalidTtl => "INVALID_TTL",
        })
    }
}
impl Error for ParseError {}

// A token borrows the original line. It does not allocate a new String.
fn token<'a>(rest: &mut &'a str) -> Option<&'a str> {
    let text = (*rest).trim_start_matches([' ', '\t']);
    if text.is_empty() {
        return None;
    }
    let end = text.find([' ', '\t']).unwrap_or(text.len());
    let (word, tail) = text.split_at(end);
    *rest = tail;
    Some(word)
}

fn key<'a>(rest: &mut &'a str) -> Result<&'a str, ParseError> {
    let key = token(rest).ok_or(ParseError::Syntax)?;
    if key.len() > MAX_KEY_BYTES
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
    {
        return Err(ParseError::InvalidKey);
    }
    Ok(key)
}

fn end(rest: &str) -> Result<(), ParseError> {
    if rest.trim_matches([' ', '\t']).is_empty() {
        Ok(())
    } else {
        Err(ParseError::Syntax)
    }
}

fn value(rest: &str) -> Result<&str, ParseError> {
    // Leading separators are syntax. Interior and trailing spaces are data.
    let value = rest.trim_start_matches([' ', '\t']);
    if value.is_empty() || value.len() > MAX_VALUE_BYTES || value.chars().any(char::is_control) {
        return Err(ParseError::InvalidValue);
    }
    Ok(value)
}

fn ttl(rest: &mut &str, milliseconds: bool, allow_zero: bool) -> Result<Duration, ParseError> {
    let text = token(rest).ok_or(ParseError::Syntax)?;
    // Reject signs, fractions, and overflow. Only decimal digits are valid.
    if !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ParseError::InvalidTtl);
    }
    let amount: u64 = text.parse().map_err(|_| ParseError::InvalidTtl)?;
    let ms = if milliseconds {
        amount
    } else {
        amount.checked_mul(1000).ok_or(ParseError::InvalidTtl)?
    };
    if (!allow_zero && ms == 0) || ms > MAX_TTL_MS {
        return Err(ParseError::InvalidTtl);
    }
    Ok(Duration::from_millis(ms))
}

pub fn parse(line: &str) -> Result<Command<'_>, ParseError> {
    // The transport removes LF and optional CR. Reject embedded controls.
    if line.chars().any(|c| c.is_control() && c != '\t') {
        return Err(ParseError::Syntax);
    }
    let mut rest = line;
    let verb = token(&mut rest).ok_or(ParseError::Syntax)?;
    let command = match verb {
        "PING" => Command::Ping,
        "STATS" => Command::Stats,
        "QUIT" => Command::Quit,
        "SET" => {
            let key = key(&mut rest)?;
            return Ok(Command::Set {
                key,
                value: value(rest)?,
                ttl: None,
            });
        }
        "SETEX" | "PSETEX" => {
            let key = key(&mut rest)?;
            let ttl = ttl(&mut rest, verb == "PSETEX", false)?;
            return Ok(Command::Set {
                key,
                value: value(rest)?,
                ttl: Some(ttl),
            });
        }
        "GET" => Command::Get(key(&mut rest)?),
        "DELETE" | "DEL" => Command::Delete(key(&mut rest)?),
        "EXISTS" => Command::Exists(key(&mut rest)?),
        "PERSIST" => Command::Persist(key(&mut rest)?),
        "TTL" | "PTTL" => Command::Ttl {
            key: key(&mut rest)?,
            milliseconds: verb == "PTTL",
        },
        "EXPIRE" | "PEXPIRE" => Command::Expire {
            key: key(&mut rest)?,
            ttl: ttl(&mut rest, verb == "PEXPIRE", true)?,
        },
        _ => return Err(ParseError::UnknownCommand),
    };
    end(rest)?;
    Ok(command)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Pong,
    Value(String),
    Nil,
    Integer(i64),
    Stats { keys: usize, payload_bytes: usize },
    Bye,
    Error(String),
}

impl fmt::Display for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => writeln!(f, "OK"),
            Self::Pong => writeln!(f, "PONG"),
            Self::Value(value) => writeln!(f, "VALUE {value}"),
            Self::Nil => writeln!(f, "NIL"),
            Self::Integer(n) => writeln!(f, "INTEGER {n}"),
            Self::Stats {
                keys,
                payload_bytes,
            } => {
                writeln!(f, "STATS keys={keys} payload_bytes={payload_bytes}")
            }
            Self::Bye => writeln!(f, "BYE"),
            Self::Error(code) => writeln!(f, "ERR {code}"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn set_borrows_and_preserves_value_spaces() {
        assert_eq!(
            parse("SET greeting hello  Rust  ").unwrap(),
            Command::Set {
                key: "greeting",
                value: "hello  Rust  ",
                ttl: None,
            }
        );
    }

    #[test]
    fn headers_allow_spaces_and_tabs() {
        assert_eq!(parse("  GET\tname  ").unwrap(), Command::Get("name"));
    }

    #[test]
    fn malformed_commands_are_rejected() {
        for line in [
            "",
            "SET",
            "GET",
            "GET a b",
            "PING extra",
            "EXPIRE a 1 extra",
        ] {
            assert!(parse(line).is_err(), "accepted {line:?}");
        }
        assert_eq!(parse("get a"), Err(ParseError::UnknownCommand));
    }

    #[test]
    fn ttl_validation_is_strict() {
        for line in [
            "SETEX a 0 v",
            "SETEX a -1 v",
            "SETEX a +1 v",
            "SETEX a 1.5 v",
            "SETEX a 999999999999999999999999 v",
            "PSETEX a 2592000001 v",
            "PEXPIRE a -1",
        ] {
            assert_eq!(parse(line), Err(ParseError::InvalidTtl), "{line}");
        }
        assert!(parse("PEXPIRE a 0").is_ok());
        assert!(parse("PSETEX a 2592000000 v").is_ok());
    }

    #[test]
    fn key_and_value_limits_count_bytes() {
        assert_eq!(
            parse(&format!("GET {}", "a".repeat(129))),
            Err(ParseError::InvalidKey)
        );
        assert_eq!(parse("GET café"), Err(ParseError::InvalidKey));
        assert_eq!(parse("SET a "), Err(ParseError::InvalidValue));
        assert_eq!(parse("SET a x\ty"), Err(ParseError::InvalidValue));
        assert!(parse("SET a नमस्ते").is_ok());
        assert!(parse(&format!("SET a {}", "x".repeat(4096))).is_ok());
        assert_eq!(
            parse(&format!("SET a {}", "x".repeat(4097))),
            Err(ParseError::InvalidValue)
        );
    }

    #[test]
    fn replies_do_not_confuse_data_with_missing_keys() {
        assert_eq!(Reply::Value("NIL".into()).to_string(), "VALUE NIL\n");
        assert_eq!(Reply::Nil.to_string(), "NIL\n");
    }
}
