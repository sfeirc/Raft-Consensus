//! The replicated state machine that sits on top of the Raft log.
//!
//! Raft itself only guarantees that every node applies the *same sequence*
//! of committed commands in the *same order*. What those commands mean is
//! entirely up to the state machine. This module provides a minimal
//! key-value store ([`KvStore`]) as the demonstration state machine used by
//! the demo binary, the correctness tests, and the benchmarks: it exists to
//! give the test suite something concrete to compare across nodes ("does
//! every node end up with byte-identical state?"), not as a product.

use std::collections::BTreeMap;

/// A deterministic state machine driven by committed log entries.
pub trait StateMachine: Default + std::fmt::Debug {
    type Command: Clone + std::fmt::Debug;
    type Output: Clone + std::fmt::Debug + PartialEq;

    fn apply(&mut self, command: &Self::Command) -> Self::Output;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvCommand {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvOutput {
    Set,
    Deleted(Option<String>),
}

/// A tiny replicated key-value store. `BTreeMap` is used (rather than
/// `HashMap`) purely so that `Debug`/`==` comparisons across nodes in tests
/// are stable and easy to read in failure output — iteration order has no
/// bearing on Raft correctness here since we compare full map equality, not
/// order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvStore {
    pub map: BTreeMap<String, String>,
}

impl StateMachine for KvStore {
    type Command = KvCommand;
    type Output = KvOutput;

    fn apply(&mut self, command: &KvCommand) -> KvOutput {
        match command {
            KvCommand::Set { key, value } => {
                self.map.insert(key.clone(), value.clone());
                KvOutput::Set
            }
            KvCommand::Delete { key } => KvOutput::Deleted(self.map.remove(key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_delete() {
        let mut kv = KvStore::default();
        kv.apply(&KvCommand::Set {
            key: "a".into(),
            value: "1".into(),
        });
        assert_eq!(kv.map.get("a"), Some(&"1".to_string()));
        let out = kv.apply(&KvCommand::Delete { key: "a".into() });
        assert_eq!(out, KvOutput::Deleted(Some("1".to_string())));
        assert!(!kv.map.contains_key("a"));
    }
}
