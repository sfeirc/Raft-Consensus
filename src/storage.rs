//! Persistent Raft state and the storage abstraction.
//!
//! The Raft paper (Ongaro & Ousterhout, §5.1) requires three pieces of state
//! to be durable across crashes and restarts: `currentTerm`, `votedFor`, and
//! the `log[]`. This module defines a [`Storage`] trait that captures exactly
//! that contract, independent of *how* it is persisted, plus [`MemStorage`],
//! the only implementation shipped in this crate.
//!
//! **Honest scope**: [`MemStorage`] does **not** write to disk. Nothing here
//! is `fsync`'d, and nothing survives a real process restart — "crashing" a
//! node in the test suite means disconnecting it from the simulated network
//! while its process (and this in-memory struct) keeps running, which is a
//! deliberate simplification, not persistence. The trait boundary is shaped
//! so a disk/`sled`/`rocksdb`-backed implementation could be substituted
//! without touching [`crate::node::RaftNode`], but no such implementation
//! exists here. See the README's "Honest scope" section for the full list of
//! what is and is not implemented.

use crate::rpc::{LogIndex, NodeId, Term};

/// A command wrapper stored in the replicated log.
///
/// Every leader appends a [`LogCommand::NoOp`] entry as the very first thing
/// it does upon election (paper §8, "a leader ... commits a blank no-op
/// entry into the log at the start of its term"). This is not decorative: it
/// is what lets a new leader safely commit entries that were replicated
/// under *previous* terms, without which a leader that receives no new
/// client requests could never advance `commit_index` past entries from
/// earlier terms (a liveness gap, not just a style choice).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogCommand<C> {
    NoOp,
    Command(C),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry<C> {
    pub term: Term,
    pub index: LogIndex,
    pub command: LogCommand<C>,
}

/// The durable-state contract every Raft node depends on.
///
/// Indices are 1-based; index `0` is the sentinel "before the log starts"
/// index used for `prev_log_index` on the very first AppendEntries.
pub trait Storage<C: Clone>: std::fmt::Debug {
    fn current_term(&self) -> Term;
    fn voted_for(&self) -> Option<NodeId>;

    /// Persist term + vote together (in a disk-backed implementation this
    /// would be a single fsync'd write — losing atomicity here is exactly
    /// the kind of durability bug that makes Raft implementations unsafe).
    fn set_term_and_vote(&mut self, term: Term, voted_for: Option<NodeId>);

    fn last_index(&self) -> LogIndex;

    fn entry(&self, index: LogIndex) -> Option<&LogEntry<C>>;

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entry(index).map(|e| e.term)
    }

    fn last_term(&self) -> Term {
        let idx = self.last_index();
        if idx == 0 {
            0
        } else {
            self.entry(idx).map(|e| e.term).unwrap_or(0)
        }
    }

    /// Append entries to the end of the log. Callers are responsible for
    /// having already resolved any conflicts (see [`Storage::truncate_from`]).
    fn append(&mut self, entries: Vec<LogEntry<C>>);

    /// Remove every entry with `index >= from`. Used when a follower
    /// discovers a conflicting entry while applying AppendEntries (paper
    /// §5.3: "If an existing entry conflicts with a new one ... delete the
    /// existing entry and all that follow it").
    fn truncate_from(&mut self, from: LogIndex);

    /// Entries from `from` (inclusive) to the end of the log.
    fn entries_from(&self, from: LogIndex) -> Vec<LogEntry<C>>;
}

/// In-memory [`Storage`] implementation. See module docs for what this does
/// *not* provide (durability across a real restart).
#[derive(Debug)]
pub struct MemStorage<C> {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry<C>>, // log[i] is the entry at index i+1
}

impl<C> Default for MemStorage<C> {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl<C: Clone + std::fmt::Debug> Storage<C> for MemStorage<C> {
    fn current_term(&self) -> Term {
        self.current_term
    }

    fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    fn set_term_and_vote(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = voted_for;
    }

    fn last_index(&self) -> LogIndex {
        self.log.len() as LogIndex
    }

    fn entry(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        if index == 0 {
            return None;
        }
        self.log.get((index - 1) as usize)
    }

    fn append(&mut self, mut entries: Vec<LogEntry<C>>) {
        self.log.append(&mut entries);
    }

    fn truncate_from(&mut self, from: LogIndex) {
        if from == 0 {
            self.log.clear();
            return;
        }
        self.log.truncate((from - 1) as usize);
    }

    fn entries_from(&self, from: LogIndex) -> Vec<LogEntry<C>> {
        if from == 0 || from > self.last_index() {
            return Vec::new();
        }
        self.log[(from - 1) as usize..].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_back() {
        let mut s: MemStorage<i32> = MemStorage::default();
        assert_eq!(s.last_index(), 0);
        s.append(vec![
            LogEntry {
                term: 1,
                index: 1,
                command: LogCommand::Command(10),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: LogCommand::Command(20),
            },
        ]);
        assert_eq!(s.last_index(), 2);
        assert_eq!(s.term_at(1), Some(1));
        assert_eq!(s.term_at(0), Some(0));
        assert_eq!(s.entry(2).unwrap().command, LogCommand::Command(20));
    }

    #[test]
    fn truncate_from_removes_suffix() {
        let mut s: MemStorage<i32> = MemStorage::default();
        s.append(vec![
            LogEntry {
                term: 1,
                index: 1,
                command: LogCommand::Command(1),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: LogCommand::Command(2),
            },
            LogEntry {
                term: 2,
                index: 3,
                command: LogCommand::Command(3),
            },
        ]);
        s.truncate_from(2);
        assert_eq!(s.last_index(), 1);
        assert_eq!(s.entry(1).unwrap().command, LogCommand::Command(1));
    }

    #[test]
    fn entries_from_is_a_suffix() {
        let mut s: MemStorage<i32> = MemStorage::default();
        s.append(vec![
            LogEntry {
                term: 1,
                index: 1,
                command: LogCommand::Command(1),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: LogCommand::Command(2),
            },
        ]);
        assert_eq!(s.entries_from(2).len(), 1);
        assert_eq!(s.entries_from(3).len(), 0);
        assert_eq!(s.entries_from(1).len(), 2);
    }
}
