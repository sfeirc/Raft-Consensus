//! A from-scratch implementation of the Raft distributed consensus
//! algorithm (Ongaro & Ousterhout, *In Search of an Understandable
//! Consensus Algorithm*), built for correctness clarity rather than for
//! production deployment. See the repository README for the full
//! explanation, measured benchmarks, and an explicit "Honest scope /
//! limitations" section.
//!
//! # Module map
//!
//! - [`rpc`] — the wire-level message types (RequestVote, AppendEntries).
//! - [`storage`] — the durable-state contract (`current_term`, `voted_for`,
//!   the log) and its in-memory implementation.
//! - [`node`] — [`node::RaftNode`], the pure, I/O-free Raft state machine:
//!   leader election + log replication + the commit-safety rule.
//! - [`state_machine`] — the replicated state machine sitting on top of the
//!   log (a small key-value store, used to prove the replicated log
//!   actually produces identical state everywhere).
//! - [`sim`] — the deterministic in-memory network simulator used to test
//!   multi-node scenarios (elections, crashes, partitions) without flaky
//!   real-time behavior.
//! - [`cluster`] — a harness wiring nodes + network + state machines
//!   together, used by both the test suite and the demo binary.

pub mod cluster;
pub mod node;
pub mod rpc;
pub mod sim;
pub mod state_machine;
pub mod storage;

pub use cluster::Cluster;
pub use node::{RaftConfig, RaftNode, RoleKind};
pub use sim::{Network, NetworkConfig};
pub use state_machine::{KvCommand, KvStore, StateMachine};
pub use storage::MemStorage;
