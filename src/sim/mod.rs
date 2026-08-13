//! Deterministic in-memory network simulation used to drive and test
//! [`crate::node::RaftNode`] instances without a real network. See
//! [`network`] for the honest-scope discussion of what this does and does
//! not model.

pub mod network;

pub use network::{Network, NetworkConfig};
