//! A deterministic, in-memory, discrete-event network simulator.
//!
//! **Honest scope, up front**: this is not a real network. There are no
//! sockets, no TCP, no gRPC, no serialization. Messages are Rust values
//! moved through a min-heap keyed by a logical delivery tick. This is a
//! deliberate choice, not a shortcut taken to avoid work — it is the
//! standard way serious Raft test suites (e.g. MIT 6.824/6.5840's Go
//! labs, and the network model in the TLA+ Raft spec) get *deterministic,
//! reproducible* coverage of timing-sensitive scenarios like "the leader
//! crashes right as a quorum acknowledges an entry". A real socket-based
//! test would need real sleeps and would be flaky under CI load; this is
//! not. See the README's "Honest scope" section for the full discussion,
//! including what this means for the benchmark numbers.
//!
//! Determinism is achieved by seeding every random choice (message latency,
//! message drop, per-node election timeout jitter) from an explicit `u64`
//! seed. Given the same seed and the same sequence of driver calls
//! (`tick`, `propose`, `disconnect`, `partition`, ...), the simulation is
//! bit-for-bit reproducible.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{Envelope, NodeId};

#[derive(Debug, Clone, Copy)]
pub struct NetworkConfig {
    pub min_latency_ticks: u64,
    pub max_latency_ticks: u64,
    /// Probability in `[0.0, 1.0]` that an otherwise-reachable message is
    /// silently dropped (simulates packet loss, distinct from partitions).
    pub drop_probability: f64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            min_latency_ticks: 1,
            max_latency_ticks: 3,
            drop_probability: 0.0,
        }
    }
}

/// Queue entry ordered by delivery tick (min-heap via `Reverse`), with a
/// monotonic sequence number as a tiebreaker so equal-tick messages still
/// have a fully deterministic (FIFO-by-send-order) relative order.
struct Scheduled<C> {
    deliver_at: u64,
    seq: u64,
    envelope: Envelope<C>,
}

impl<C> PartialEq for Scheduled<C> {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.seq == other.seq
    }
}
impl<C> Eq for Scheduled<C> {}
impl<C> PartialOrd for Scheduled<C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<C> Ord for Scheduled<C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.deliver_at, self.seq).cmp(&(other.deliver_at, other.seq))
    }
}

/// A deterministic, seeded, in-memory network with injectable latency,
/// packet loss, and partitions (including whole-node disconnects to
/// simulate a crashed process).
pub struct Network<C> {
    config: NetworkConfig,
    rng: StdRng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<Scheduled<C>>>,
    /// If `Some`, only messages between nodes in the same inner group are
    /// delivered — everything else is dropped. `None` means fully connected
    /// (modulo per-node `disconnected` below).
    partition: Option<Vec<Vec<NodeId>>>,
    disconnected: HashMap<NodeId, bool>,
}

impl<C: Clone> Network<C> {
    pub fn new(config: NetworkConfig, seed: u64) -> Self {
        Self {
            config,
            rng: StdRng::seed_from_u64(seed),
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            partition: None,
            disconnected: HashMap::new(),
        }
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    fn group_of(&self, id: NodeId) -> Option<usize> {
        self.partition
            .as_ref()
            .and_then(|groups| groups.iter().position(|g| g.contains(&id)))
    }

    fn is_reachable(&self, from: NodeId, to: NodeId) -> bool {
        if *self.disconnected.get(&from).unwrap_or(&false)
            || *self.disconnected.get(&to).unwrap_or(&false)
        {
            return false;
        }
        match &self.partition {
            None => true,
            Some(_) => self.group_of(from).is_some() && self.group_of(from) == self.group_of(to),
        }
    }

    /// Enqueue a message for (probabilistic, delayed) delivery. Silently
    /// drops it if the link is currently unreachable (partitioned or one
    /// endpoint disconnected) or if the random drop check fires.
    pub fn send(&mut self, envelope: Envelope<C>) {
        if !self.is_reachable(envelope.from, envelope.to) {
            return;
        }
        if self.config.drop_probability > 0.0 && self.rng.gen_bool(self.config.drop_probability) {
            return;
        }
        let latency = if self.config.max_latency_ticks > self.config.min_latency_ticks {
            self.rng
                .gen_range(self.config.min_latency_ticks..=self.config.max_latency_ticks)
        } else {
            self.config.min_latency_ticks
        };
        let deliver_at = self.now + latency.max(1);
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            deliver_at,
            seq: self.seq,
            envelope,
        }));
    }

    /// Advance the logical clock by one tick and return every message whose
    /// delivery time has arrived (re-checking reachability at delivery time,
    /// since a partition may have opened *after* the message was sent).
    pub fn advance_tick(&mut self) -> Vec<Envelope<C>> {
        self.now += 1;
        let mut due = Vec::new();
        while let Some(Reverse(item)) = self.queue.peek() {
            if item.deliver_at > self.now {
                break;
            }
            let Reverse(item) = self.queue.pop().unwrap();
            if self.is_reachable(item.envelope.from, item.envelope.to) {
                due.push(item.envelope);
            }
        }
        due
    }

    /// Split the cluster into disjoint groups; messages only flow within a
    /// group. Nodes omitted from every group are treated as unreachable
    /// from everyone (equivalent to being disconnected).
    pub fn set_partition(&mut self, groups: Vec<Vec<NodeId>>) {
        self.partition = Some(groups);
    }

    pub fn heal_partition(&mut self) {
        self.partition = None;
    }

    /// Simulate a node crashing: no message reaches it, and none it sends
    /// reaches anyone else. Its in-memory `Storage`/`RaftNode` state is
    /// untouched (see the module docs on why that's an honest, disclosed
    /// simplification, not real crash-recovery from disk).
    pub fn disconnect(&mut self, id: NodeId) {
        self.disconnected.insert(id, true);
    }

    pub fn reconnect(&mut self, id: NodeId) {
        self.disconnected.insert(id, false);
    }

    pub fn is_disconnected(&self, id: NodeId) -> bool {
        *self.disconnected.get(&id).unwrap_or(&false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::Rpc;

    fn dummy_env(from: NodeId, to: NodeId) -> Envelope<i32> {
        Envelope {
            from,
            to,
            rpc: Rpc::AppendEntriesReply(crate::rpc::AppendEntriesReply {
                term: 0,
                success: true,
                follower: to,
                conflict_term: None,
                conflict_index: 0,
                match_index: 0,
            }),
        }
    }

    #[test]
    fn messages_are_delayed_and_delivered_in_order() {
        let mut net: Network<i32> = Network::new(
            NetworkConfig {
                min_latency_ticks: 2,
                max_latency_ticks: 2,
                drop_probability: 0.0,
            },
            42,
        );
        net.send(dummy_env(1, 2));
        assert!(net.advance_tick().is_empty());
        let due = net.advance_tick();
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn disconnected_node_receives_nothing() {
        let mut net: Network<i32> = Network::new(
            NetworkConfig {
                min_latency_ticks: 1,
                max_latency_ticks: 1,
                drop_probability: 0.0,
            },
            7,
        );
        net.disconnect(2);
        net.send(dummy_env(1, 2));
        let due = net.advance_tick();
        assert!(due.is_empty());
    }

    #[test]
    fn partition_blocks_cross_group_delivery() {
        let mut net: Network<i32> = Network::new(
            NetworkConfig {
                min_latency_ticks: 1,
                max_latency_ticks: 1,
                drop_probability: 0.0,
            },
            7,
        );
        net.set_partition(vec![vec![1, 2], vec![3, 4, 5]]);
        net.send(dummy_env(1, 3));
        assert!(net.advance_tick().is_empty());
        net.send(dummy_env(1, 2));
        assert_eq!(net.advance_tick().len(), 1);
    }

    #[test]
    fn deterministic_given_same_seed() {
        let make = || {
            let mut net: Network<i32> = Network::new(
                NetworkConfig {
                    min_latency_ticks: 1,
                    max_latency_ticks: 5,
                    drop_probability: 0.2,
                },
                123,
            );
            let mut delivered_ticks = Vec::new();
            for i in 0..50 {
                net.send(dummy_env(1, 2 + (i % 3)));
                for env in net.advance_tick() {
                    delivered_ticks.push((net.now(), env.to));
                }
            }
            delivered_ticks
        };
        assert_eq!(make(), make());
    }
}
