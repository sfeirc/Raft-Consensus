//! Test/demo harness: wires N [`RaftNode`]s to a simulated [`Network`] and a
//! per-node copy of a [`StateMachine`], and drives everything tick-by-tick.
//!
//! This is the piece both `tests/correctness.rs` and `src/bin/demo.rs`
//! build on: it is what lets a test say "propose this command, disconnect
//! this node, run for a while, assert every reachable node's state machine
//! matches" in a few lines, entirely deterministically.

use std::collections::HashMap;

use crate::node::{Committed, NotLeaderError, RaftConfig, RaftNode, RoleKind};
use crate::rpc::{LogIndex, NodeId, Term};
use crate::sim::{Network, NetworkConfig};
use crate::state_machine::StateMachine;
use crate::storage::{MemStorage, Storage};

pub struct Cluster<SM: StateMachine> {
    pub nodes: HashMap<NodeId, RaftNode<SM::Command, MemStorage<SM::Command>>>,
    pub machines: HashMap<NodeId, SM>,
    pub network: Network<SM::Command>,
    pub tick_count: u64,
}

impl<SM: StateMachine> Cluster<SM> {
    pub fn new(ids: &[NodeId], config: RaftConfig, net_config: NetworkConfig, seed: u64) -> Self {
        let mut nodes = HashMap::new();
        let mut machines = HashMap::new();
        for &id in ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            // Distinct-but-deterministic per-node seed: mixed with a large
            // odd constant so nearby ids don't produce correlated RNG
            // streams.
            let node_seed = seed.wrapping_add(id.wrapping_mul(0x9E3779B97F4A7C15));
            let storage = MemStorage::default();
            nodes.insert(id, RaftNode::new(id, peers, storage, node_seed, config));
            machines.insert(id, SM::default());
        }
        let network = Network::new(net_config, seed ^ 0xA5A5_A5A5_A5A5_A5A5);
        Self {
            nodes,
            machines,
            network,
            tick_count: 0,
        }
    }

    fn apply_committed(&mut self, node_id: NodeId, committed: Vec<Committed<SM::Command>>) {
        if committed.is_empty() {
            return;
        }
        let machine = self
            .machines
            .get_mut(&node_id)
            .expect("machine exists for every node");
        for c in committed {
            machine.apply(&c.command);
        }
    }

    /// Advance the whole cluster by one logical tick: deliver messages that
    /// are due, then let every node's timers tick, then enqueue whatever new
    /// messages that produced.
    pub fn tick(&mut self) {
        self.tick_count += 1;
        let due = self.network.advance_tick();
        let mut outgoing = Vec::new();

        for envelope in due {
            let to = envelope.to;
            if let Some(node) = self.nodes.get_mut(&to) {
                let result = node.receive(envelope);
                outgoing.extend(result.effects);
                self.apply_committed(to, result.committed);
            }
        }

        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            let node = self.nodes.get_mut(&id).unwrap();
            let result = node.tick();
            outgoing.extend(result.effects);
            self.apply_committed(id, result.committed);
        }

        for envelope in outgoing {
            self.network.send(envelope);
        }
    }

    pub fn run_ticks(&mut self, n: u64) {
        for _ in 0..n {
            self.tick();
        }
    }

    pub fn propose(
        &mut self,
        node_id: NodeId,
        command: SM::Command,
    ) -> Result<LogIndex, NotLeaderError> {
        let node = self.nodes.get_mut(&node_id).expect("unknown node id");
        let (index, result) = node.propose(command)?;
        for envelope in result.effects {
            self.network.send(envelope);
        }
        self.apply_committed(node_id, result.committed);
        Ok(index)
    }

    /// Propose to whichever node currently believes it's the leader. Useful
    /// in tests where "the" leader may have changed.
    pub fn propose_via_current_leader(&mut self, command: SM::Command) -> Option<LogIndex> {
        let leader = self.current_leader()?;
        self.propose(leader, command).ok()
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|n| n.role() == RoleKind::Leader)
            .map(|n| n.id)
    }

    /// Find a leader among a specific subset of nodes (e.g. "the nodes that
    /// are still reachable"). Unlike [`Cluster::current_leader`], this is
    /// safe to use in fault-injection scenarios: a disconnected ("crashed")
    /// node legitimately keeps believing it's still leader in its old term
    /// — that's correct Raft behavior, not a bug — so naively asking "who
    /// is *the* leader" is ambiguous while more than one node self-identifies
    /// as leader in different terms. Restricting the search to a known-alive
    /// subset removes that ambiguity.
    pub fn leader_among(&self, ids: &[NodeId]) -> Option<NodeId> {
        ids.iter()
            .copied()
            .find(|id| self.nodes.get(id).map(|n| n.is_leader()).unwrap_or(false))
    }

    /// All nodes that currently believe themselves to be leader, paired
    /// with their term. Used by [`Cluster::assert_at_most_one_leader_per_term`].
    pub fn leaders_by_term(&self) -> Vec<(Term, NodeId)> {
        self.nodes
            .values()
            .filter(|n| n.role() == RoleKind::Leader)
            .map(|n| (n.current_term(), n.id))
            .collect()
    }

    /// Core election-safety invariant: at most one leader per term, ever.
    /// Panics with a descriptive message if violated (used throughout the
    /// test suite as a continuous, not just end-of-test, check).
    pub fn assert_at_most_one_leader_per_term(&self) {
        let leaders = self.leaders_by_term();
        let mut by_term: HashMap<Term, Vec<NodeId>> = HashMap::new();
        for (term, id) in leaders {
            by_term.entry(term).or_default().push(id);
        }
        for (term, ids) in by_term {
            assert!(
                ids.len() <= 1,
                "election safety violated: {} leaders in term {}: {:?}",
                ids.len(),
                term,
                ids
            );
        }
    }

    /// Log-matching / state-machine-safety invariant: for any index
    /// committed on two or more nodes, the (term, command) at that index
    /// must be identical everywhere it's committed. This is the general
    /// form of "a committed entry can never be lost or overwritten".
    pub fn assert_committed_logs_agree(&self) {
        let mut reference: HashMap<LogIndex, (Term, String)> = HashMap::new();
        for node in self.nodes.values() {
            let ci = node.commit_index();
            for idx in 1..=ci {
                let Some(entry) = node.storage().entry(idx) else {
                    continue;
                };
                let key = format!("{:?}", entry.command);
                match reference.get(&idx) {
                    None => {
                        reference.insert(idx, (entry.term, key));
                    }
                    Some((ref_term, ref_key)) => {
                        assert_eq!(
                            (*ref_term, ref_key.as_str()),
                            (entry.term, key.as_str()),
                            "log safety violated at committed index {idx}: nodes disagree on the committed entry"
                        );
                    }
                }
            }
        }
    }

    pub fn disconnect(&mut self, id: NodeId) {
        self.network.disconnect(id);
    }
    pub fn reconnect(&mut self, id: NodeId) {
        self.network.reconnect(id);
    }
    pub fn partition(&mut self, groups: Vec<Vec<NodeId>>) {
        self.network.set_partition(groups);
    }
    pub fn heal_partition(&mut self) {
        self.network.heal_partition();
    }

    pub fn machine(&self, id: NodeId) -> &SM {
        self.machines.get(&id).expect("unknown node id")
    }

    pub fn commit_index(&self, id: NodeId) -> LogIndex {
        self.nodes.get(&id).expect("unknown node id").commit_index()
    }

    pub fn all_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn reachable_ids(&self) -> Vec<NodeId> {
        self.all_ids()
            .into_iter()
            .filter(|&id| !self.network.is_disconnected(id))
            .collect()
    }

    /// Run ticks until some node becomes leader (or `max_ticks` elapses),
    /// checking the election-safety invariant on every tick along the way.
    pub fn run_until_leader(&mut self, max_ticks: u64) -> Option<NodeId> {
        for _ in 0..max_ticks {
            self.tick();
            self.assert_at_most_one_leader_per_term();
            if let Some(l) = self.current_leader() {
                return Some(l);
            }
        }
        None
    }

    /// Run ticks until every node in `ids` has caught its commit_index up to
    /// at least `target`, or `max_ticks` elapses. Returns whether it
    /// succeeded. Checks both safety invariants on every tick.
    pub fn run_until_committed(
        &mut self,
        ids: &[NodeId],
        target: LogIndex,
        max_ticks: u64,
    ) -> bool {
        for _ in 0..max_ticks {
            self.tick();
            self.assert_at_most_one_leader_per_term();
            self.assert_committed_logs_agree();
            if ids.iter().all(|id| self.commit_index(*id) >= target) {
                return true;
            }
        }
        false
    }
}
