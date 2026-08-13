//! The core Raft state machine: leader election + log replication.
//!
//! `RaftNode` is deliberately I/O-free and time-free: it never sleeps, never
//! touches a clock, never opens a socket. It only reacts to two kinds of
//! input — [`RaftNode::tick`] (one logical unit of time has passed) and
//! [`RaftNode::receive`] (a message arrived) — and produces a
//! [`StepResult`] describing what to send and what has newly been committed.
//! This makes the whole thing trivially deterministic to test: drive it with
//! a seeded RNG and a logical tick counter (see [`crate::sim`] and
//! [`crate::cluster`]) and the exact same sequence of inputs always produces
//! the exact same sequence of outputs, on any machine, forever. That is what
//! keeps the correctness tests (including the fault-injection ones) free of
//! `sleep`-based flakiness.
//!
//! The algorithm follows the Raft paper (Ongaro & Ousterhout, "In Search of
//! an Understandable Consensus Algorithm", Figure 2) directly: RequestVote /
//! AppendEntries handling, the log-matching / conflict-truncation rule
//! (§5.3), the up-to-date-log check that gates vote granting (§5.4.1), and
//! the "commit only via a current-term entry" rule that is the crux of
//! Raft's safety proof (§5.4.2, and the Figure 8 hazard it closes).

use std::collections::{HashMap, HashSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{
    AppendEntriesArgs, AppendEntriesReply, Envelope, LogIndex, NodeId, RequestVoteArgs,
    RequestVoteReply, Rpc, Term,
};
use crate::storage::{LogCommand, LogEntry, Storage};

/// Maximum number of entries batched into a single AppendEntries RPC. Real
/// systems tune this against message-size limits; we just cap it so a
/// far-behind follower doesn't get one enormous message.
const MAX_BATCH: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleKind {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
enum Role {
    Follower,
    Candidate,
    Leader {
        next_index: HashMap<NodeId, LogIndex>,
        match_index: HashMap<NodeId, LogIndex>,
    },
}

impl Role {
    fn kind(&self) -> RoleKind {
        match self {
            Role::Follower => RoleKind::Follower,
            Role::Candidate => RoleKind::Candidate,
            Role::Leader { .. } => RoleKind::Leader,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RaftConfig {
    /// Minimum randomized election timeout, in ticks.
    pub min_election_ticks: u64,
    /// Maximum randomized election timeout, in ticks.
    pub max_election_ticks: u64,
    /// Leader heartbeat / replication period, in ticks. Must be well below
    /// `min_election_ticks` or followers will time out between heartbeats.
    pub heartbeat_ticks: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            min_election_ticks: 10,
            max_election_ticks: 20,
            heartbeat_ticks: 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed<C> {
    pub index: LogIndex,
    pub term: Term,
    pub command: C,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StepResult<C> {
    pub effects: Vec<Envelope<C>>,
    pub committed: Vec<Committed<C>>,
}

impl<C> StepResult<C> {
    fn empty() -> Self {
        Self {
            effects: Vec::new(),
            committed: Vec::new(),
        }
    }
    fn single(env: Envelope<C>) -> Self {
        Self {
            effects: vec![env],
            committed: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeaderError {
    pub leader_hint: Option<NodeId>,
}

pub struct RaftNode<C: Clone + std::fmt::Debug, S: Storage<C>> {
    pub id: NodeId,
    peers: Vec<NodeId>,
    storage: S,
    role: Role,
    commit_index: LogIndex,
    last_applied: LogIndex,
    leader_id: Option<NodeId>,
    votes_received: HashSet<NodeId>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    config: RaftConfig,
    rng: StdRng,
    _marker: std::marker::PhantomData<C>,
}

impl<C: Clone + std::fmt::Debug, S: Storage<C>> std::fmt::Debug for RaftNode<C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftNode")
            .field("id", &self.id)
            .field("role", &self.role.kind())
            .field("term", &self.storage.current_term())
            .field("commit_index", &self.commit_index)
            .field("last_index", &self.storage.last_index())
            .finish()
    }
}

impl<C: Clone + std::fmt::Debug, S: Storage<C>> RaftNode<C, S> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, storage: S, seed: u64, config: RaftConfig) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let election_timeout = rng.gen_range(config.min_election_ticks..=config.max_election_ticks);
        Self {
            id,
            peers,
            storage,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_id: None,
            votes_received: HashSet::new(),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            config,
            rng,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn role(&self) -> RoleKind {
        self.role.kind()
    }
    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }
    pub fn current_term(&self) -> Term {
        self.storage.current_term()
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn log_len(&self) -> LogIndex {
        self.storage.last_index()
    }
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn storage(&self) -> &S {
        &self.storage
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self
            .rng
            .gen_range(self.config.min_election_ticks..=self.config.max_election_ticks);
    }

    fn step_down(&mut self) {
        self.role = Role::Follower;
        self.votes_received.clear();
    }

    /// Move `last_applied` up to `commit_index`, returning the real
    /// (non-`NoOp`) commands that just became committed, in log order.
    fn advance_last_applied_and_collect(&mut self) -> Vec<Committed<C>> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.storage.entry(self.last_applied) {
                if let LogCommand::Command(cmd) = &entry.command {
                    out.push(Committed {
                        index: entry.index,
                        term: entry.term,
                        command: cmd.clone(),
                    });
                }
            }
        }
        out
    }

    // ---- driving inputs -------------------------------------------------

    pub fn tick(&mut self) -> StepResult<C> {
        match &self.role {
            Role::Leader { .. } => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.config.heartbeat_ticks {
                    self.heartbeat_elapsed = 0;
                    let effects = self.replicate_to_all();
                    return StepResult {
                        effects,
                        committed: Vec::new(),
                    };
                }
                StepResult::empty()
            }
            _ => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.election_timeout {
                    self.start_election()
                } else {
                    StepResult::empty()
                }
            }
        }
    }

    pub fn propose(&mut self, command: C) -> Result<(LogIndex, StepResult<C>), NotLeaderError> {
        if !self.is_leader() {
            return Err(NotLeaderError {
                leader_hint: self.leader_id,
            });
        }
        let term = self.storage.current_term();
        let index = self.storage.last_index() + 1;
        self.storage.append(vec![LogEntry {
            term,
            index,
            command: LogCommand::Command(command),
        }]);
        let effects = self.replicate_to_all();
        // A single-node cluster can commit its own proposal immediately.
        let committed = self.try_advance_commit_index();
        Ok((index, StepResult { effects, committed }))
    }

    pub fn receive(&mut self, envelope: Envelope<C>) -> StepResult<C> {
        let from = envelope.from;
        match envelope.rpc {
            Rpc::RequestVote(args) => self.handle_request_vote(from, args),
            Rpc::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Rpc::AppendEntries(args) => self.handle_append_entries(from, args),
            Rpc::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply),
        }
    }

    // ---- election ---------------------------------------------------------

    fn start_election(&mut self) -> StepResult<C> {
        let new_term = self.storage.current_term() + 1;
        self.storage.set_term_and_vote(new_term, Some(self.id));
        self.role = Role::Candidate;
        self.votes_received = HashSet::from([self.id]);
        self.leader_id = None;
        self.reset_election_timer();

        if self.peers.is_empty() {
            return self.become_leader();
        }

        let effects = self
            .peers
            .iter()
            .map(|&p| Envelope {
                from: self.id,
                to: p,
                rpc: Rpc::RequestVote(RequestVoteArgs {
                    term: new_term,
                    candidate_id: self.id,
                    last_log_index: self.storage.last_index(),
                    last_log_term: self.storage.last_term(),
                }),
            })
            .collect();
        StepResult {
            effects,
            committed: Vec::new(),
        }
    }

    fn become_leader(&mut self) -> StepResult<C> {
        let last_idx = self.storage.last_index();
        let next_index = self.peers.iter().map(|&p| (p, last_idx + 1)).collect();
        let match_index = self.peers.iter().map(|&p| (p, 0)).collect();
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;

        // Paper §8: append a no-op so this leader can eventually commit
        // entries from earlier terms (see the doc comment on `LogCommand`).
        let term = self.storage.current_term();
        let idx = self.storage.last_index() + 1;
        self.storage.append(vec![LogEntry {
            term,
            index: idx,
            command: LogCommand::NoOp,
        }]);

        let effects = self.replicate_to_all();
        let committed = self.try_advance_commit_index();
        StepResult { effects, committed }
    }

    fn handle_request_vote(&mut self, from: NodeId, args: RequestVoteArgs) -> StepResult<C> {
        if args.term < self.storage.current_term() {
            return StepResult::single(self.reply_vote(from, false));
        }
        if args.term > self.storage.current_term() {
            self.storage.set_term_and_vote(args.term, None);
            self.step_down();
            self.leader_id = None;
        }

        let voted_for = self.storage.voted_for();
        let can_vote = voted_for.is_none() || voted_for == Some(args.candidate_id);
        let up_to_date = (args.last_log_term, args.last_log_index)
            >= (self.storage.last_term(), self.storage.last_index());

        if can_vote && up_to_date {
            self.storage
                .set_term_and_vote(self.storage.current_term(), Some(args.candidate_id));
            self.reset_election_timer();
            StepResult::single(self.reply_vote(from, true))
        } else {
            StepResult::single(self.reply_vote(from, false))
        }
    }

    fn reply_vote(&self, to: NodeId, granted: bool) -> Envelope<C> {
        Envelope {
            from: self.id,
            to,
            rpc: Rpc::RequestVoteReply(RequestVoteReply {
                term: self.storage.current_term(),
                vote_granted: granted,
                voter: self.id,
            }),
        }
    }

    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) -> StepResult<C> {
        if reply.term > self.storage.current_term() {
            self.storage.set_term_and_vote(reply.term, None);
            self.step_down();
            self.leader_id = None;
            return StepResult::empty();
        }
        if !matches!(self.role, Role::Candidate) || reply.term != self.storage.current_term() {
            return StepResult::empty();
        }
        if reply.vote_granted {
            self.votes_received.insert(reply.voter);
            if self.votes_received.len() >= self.majority() {
                return self.become_leader();
            }
        }
        StepResult::empty()
    }

    // ---- log replication ----------------------------------------------------

    fn replicate_to_all(&mut self) -> Vec<Envelope<C>> {
        self.peers
            .clone()
            .into_iter()
            .filter_map(|p| self.build_append_entries_for(p))
            .collect()
    }

    fn build_append_entries_for(&self, peer: NodeId) -> Option<Envelope<C>> {
        let Role::Leader { next_index, .. } = &self.role else {
            return None;
        };
        let next = *next_index.get(&peer).unwrap_or(&1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.storage.term_at(prev_log_index).unwrap_or(0);
        let mut entries = self.storage.entries_from(next);
        entries.truncate(MAX_BATCH);
        Some(Envelope {
            from: self.id,
            to: peer,
            rpc: Rpc::AppendEntries(AppendEntriesArgs {
                term: self.storage.current_term(),
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            }),
        })
    }

    fn reply_append(
        &self,
        to: NodeId,
        success: bool,
        conflict_term: Option<Term>,
        conflict_index: LogIndex,
        match_index: LogIndex,
    ) -> Envelope<C> {
        Envelope {
            from: self.id,
            to,
            rpc: Rpc::AppendEntriesReply(AppendEntriesReply {
                term: self.storage.current_term(),
                success,
                follower: self.id,
                conflict_term,
                conflict_index,
                match_index,
            }),
        }
    }

    fn handle_append_entries(&mut self, from: NodeId, args: AppendEntriesArgs<C>) -> StepResult<C> {
        if args.term < self.storage.current_term() {
            return StepResult::single(self.reply_append(from, false, None, 0, 0));
        }
        if args.term > self.storage.current_term() {
            self.storage.set_term_and_vote(args.term, None);
        }
        // A valid current-term leader exists: become/stay follower.
        self.step_down();
        self.leader_id = Some(args.leader_id);
        self.reset_election_timer();

        if args.prev_log_index > 0 {
            match self.storage.term_at(args.prev_log_index) {
                None => {
                    let conflict_index = self.storage.last_index() + 1;
                    return StepResult::single(self.reply_append(
                        from,
                        false,
                        None,
                        conflict_index,
                        0,
                    ));
                }
                Some(t) if t != args.prev_log_term => {
                    let mut idx = args.prev_log_index;
                    while idx > 1 && self.storage.term_at(idx - 1) == Some(t) {
                        idx -= 1;
                    }
                    return StepResult::single(self.reply_append(from, false, Some(t), idx, 0));
                }
                _ => {}
            }
        }

        for (offset, entry) in args.entries.iter().enumerate() {
            let idx = args.prev_log_index + 1 + offset as u64;
            match self.storage.term_at(idx) {
                Some(t) if t == entry.term => continue, // already present & matching
                Some(_) => {
                    self.storage.truncate_from(idx);
                    self.storage.append(args.entries[offset..].to_vec());
                    break;
                }
                None => {
                    self.storage.append(args.entries[offset..].to_vec());
                    break;
                }
            }
        }

        let last_new_index = args.prev_log_index + args.entries.len() as u64;
        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(last_new_index);
        }
        let committed = self.advance_last_applied_and_collect();
        let reply = self.reply_append(from, true, None, 0, last_new_index);
        StepResult {
            effects: vec![reply],
            committed,
        }
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) -> StepResult<C> {
        if reply.term > self.storage.current_term() {
            self.storage.set_term_and_vote(reply.term, None);
            self.step_down();
            self.leader_id = None;
            return StepResult::empty();
        }
        if reply.term < self.storage.current_term() {
            return StepResult::empty();
        }
        let from = reply.follower;
        let is_known_peer =
            matches!(&self.role, Role::Leader { next_index, .. } if next_index.contains_key(&from));
        if !is_known_peer {
            return StepResult::empty();
        }

        if reply.success {
            if let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
            {
                let mi = match_index
                    .get(&from)
                    .copied()
                    .unwrap_or(0)
                    .max(reply.match_index);
                match_index.insert(from, mi);
                next_index.insert(from, mi + 1);
            }
        } else {
            let new_next =
                self.compute_backtrack_next_index(reply.conflict_term, reply.conflict_index);
            if let Role::Leader { next_index, .. } = &mut self.role {
                next_index.insert(from, new_next.max(1));
            }
        }

        let mut effects = Vec::new();
        // If there's more to send (new entries, or we need to retry after a
        // conflict), fire off another AppendEntries immediately rather than
        // waiting for the next heartbeat tick.
        if let Some(env) = self.build_append_entries_for(from) {
            let needs_resend = !reply.success
                || match &self.role {
                    Role::Leader { next_index, .. } => {
                        next_index.get(&from).copied().unwrap_or(1) <= self.storage.last_index()
                    }
                    _ => false,
                };
            if needs_resend {
                effects.push(env);
            }
        }
        let committed = self.try_advance_commit_index();
        StepResult { effects, committed }
    }

    fn compute_backtrack_next_index(
        &self,
        conflict_term: Option<Term>,
        conflict_index: LogIndex,
    ) -> LogIndex {
        let Some(ct) = conflict_term else {
            return conflict_index;
        };
        let mut idx = self.storage.last_index();
        while idx > 0 {
            match self.storage.term_at(idx) {
                Some(t) if t == ct => return idx + 1,
                Some(t) if t < ct => break,
                _ => {}
            }
            idx -= 1;
        }
        conflict_index
    }

    /// The core Raft safety rule (§5.4.2): a leader may only advance
    /// `commit_index` to N if a majority of `match_index` values are >= N
    /// **and** the entry at N was written during the leader's *current*
    /// term. Committing a majority-replicated entry from an *earlier* term
    /// purely by vote count is the exact hazard the paper's Figure 8
    /// illustrates — it can be silently overwritten by a later leader. This
    /// rule is gate-tested directly in `tests/correctness.rs`.
    fn try_advance_commit_index(&mut self) -> Vec<Committed<C>> {
        let current_term = self.storage.current_term();
        let Role::Leader { match_index, .. } = &self.role else {
            return Vec::new();
        };
        let mut indices: Vec<LogIndex> = match_index.values().copied().collect();
        indices.push(self.storage.last_index()); // leader always matches its own log
        indices.sort_unstable_by(|a, b| b.cmp(a));
        let majority = self.majority();
        if indices.len() < majority {
            return Vec::new();
        }
        let candidate_n = indices[majority - 1];
        if candidate_n > self.commit_index
            && self.storage.term_at(candidate_n) == Some(current_term)
        {
            self.commit_index = candidate_n;
            return self.advance_last_applied_and_collect();
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;

    /// Test-only constructor that hand-builds a leader with a specific log
    /// and term, bypassing the normal election path. This module is a child
    /// of `node`, so it can reach into `RaftNode`'s private fields directly
    /// (ordinary Rust visibility rules) — no backdoor is exposed outside
    /// `#[cfg(test)]`. This is what lets the safety-rule test below set up
    /// the exact Figure-8-shaped scenario precisely, instead of hoping
    /// message timing in a full simulation happens to produce it.
    fn leader_with_log(
        current_term: Term,
        entries: Vec<(Term, LogCommand<i32>)>,
        peers: Vec<NodeId>,
    ) -> RaftNode<i32, MemStorage<i32>> {
        let mut storage: MemStorage<i32> = MemStorage::default();
        let log_entries: Vec<LogEntry<i32>> = entries
            .into_iter()
            .enumerate()
            .map(|(i, (term, command))| LogEntry {
                term,
                index: (i + 1) as u64,
                command,
            })
            .collect();
        storage.append(log_entries);
        storage.set_term_and_vote(current_term, Some(1));
        let mut node = RaftNode::new(1, peers.clone(), storage, 1, RaftConfig::default());
        let next_index: HashMap<NodeId, LogIndex> = peers
            .iter()
            .map(|&p| (p, node.storage.last_index() + 1))
            .collect();
        let match_index: HashMap<NodeId, LogIndex> = peers.iter().map(|&p| (p, 0)).collect();
        node.role = Role::Leader {
            next_index,
            match_index,
        };
        node.leader_id = Some(1);
        node
    }

    fn ack(term: Term, follower: NodeId, match_index: LogIndex) -> Envelope<i32> {
        Envelope {
            from: follower,
            to: 1,
            rpc: Rpc::AppendEntriesReply(AppendEntriesReply {
                term,
                success: true,
                follower,
                conflict_term: None,
                conflict_index: 0,
                match_index,
            }),
        }
    }

    /// The single most important safety property in Raft: a leader may only
    /// treat an entry as committed once *majority replication of an entry
    /// from its own current term* has happened — never from vote-counting a
    /// majority-replicated entry from an *older* term alone. This is
    /// exactly the hazard illustrated by Figure 8 in the Raft paper: if a
    /// leader could commit by counting replicas regardless of term, a
    /// later-elected leader (that never saw the entry) could overwrite an
    /// already "committed" entry, breaking the fundamental guarantee that
    /// committed entries are never lost.
    ///
    /// This test builds a 5-node leader (id=1) sitting in term 4 whose log
    /// contains an entry at index 5 written back in term 2 (as if it were
    /// replicated by a previous leader). It then feeds AppendEntriesReply
    /// messages directly, exactly as the network layer would, to prove:
    ///   1. Once a majority (3 of 5, including the leader) has replicated
    ///      index 5, `commit_index` must still be 0 — NOT 5 — because
    ///      index 5's term (2) is not the leader's current term (4).
    ///   2. Once the leader's own current-term entry (index 6) also reaches
    ///      a majority, `commit_index` jumps to 6 and index 5 (and all
    ///      earlier entries) become committed *transitively* — which is the
    ///      paper's actual rule, not an accident of this implementation.
    #[test]
    fn commit_index_requires_current_term_entry_replicated_to_majority() {
        let mut entries = vec![];
        for i in 1..=4u64 {
            entries.push((1, LogCommand::Command(i as i32)));
        }
        entries.push((2, LogCommand::Command(50))); // index 5, OLDER term (2), current term will be 4
        let mut node = leader_with_log(4, entries, vec![2, 3, 4, 5]);
        assert_eq!(node.storage.last_index(), 5);
        assert_eq!(node.commit_index, 0);

        // Follower 2 and follower 3 both ack up through index 5. Together
        // with the leader itself that's 3 of 5 nodes matching index 5 — a
        // majority by pure vote count.
        let r1 = node.receive(ack(4, 2, 5));
        assert!(r1.committed.is_empty());
        let r2 = node.receive(ack(4, 3, 5));
        assert!(
            r2.committed.is_empty(),
            "must NOT commit an older-term entry just because a majority replicated it \
             (this is the Figure 8 hazard from the Raft paper)"
        );
        assert_eq!(
            node.commit_index, 0,
            "commit_index must still be 0 after majority-replicating a stale-term entry"
        );

        // Now the leader's own no-op-style current-term entry (index 6,
        // term 4) also reaches the very same majority.
        node.storage.append(vec![LogEntry {
            term: 4,
            index: 6,
            command: LogCommand::<i32>::NoOp,
        }]);
        let r3 = node.receive(ack(4, 2, 6));
        assert!(
            r3.committed.is_empty(),
            "only 2 of 5 nodes (leader + follower 2) match index 6 so far"
        );
        let r4 = node.receive(ack(4, 3, 6));

        assert_eq!(
            node.commit_index, 6,
            "current-term entry replicated to majority must commit"
        );
        assert_eq!(
            r4.committed.len(),
            5,
            "indices 1..=5 (all Command entries) become committed transitively; index 6 is a NoOp and isn't surfaced"
        );
        assert!(
            r4.committed.iter().any(|c| c.index == 5 && c.command == 50),
            "the older-term entry at index 5 must be among the newly committed entries, safely, \
             once it rides in transitively behind a current-term commit"
        );
    }

    #[test]
    fn single_node_cluster_commits_immediately_on_propose() {
        let storage: MemStorage<i32> = MemStorage::default();
        let mut node = RaftNode::new(1, vec![], storage, 1, RaftConfig::default());
        // No peers: starting an election immediately wins (peers.is_empty()).
        let result = node.start_election();
        assert!(node.is_leader());
        // Becoming leader appends + commits its own no-op (majority of 1).
        assert_eq!(node.commit_index(), 1);
        assert!(
            result.committed.is_empty(),
            "the no-op itself is never surfaced to the state machine"
        );

        let (index, step) = node.propose(99).expect("leader can propose");
        assert_eq!(index, 2);
        assert_eq!(step.committed.len(), 1);
        assert_eq!(step.committed[0].command, 99);
        assert_eq!(node.commit_index(), 2);
    }

    #[test]
    fn two_node_election_reaches_majority_with_one_vote() {
        let storage: MemStorage<i32> = MemStorage::default();
        let mut candidate = RaftNode::new(1, vec![2], storage, 1, RaftConfig::default());
        let step = candidate.start_election();
        assert_eq!(candidate.role(), RoleKind::Candidate);
        assert_eq!(step.effects.len(), 1);
        let Rpc::RequestVote(args) = &step.effects[0].rpc else {
            panic!("expected RequestVote")
        };
        assert_eq!(args.term, 1);

        let reply = RequestVoteReply {
            term: 1,
            vote_granted: true,
            voter: 2,
        };
        let result = candidate.receive(Envelope {
            from: 2,
            to: 1,
            rpc: Rpc::RequestVoteReply(reply),
        });
        assert!(
            candidate.is_leader(),
            "1 self-vote + 1 granted vote = majority of 2"
        );
        assert!(
            !result.effects.is_empty(),
            "new leader immediately replicates its no-op"
        );
    }

    #[test]
    fn vote_is_refused_when_candidate_log_is_less_up_to_date() {
        // Follower has a longer log (2 entries at term 1) than the
        // candidate's claimed log (empty) — the up-to-date check in
        // §5.4.1 must refuse the vote.
        let mut storage: MemStorage<i32> = MemStorage::default();
        storage.append(vec![
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
        let mut follower = RaftNode::new(2, vec![1, 3], storage, 2, RaftConfig::default());
        let args = RequestVoteArgs {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        let result = follower.receive(Envelope {
            from: 1,
            to: 2,
            rpc: Rpc::RequestVote(args),
        });
        let Rpc::RequestVoteReply(reply) = &result.effects[0].rpc else {
            panic!("expected reply")
        };
        assert!(
            !reply.vote_granted,
            "must not vote for a candidate with a less up-to-date log"
        );
    }

    #[test]
    fn append_entries_conflict_is_backtracked_and_truncated() {
        // Follower has a conflicting entry at index 2 (term 1) that the
        // real leader's log disagrees with (leader has term 2 at index 2).
        let mut follower_storage: MemStorage<i32> = MemStorage::default();
        follower_storage.append(vec![
            LogEntry {
                term: 1,
                index: 1,
                command: LogCommand::Command(10),
            },
            LogEntry {
                term: 1,
                index: 2,
                command: LogCommand::Command(20),
            }, // conflicting
        ]);
        follower_storage.set_term_and_vote(1, None);
        let mut follower = RaftNode::new(2, vec![1], follower_storage, 2, RaftConfig::default());

        let args = AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 2,
                index: 2,
                command: LogCommand::Command(200),
            }],
            leader_commit: 0,
        };
        let result = follower.receive(Envelope {
            from: 1,
            to: 2,
            rpc: Rpc::AppendEntries(args),
        });
        let Rpc::AppendEntriesReply(reply) = &result.effects[0].rpc else {
            panic!("expected reply")
        };
        assert!(reply.success);
        assert_eq!(follower.log_len(), 2);
        assert_eq!(
            follower.storage.entry(2).unwrap().command,
            LogCommand::Command(200)
        );
    }
}
