//! RPC message types (RequestVote, AppendEntries) as defined in the Raft
//! paper, Figure 2. These are plain data, transport-agnostic: this crate
//! delivers them via an in-memory simulated network (see [`crate::sim`]),
//! not sockets or gRPC. See the crate-level docs / README for why.

use crate::storage::LogEntry;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
    pub voter: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesArgs<C> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    pub follower: NodeId,
    /// Fast conflict backtracking (paper §5.3 optimization). When
    /// `success == false` because of a log mismatch, the leader can jump
    /// `next_index` straight to `conflict_index` (or, if `conflict_term` is
    /// known, to the end of that term in its own log) instead of
    /// decrementing by one entry per round trip.
    pub conflict_term: Option<Term>,
    pub conflict_index: LogIndex,
    /// Index the follower's log now matches the leader's up to, on success.
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rpc<C> {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs<C>),
    AppendEntriesReply(AppendEntriesReply),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<C> {
    pub from: NodeId,
    pub to: NodeId,
    pub rpc: Rpc<C>,
}
