//! End-to-end correctness tests, run through the deterministic simulated
//! network (`raft_consensus::Cluster`), exactly as described in the README.
//!
//! Every scenario here is deterministic: fixed seeds, tick-driven, no
//! `sleep`, no wall-clock reliance. Given the same seed, every run produces
//! bit-identical behavior — that's what lets fault-injection scenarios
//! (crash, partition) live in CI without flakiness.
//!
//! Where a scenario is run under several seeds, it's to demonstrate the
//! property holds broadly (different random election-timeout orderings),
//! not because any individual run is flaky.

use raft_consensus::cluster::Cluster;
use raft_consensus::node::RaftConfig;
use raft_consensus::sim::NetworkConfig;
use raft_consensus::state_machine::{KvCommand, KvStore};

fn fast_config() -> RaftConfig {
    RaftConfig {
        min_election_ticks: 10,
        max_election_ticks: 20,
        heartbeat_ticks: 3,
    }
}

fn reliable_net() -> NetworkConfig {
    NetworkConfig {
        min_latency_ticks: 1,
        max_latency_ticks: 3,
        drop_probability: 0.0,
    }
}

fn lossy_net() -> NetworkConfig {
    NetworkConfig {
        min_latency_ticks: 1,
        max_latency_ticks: 4,
        drop_probability: 0.05,
    }
}

fn set_cmd(k: &str, v: &str) -> KvCommand {
    KvCommand::Set {
        key: k.to_string(),
        value: v.to_string(),
    }
}

// ---------------------------------------------------------------------------
// 1. Leader election, no partition.
// ---------------------------------------------------------------------------

#[test]
fn elects_exactly_one_leader_with_no_partition_3_nodes() {
    for seed in 0..10u64 {
        let mut cluster: Cluster<KvStore> =
            Cluster::new(&[1, 2, 3], fast_config(), reliable_net(), seed);
        let leader = cluster.run_until_leader(200);
        assert!(
            leader.is_some(),
            "seed {seed}: no leader elected within 200 ticks"
        );
        cluster.assert_at_most_one_leader_per_term();

        let leader_count = cluster
            .all_ids()
            .iter()
            .filter(|&&id| cluster.nodes.get(&id).unwrap().is_leader())
            .count();
        assert_eq!(leader_count, 1, "seed {seed}: expected exactly one leader");
    }
}

#[test]
fn elects_exactly_one_leader_with_no_partition_5_nodes() {
    for seed in 0..10u64 {
        let mut cluster: Cluster<KvStore> =
            Cluster::new(&[1, 2, 3, 4, 5], fast_config(), reliable_net(), seed);
        let leader = cluster.run_until_leader(200);
        assert!(
            leader.is_some(),
            "seed {seed}: no leader elected within 200 ticks"
        );
        cluster.assert_at_most_one_leader_per_term();
    }
}

#[test]
fn replicates_log_to_all_nodes_and_produces_identical_state_machines() {
    let mut cluster: Cluster<KvStore> =
        Cluster::new(&[1, 2, 3, 4, 5], fast_config(), reliable_net(), 1);
    let leader = cluster.run_until_leader(200).expect("leader elected");

    let commands = vec![
        set_cmd("a", "1"),
        set_cmd("b", "2"),
        KvCommand::Delete {
            key: "a".to_string(),
        },
        set_cmd("c", "3"),
    ];
    let mut last_index = 0;
    for cmd in commands {
        last_index = cluster
            .propose(leader, cmd)
            .expect("leader accepts proposal");
    }

    let all = cluster.all_ids();
    assert!(
        cluster.run_until_committed(&all, last_index, 300),
        "all nodes should commit every proposed entry"
    );

    // Hand-compute the expected converged state instead of trusting whatever
    // the code produced.
    let mut expected = std::collections::BTreeMap::new();
    expected.insert("b".to_string(), "2".to_string());
    expected.insert("c".to_string(), "3".to_string());

    for id in all {
        assert_eq!(
            cluster.machine(id).map,
            expected,
            "node {id} diverged from expected converged state"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Fault tolerance: leader crash -> re-election -> continued replication.
// ---------------------------------------------------------------------------

#[test]
fn fault_tolerance_new_leader_elected_after_leader_crash_and_replication_continues() {
    let mut cluster: Cluster<KvStore> =
        Cluster::new(&[1, 2, 3, 4, 5], fast_config(), reliable_net(), 7);
    let leader1 = cluster
        .run_until_leader(200)
        .expect("initial leader elected");

    let idx1 = cluster.propose(leader1, set_cmd("k1", "v1")).unwrap();
    let all = cluster.all_ids();
    assert!(cluster.run_until_committed(&all, idx1, 200));

    // Simulate the leader crashing.
    cluster.disconnect(leader1);
    let remaining: Vec<u64> = all.iter().copied().filter(|&id| id != leader1).collect();

    // A new leader must emerge among the remaining 4 nodes, in a higher term.
    // Note: the crashed (disconnected) node legitimately keeps believing
    // it's still leader in its old term -- that's expected Raft behavior,
    // not a bug -- so we look for a leader specifically among the *reachable*
    // nodes rather than asking the ambiguous "who is the leader" question.
    let term_before = cluster.nodes.get(&leader1).unwrap().current_term();
    let mut new_leader = None;
    for _ in 0..300 {
        cluster.tick();
        cluster.assert_at_most_one_leader_per_term();
        if let Some(l) = cluster.leader_among(&remaining) {
            new_leader = Some(l);
            break;
        }
    }
    let leader2 = new_leader.expect("a new leader should be elected after the old leader crashes");
    assert_ne!(leader2, leader1);
    assert!(
        cluster.nodes.get(&leader2).unwrap().current_term() > term_before,
        "new leader's term must be strictly greater than the crashed leader's term"
    );

    // Replication continues among the reachable majority.
    let idx2 = cluster
        .propose(leader2, set_cmd("k2", "v2"))
        .expect("new leader accepts proposals");
    assert!(
        cluster.run_until_committed(&remaining, idx2, 300),
        "surviving nodes must commit the new entry"
    );

    let mut expected = std::collections::BTreeMap::new();
    expected.insert("k1".to_string(), "v1".to_string());
    expected.insert("k2".to_string(), "v2".to_string());
    for &id in &remaining {
        assert_eq!(
            cluster.machine(id).map,
            expected,
            "node {id} should have both entries applied"
        );
    }

    // Reconnect the old leader: it must step down (see a higher term) and
    // catch its log + state machine up to match everyone else exactly.
    cluster.reconnect(leader1);
    assert!(
        cluster.run_until_committed(&all, idx2, 300),
        "reconnected old leader must catch up"
    );
    cluster.assert_at_most_one_leader_per_term();
    for id in &all {
        assert_eq!(
            cluster.machine(*id).map,
            expected,
            "node {id} must converge after the crashed leader rejoins"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Network partition: minority can't commit, majority can, then heals.
// ---------------------------------------------------------------------------

#[test]
fn minority_partition_cannot_commit_majority_can_and_partition_heals_correctly() {
    let mut cluster: Cluster<KvStore> =
        Cluster::new(&[1, 2, 3, 4, 5], fast_config(), reliable_net(), 11);
    let leader = cluster
        .run_until_leader(200)
        .expect("initial leader elected");

    let idx0 = cluster.propose(leader, set_cmd("base", "0")).unwrap();
    let all = cluster.all_ids();
    assert!(cluster.run_until_committed(&all, idx0, 200));

    // Partition so the current leader ends up alone with one follower
    // (minority of 2), and the other three form the majority side.
    let others: Vec<u64> = all.iter().copied().filter(|&id| id != leader).collect();
    let minority_partner = others[0];
    let majority_side: Vec<u64> = others[1..].to_vec();
    let minority_side = vec![leader, minority_partner];
    cluster.partition(vec![minority_side.clone(), majority_side.clone()]);

    // The (old) leader, still isolated with only 1 follower, accepts a
    // proposal locally but must never be able to commit it (needs 3 of 5).
    let stuck_index = cluster
        .propose(leader, set_cmd("stuck", "should-not-commit"))
        .unwrap();
    cluster.run_ticks(150);
    cluster.assert_at_most_one_leader_per_term();
    assert!(
        cluster.commit_index(leader) < stuck_index,
        "the minority-side leader must not be able to commit without a majority"
    );

    // The majority side must independently elect its own leader (it has
    // exactly 3 of 5 nodes, which is a majority) and be able to commit.
    let mut majority_leader = None;
    for _ in 0..300 {
        cluster.tick();
        cluster.assert_at_most_one_leader_per_term();
        if let Some(l) = majority_side
            .iter()
            .copied()
            .find(|&id| cluster.nodes.get(&id).unwrap().is_leader())
        {
            majority_leader = Some(l);
            break;
        }
    }
    let majority_leader =
        majority_leader.expect("majority partition must be able to elect its own leader");

    let idx1 = cluster
        .propose(majority_leader, set_cmd("progress", "1"))
        .expect("majority leader accepts proposals");
    assert!(
        cluster.run_until_committed(&majority_side, idx1, 300),
        "majority side must be able to commit new entries while partitioned"
    );

    // Heal the partition and let everything settle.
    cluster.heal_partition();
    assert!(
        cluster.run_until_committed(&all, idx1, 400),
        "all nodes must resync after the partition heals"
    );
    cluster.assert_at_most_one_leader_per_term();
    cluster.assert_committed_logs_agree();

    let mut expected = std::collections::BTreeMap::new();
    expected.insert("base".to_string(), "0".to_string());
    expected.insert("progress".to_string(), "1".to_string());
    for id in &all {
        assert_eq!(
            cluster.machine(*id).map,
            expected,
            "node {id}: the never-committed 'stuck' entry must NOT appear anywhere after healing"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Log safety across successive leader changes (the core Raft guarantee).
// ---------------------------------------------------------------------------

/// This is the property Raft exists to guarantee: once an entry is
/// committed (replicated to a majority under the rule that requires a
/// current-term entry to also be replicated — see the unit test
/// `node::tests::commit_index_requires_current_term_entry_replicated_to_majority`
/// for that rule tested in isolation), it can never be lost or overwritten,
/// no matter how many leader changes happen afterwards.
///
/// This test drives that guarantee end-to-end through the real simulated
/// network across *three* successive leader crashes, checking after every
/// round that every previously committed entry is still there, byte-for-byte,
/// on every reachable node — not just that the final sizes match.
#[test]
fn committed_entries_survive_multiple_successive_leader_changes() {
    let mut cluster: Cluster<KvStore> =
        Cluster::new(&[1, 2, 3, 4, 5], fast_config(), reliable_net(), 21);
    let all = cluster.all_ids();

    let mut committed_so_far: Vec<KvCommand> = Vec::new();
    let mut last_index = 0u64;
    // At most one node is ever disconnected at a time (see below) -- a real
    // Raft cluster cannot make progress without a majority, so cumulatively
    // crashing 3 of 5 nodes without ever letting one back in would make
    // this an unreachable-majority test, not a leader-change test. Each
    // round instead: bring back whoever was crashed last round, *then*
    // crash the new current leader, keeping 4 of 5 nodes reachable at all
    // times.
    let mut previously_crashed: Option<u64> = None;

    for round in 0..4 {
        let leader = if round == 0 {
            cluster
                .run_until_leader(200)
                .expect("initial leader elected")
        } else {
            if let Some(prev) = previously_crashed.take() {
                cluster.reconnect(prev);
                assert!(
                    cluster.run_until_committed(&all, last_index, 400),
                    "round {round}: previously-crashed node must catch up after reconnecting"
                );
            }
            let old_leader = cluster
                .leader_among(&all)
                .expect("a leader exists before crashing it");
            cluster.disconnect(old_leader);
            previously_crashed = Some(old_leader);
            let alive: Vec<u64> = all.iter().copied().filter(|&id| id != old_leader).collect();

            let mut elected = None;
            for _ in 0..400 {
                cluster.tick();
                cluster.assert_at_most_one_leader_per_term();
                cluster.assert_committed_logs_agree();
                if let Some(l) = cluster.leader_among(&alive) {
                    elected = Some(l);
                    break;
                }
            }
            elected.unwrap_or_else(|| {
                panic!("round {round}: no new leader elected after crashing {old_leader}")
            })
        };

        let alive: Vec<u64> = all
            .iter()
            .copied()
            .filter(|&id| Some(id) != previously_crashed)
            .collect();

        let key = format!("round{round}");
        let cmd = set_cmd(&key, &round.to_string());
        last_index = cluster
            .propose(leader, cmd.clone())
            .expect("current leader accepts proposal");
        assert!(
            cluster.run_until_committed(&alive, last_index, 400),
            "round {round}: surviving nodes must commit the new entry"
        );
        committed_so_far.push(cmd);

        // The critical check: every previously committed entry must still
        // be present, unchanged, on every currently-alive node.
        cluster.assert_committed_logs_agree();
        let mut expected = std::collections::BTreeMap::new();
        for c in &committed_so_far {
            if let KvCommand::Set { key, value } = c {
                expected.insert(key.clone(), value.clone());
            }
        }
        for &id in &alive {
            assert_eq!(
                cluster.machine(id).map,
                expected,
                "round {round}, node {id}: a previously committed entry was lost or altered"
            );
        }
    }

    // Bring every crashed node back and confirm the whole cluster, including
    // the ones that crashed earlier, converges to the exact same final state.
    for &id in &all {
        cluster.reconnect(id);
    }
    assert!(
        cluster.run_until_committed(&all, last_index, 500),
        "all 5 nodes must fully resync at the end"
    );
    cluster.assert_at_most_one_leader_per_term();
    cluster.assert_committed_logs_agree();

    let mut expected = std::collections::BTreeMap::new();
    for c in &committed_so_far {
        if let KvCommand::Set { key, value } = c {
            expected.insert(key.clone(), value.clone());
        }
    }
    for id in &all {
        assert_eq!(
            cluster.machine(*id).map,
            expected,
            "node {id}: final full-cluster convergence mismatch"
        );
    }
}

// ---------------------------------------------------------------------------
// 5. Robustness under lossy/latent links (still deterministic via seeding).
// ---------------------------------------------------------------------------

#[test]
fn cluster_still_converges_under_message_loss_and_variable_latency() {
    for seed in 0..5u64 {
        let mut cluster: Cluster<KvStore> =
            Cluster::new(&[1, 2, 3, 4, 5], fast_config(), lossy_net(), seed);
        let leader = cluster
            .run_until_leader(400)
            .expect("leader elected even with message loss");
        let idx = cluster
            .propose(leader, set_cmd("x", "1"))
            .expect("proposal accepted");
        let all = cluster.all_ids();
        assert!(
            cluster.run_until_committed(&all, idx, 2000),
            "seed {seed}: cluster must still converge under 5% message loss, just possibly slower"
        );
        cluster.assert_committed_logs_agree();
    }
}
