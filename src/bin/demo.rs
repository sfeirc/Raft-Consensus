//! Scripted, non-interactive demo of the simulated Raft cluster.
//!
//! This is what `docker run` executes to prove the image actually works: it
//! spins up a 5-node cluster over the in-memory network simulator, elects a
//! leader, replicates a handful of key-value commands, kills the leader,
//! confirms a new one takes over and replication continues, brings the old
//! leader back, and finally prints every node's converged state so you can
//! see with your own eyes that they're identical. It exits non-zero if
//! anything along the way fails, so a green `docker run` genuinely means
//! something.

use raft_consensus::cluster::Cluster;
use raft_consensus::node::RaftConfig;
use raft_consensus::sim::NetworkConfig;
use raft_consensus::state_machine::{KvCommand, KvStore};

fn set(k: &str, v: &str) -> KvCommand {
    KvCommand::Set {
        key: k.to_string(),
        value: v.to_string(),
    }
}

fn print_states(cluster: &Cluster<KvStore>) {
    let mut ids = cluster.all_ids();
    ids.sort_unstable();
    for id in ids {
        println!("    node {id}: {:?}", cluster.machine(id).map);
    }
}

fn main() {
    println!("=== Raft-Consensus demo: 5-node simulated cluster ===\n");

    let config = RaftConfig {
        min_election_ticks: 10,
        max_election_ticks: 20,
        heartbeat_ticks: 3,
    };
    let net = NetworkConfig {
        min_latency_ticks: 1,
        max_latency_ticks: 3,
        drop_probability: 0.0,
    };
    let mut cluster: Cluster<KvStore> = Cluster::new(&[1, 2, 3, 4, 5], config, net, 42);

    print!("[1] Electing an initial leader ... ");
    let leader1 = cluster
        .run_until_leader(200)
        .expect("a leader must be elected");
    println!(
        "leader = node {leader1} (term {})",
        cluster.nodes.get(&leader1).unwrap().current_term()
    );

    println!("[2] Replicating 3 client commands through the leader ...");
    let mut last_index = 0;
    for (k, v) in [
        ("service", "checkout"),
        ("region", "eu-west-1"),
        ("replicas", "5"),
    ] {
        last_index = cluster
            .propose(leader1, set(k, v))
            .expect("leader accepts proposal");
    }
    let all = cluster.all_ids();
    let ok = cluster.run_until_committed(&all, last_index, 300);
    assert!(ok, "all nodes must commit the replicated entries");
    println!("    replicated + committed on all {} nodes:", all.len());
    print_states(&cluster);

    println!("\n[3] Crashing the leader (node {leader1}) ...");
    let remaining: Vec<u64> = all.iter().copied().filter(|&id| id != leader1).collect();
    cluster.disconnect(leader1);
    // Look for a leader specifically among the reachable nodes: the crashed
    // node legitimately keeps believing it's still leader in its old term
    // (correct Raft behavior, not a bug), so naively asking "who is the
    // current leader" is ambiguous while both self-proclaimed leaders exist
    // at once.
    let mut leader2 = None;
    for _ in 0..300 {
        cluster.tick();
        if let Some(l) = cluster.leader_among(&remaining) {
            leader2 = Some(l);
            break;
        }
    }
    let leader2 = leader2.expect("a new leader must be elected after the crash");
    println!(
        "    new leader = node {leader2} (term {}) — election completed in a fresh term, as expected",
        cluster.nodes.get(&leader2).unwrap().current_term()
    );

    println!("[4] Replicating another command through the new leader ...");
    let idx2 = cluster
        .propose(leader2, set("failover", "confirmed"))
        .expect("new leader accepts proposal");
    let ok = cluster.run_until_committed(&remaining, idx2, 300);
    assert!(ok, "surviving nodes must commit the new entry");
    println!("    state on the 4 surviving nodes:");
    print_states(&cluster);

    println!("\n[5] Reconnecting the old leader (node {leader1}) ...");
    cluster.reconnect(leader1);
    let ok = cluster.run_until_committed(&all, idx2, 400);
    assert!(
        ok,
        "the rejoined node must catch its log up to the rest of the cluster"
    );
    cluster.assert_at_most_one_leader_per_term();
    cluster.assert_committed_logs_agree();

    println!(
        "    final state on all {} nodes (should be identical):",
        all.len()
    );
    print_states(&cluster);

    let reference = cluster.machine(all[0]).map.clone();
    let all_match = all.iter().all(|&id| cluster.machine(id).map == reference);
    assert!(
        all_match,
        "every node must converge to byte-identical state"
    );

    println!("\n=== Demo complete: leader election, log replication, a simulated leader crash + re-election,");
    println!("=== and post-crash convergence all verified in-process. Exiting 0. ===");
}
