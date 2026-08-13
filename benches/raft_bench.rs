//! Real, measured benchmarks — run with `cargo bench` and reported in the
//! README with the exact numbers this produced on the machine used to build
//! this repository (see the README's "Benchmarks" section for methodology
//! and the honest caveats about what "election latency" means when there's
//! no real network in the loop).
//!
//! Two benchmarks:
//!   1. Log replication throughput: with the simulated network latency
//!      pinned to its minimum (1 tick, i.e. "as fast as the simulator can
//!      go"), how many client commands per second can a leader propose,
//!      replicate, and get committed across the whole cluster? This is
//!      fundamentally a CPU-bound measurement of this implementation's
//!      per-entry overhead (log/hashmap bookkeeping, RPC construction), not
//!      a network benchmark.
//!   2. Election latency: wall-clock time for the simulator to run the
//!      "leader crashes -> new leader elected" scenario to completion, for a
//!      few fixed seeds (fixed, not random, so the benchmark itself is
//!      reproducible sample-to-sample).

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use raft_consensus::cluster::Cluster;
use raft_consensus::node::RaftConfig;
use raft_consensus::sim::NetworkConfig;
use raft_consensus::state_machine::{KvCommand, KvStore};

fn zero_latency_net() -> NetworkConfig {
    NetworkConfig {
        min_latency_ticks: 1,
        max_latency_ticks: 1,
        drop_probability: 0.0,
    }
}

fn default_config() -> RaftConfig {
    RaftConfig {
        min_election_ticks: 10,
        max_election_ticks: 20,
        heartbeat_ticks: 3,
    }
}

fn bench_replication_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_replication_throughput");
    // Each iteration proposes+replicates+commits this many entries in one
    // shot, so the per-iteration cost is substantial (hundreds of ms) —
    // BatchSize::PerIteration is used (instead of the default SmallInput)
    // so criterion times each iteration on its own rather than lumping
    // several setup+run cycles into one timed batch, which would otherwise
    // blow up wall-clock time and make the reported variance meaningless.
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(8));
    const N_ENTRIES: u64 = 500;

    for cluster_size in [3u64, 5] {
        group.throughput(Throughput::Elements(N_ENTRIES));
        group.bench_function(format!("{cluster_size}_node_cluster"), |b| {
            b.iter_batched(
                || {
                    let ids: Vec<u64> = (1..=cluster_size).collect();
                    let mut cluster: Cluster<KvStore> =
                        Cluster::new(&ids, default_config(), zero_latency_net(), 1);
                    let leader = cluster
                        .run_until_leader(200)
                        .expect("leader elected in setup");
                    let all = cluster.all_ids();
                    (cluster, leader, all)
                },
                |(mut cluster, leader, all)| {
                    for i in 0..N_ENTRIES {
                        let idx = cluster
                            .propose(
                                leader,
                                KvCommand::Set {
                                    key: format!("k{i}"),
                                    value: format!("v{i}"),
                                },
                            )
                            .expect("leader accepts proposal");
                        // At 1-tick minimum latency a handful of ticks is
                        // always enough for the ack round trip to land.
                        let ok = cluster.run_until_committed(&all, idx, 20);
                        assert!(ok, "entry must commit within the tick budget");
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_election_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("election_latency_after_leader_crash");
    // Fixed seeds: the benchmark itself must be reproducible sample-to-
    // sample, so we don't draw a fresh random seed per iteration -- we
    // measure the same deterministic crash/election scenario repeatedly,
    // for a few representative seeds.
    for seed in [1u64, 2, 3] {
        group.bench_function(format!("5_node_cluster_seed_{seed}"), |b| {
            b.iter_batched(
                || {
                    let mut cluster: Cluster<KvStore> =
                        Cluster::new(&[1, 2, 3, 4, 5], default_config(), zero_latency_net(), seed);
                    let leader = cluster
                        .run_until_leader(200)
                        .expect("leader elected in setup");
                    let all = cluster.all_ids();
                    let remaining: Vec<u64> = all.into_iter().filter(|&id| id != leader).collect();
                    cluster.disconnect(leader);
                    (cluster, remaining)
                },
                |(mut cluster, remaining)| {
                    for _ in 0..500 {
                        cluster.tick();
                        if cluster.leader_among(&remaining).is_some() {
                            break;
                        }
                    }
                    assert!(
                        cluster.leader_among(&remaining).is_some(),
                        "must elect a new leader within budget"
                    );
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_replication_throughput,
    bench_election_latency
);
criterion_main!(benches);
