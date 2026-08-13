# Raft-Consensus — runtime image for the simulated-cluster demo.
#
# This is deliberately NOT a "cargo build inside Docker" multi-stage image.
# On this development machine, TLS traffic *inside* Docker containers is
# intercepted by a corporate proxy/inspection appliance whose CA is trusted
# on the host but not inside a fresh `rust:*` build container, so a
# `cargo build` that needs to fetch crates from crates.io during `docker
# build` fails with certificate verification errors. Rather than fight that
# per-environment problem, the binary is built on the HOST (which already
# has the right trust store) via the normal, required, verified step:
#
#     cargo build --release --bin raft-demo
#     mkdir -p dist && cp target/release/raft-demo dist/
#     docker build -t raft-consensus .
#
# This image then does zero network access during `docker build` beyond
# pulling the base image itself: it only copies the already-built binary
# into a minimal runtime layer. The base image is `ubuntu:24.04` (not
# `debian:*-slim`) specifically because it was built and tested on Ubuntu
# 24.04 with glibc 2.39 — a binary linked against glibc 2.39 will not run on
# a Debian bookworm base (glibc ~2.36), so the runtime base is matched to
# the build host's glibc version. CI builds this same way: `cargo build
# --release` on the runner, then `docker build`, so the image in CI is
# exercised through the identical path (see .github/workflows/ci.yml).
FROM ubuntu:24.04

LABEL org.opencontainers.image.source="https://github.com/sfeirc/Raft-Consensus"
LABEL org.opencontainers.image.description="From-scratch Raft consensus implementation: leader election, log replication, and a deterministic in-memory network simulator, demoed via a 5-node cluster with a simulated leader crash"
LABEL org.opencontainers.image.licenses="MIT"

COPY dist/raft-demo /usr/local/bin/raft-demo

ENTRYPOINT ["/usr/local/bin/raft-demo"]
