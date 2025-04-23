#!/bin/bash

# Run all checks locally

cargo fmt --check
cargo clippy --no-deps --all-targets --all-features -- --deny warnings
RUSTFLAGS="-D warnings" cargo check --all-targets --all-features
cargo test --all-targets --all-features
cargo test --test storage-full --features="failpoints/failpoints"
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
