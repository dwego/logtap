# --- build stage ---
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

# Cache dependency compilation separately from source changes: with only
# Cargo.toml/Cargo.lock copied, `cargo build` here only re-runs when
# dependencies actually change, not on every source edit.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "" > src/lib.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
# BuildKit normalizes COPY'd file timestamps, which defeats cargo's
# mtime-based rebuild detection — without this, cargo can decide nothing
# changed and silently keep linking the dummy binary from the step above.
RUN find src -name '*.rs' -exec touch {} + && cargo build --release

# --- runtime stage ---
FROM debian:bookworm-slim

RUN useradd --system --create-home --home-dir /app logtap
WORKDIR /app

COPY --from=builder /app/target/release/logtap /usr/local/bin/logtap

USER logtap

# logtap.toml and the source log file are expected to be mounted in at
# runtime (see README) — the binary itself carries no config or state.
ENTRYPOINT ["logtap"]
