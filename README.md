# logtap

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/dwego/logtap/actions/workflows/ci.yml/badge.svg)](https://github.com/dwego/logtap/actions/workflows/ci.yml)

An asynchronous, minimalist log aggregator written in Rust, built around one core idea: **real backpressure**. The pipeline should never blow up memory just because the destination got slow.

> **Status:** early-stage / MVP. The core pipeline works end-to-end and is covered by tests, but the project is still small on purpose — see [Roadmap](#roadmap) for what's deliberately not built yet.

## What it does

`logtap` watches a log source (today, a file; `stdin` is planned), turns each line into a structured record, applies filter and masking rules, and delivers everything in batches to an HTTP destination — without ever letting the internal work queue grow without bound.

Think of it as a small, readable take on tools like Fluentd or Vector: it doesn't compete on feature count, it competes on being easy to understand end to end, from the first line of code to the last.

## Architecture

The project is organized as a pipeline of independent stages, each running as its own async task, connected only by channels:

```
log file
   │
   ▼
source.rs  ──String──▶  parser.rs  ──LogLine──▶  filter.rs  ──LogLine──▶  sink.rs
(tails the                (raw text becomes        (drops and              (batches records
 file via                  structured JSON)          masks fields            and sends them
 notify/inotify)                                      per rule)               over HTTP)
```

Every arrow is a `tokio::sync::mpsc::channel` with a bounded capacity (`channel_capacity`, configurable). That bounded capacity is the entire design in one sentence: when a stage produces faster than the next one can consume, the channel fills up and the sending call (`send().await` / `blocking_send()`) simply waits — no unbounded queue, no runaway `Vec`, no process getting OOM-killed.

**Why separate stages instead of one big function.** Each piece (`source`, `parser`, `filter`, `sink`) only knows about its input channel and its output channel. That means swapping the internal implementation of any stage — for example, changing how `source.rs` watches the file, or swapping the HTTP client used in `sink.rs` — doesn't require touching any other file, as long as the function signature stays the same.

**The central type, `LogLine`.** Rather than a fixed struct with predefined fields, `LogLine` is a type alias for `serde_json::Value`. Real-world logs rarely share a single schema — every application logs different fields — and forcing a rigid struct would mean the parser silently drops or truncates whatever doesn't fit. `Value` accepts any JSON log shape without `logtap` needing to know the field set up front.

**Filtering as configuration, not code.** What to drop and what to mask lives in `logtap.toml`, not in `if` statements scattered through the codebase. `FilterRule` entries match on a field using `Equals`, `Contains`, or `Regex`, and either `Drop` the record or `Mask` the field (replacing its value with `"***"`). This lets operators change behavior without recompiling — including masking sensitive fields such as emails or API keys — by adding a rule, rather than depending on someone remembering to write one into the source.

## How this is meant to scale

"Scaling" here doesn't just mean "handle more volume" — it means three distinct things, and the project is structured so each can evolve independently.

### 1. Volume (more logs per second)

The natural bottleneck in a staged pipeline is its slowest stage — usually the sink, since it depends on the network. The current architecture already absorbs this in two ways:

- **Batching amortizes network cost.** Instead of one HTTP request per line, logs accumulate until `batch_size` or `flush_interval_secs` is reached, whichever comes first. The fixed cost of each request (handshake, round-trip latency) is spread across dozens or hundreds of log lines instead of paid on every single one.
- **Backpressure protects the process, not just the sink.** If inbound volume spikes (a traffic burst, an incident generating excessive logs), the bounded channels absorb the pressure without letting memory grow — the source's own read rate slows down automatically as a side effect.

The natural next step for volume is parallelizing the sink itself: today there's a single `run_sink` task consuming from one channel. Nothing prevents running multiple sink tasks reading from the same channel (`tokio::mpsc` already supports multiple concurrent consumers), as long as the destination can handle concurrent requests.

### 2. Surface area (more sources, more destinations, more formats)

Today `source.rs` and `sink.rs` are single files with no trait abstraction — a deliberate choice for the MVP, to avoid paying the cost of indirection before a second real implementation exists. This is exactly where the project is expected to grow:

```
source/             sink/
├── mod.rs (trait)   ├── mod.rs (trait)
├── file.rs          ├── http.rs
└── stdin.rs         ├── stdout.rs
                      └── file.rs
```

Once there's a second source (`stdin`, alongside the file tail) or a second destination (a local audit log as backup, alongside HTTP), it's worth introducing the trait — and the refactor stays small, because the rest of the pipeline (`parser`, `filter`, the channels) doesn't change at all. The same logic applies to fanning out to multiple sinks at once — shipping the same log to HTTP and to a local audit file without duplicating filter logic.

### 3. Reliability (the system must not silently lose data)

This is the most important axis, and the first one that needs work. Today, if a batch fails to send, it's dropped — no retry, no persistence. That's acceptable for an MVP, but it's the first thing that needs to change before `logtap` is trusted with data that actually matters:

- **Retry with exponential backoff** in the sink before giving up on a batch.
- **A local dead-letter file** — batches that fail repeatedly get written to disk (e.g. `logtap.failed.jsonl`) instead of lost, so they can be reprocessed later.
- **Log rotation detection** — when the watched file is renamed and recreated (standard `logrotate` behavior in production), the source needs to notice and start reading the new file, instead of continuing to hold a handle to a file that no longer exists.

### 4. Operability (visibility into the pipeline itself)

A log shipper nobody can observe is a blind spot inside another observability system — which is a bit ironic. The natural direction here:

- **Internal metrics**: lines read, parsed, dropped by filter, dropped by parse error, sent successfully, failed.
- **A Prometheus-format `/metrics` endpoint**, so `logtap` can be monitored by the same tooling that watches the rest of the infrastructure.
- **Degradation signals, not just outright failure** — for example, exposing when a channel is consistently near capacity, which is the earliest symptom of a downstream stage slowing down, before it turns into actual data loss.

## What stays fixed as the project grows

Whichever of these directions the project tackles first, two architectural decisions stay fixed, because they're the foundation everything else depends on:

- **Communication only through bounded channels.** No unbounded queue anywhere in the pipeline — this is the guarantee that keeps the rest of the system predictable under load.
- **Stages that know nothing about each other beyond the data type they exchange.** A new stage, a new source, a new destination — all of them fit the same shape of "receive from a channel, process, send to the next one," with no cascading changes through the rest of the code.

That discipline, more than any specific feature, is what determines whether the project can keep growing without turning into something no one can maintain.

## Getting started

`logtap` is currently a library crate — there is no CLI binary yet (see [Roadmap](#roadmap)). It's driven through `logtap::run`:

```rust
use logtap::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config {
        source_path: "app.log".into(),
        sink_url: "http://localhost:8080/logs".to_string(),
        batch_size: 100,
        flush_interval_secs: 5,
        channel_capacity: 1000,
        filter_rules: vec![],
    };

    logtap::run(cfg).await
}
```

### Configuration (`logtap.toml`)

`Config` is deserializable from TOML via `serde`:

```toml
source_path = "app.log"
sink_url = "http://localhost:8080/logs"
batch_size = 100
flush_interval_secs = 5
channel_capacity = 1000

[[filter_rules]]
field = "level"
op = "equals"
value = "debug"
action = "drop"

[[filter_rules]]
field = "email"
op = "regex"
value = "^[^@]+@[^@]+\\.[^@]+$"
action = "mask"
```

| Field | Type | Description |
|---|---|---|
| `source_path` | path | File to tail. |
| `sink_url` | string | HTTP endpoint the batches are `POST`ed to as a JSON array. |
| `batch_size` | integer | Max records buffered before a flush is triggered. |
| `flush_interval_secs` | integer | Max time between flushes, even if `batch_size` isn't reached. |
| `channel_capacity` | integer | Bound applied to every internal channel — the backpressure knob. |
| `filter_rules` | array | Ordered `FilterRule` entries (`field`, `op`, `value`, `action`); optional, defaults to empty. |

`op` accepts `equals`, `contains`, or `regex`. `action` accepts `drop` or `mask`.

## Development

```bash
cargo test --all      # unit + integration tests
cargo fmt --all       # formatting
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs all three on every pull request and on pushes to `main`.