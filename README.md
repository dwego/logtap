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

This is the most important axis. Most of it is already in place:

- **Retry with exponential backoff** in the sink before giving up on a batch.
- **A local dead-letter file** — batches that exhaust their retries get written to `logtap.failed.jsonl` instead of lost, one record per line, in the same shape `source` already produces so it can be replayed through `logtap` itself (see [Reprocessing the dead-letter file](#reprocessing-the-dead-letter-file)).
- **Size-capped, rotating dead-letter files** — `logtap.failed.jsonl` rotates into `.1`, `.2`, etc. once it crosses `dead_letter_max_bytes`, so a prolonged outage fills disk predictably instead of without limit. Discarding the oldest rotated file is real data loss, so it's always logged loudly, never silently.

What's still missing: **log rotation detection** — when the watched file is renamed and recreated (standard `logrotate` behavior in production), `source` needs to notice and start reading the new file, instead of continuing to hold a handle to a file that no longer exists.

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

Build the binary and point it at a config file:

```bash
cargo build --release
./target/release/logtap --config-path logtap.toml
```

`--config-path` defaults to `logtap.toml` in the current directory, so with a config file already in place, `./target/release/logtap` on its own is enough. See [`logtap.toml`](logtap.toml) in this repo for a working example.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Clean run. (Today the pipeline runs until killed — this is reserved for a future graceful shutdown.) |
| `1` | An expected, fixable error: missing config file, invalid TOML, a malformed field. |
| `101` | An unexpected internal panic — a bug, not a config problem. |

### Docker

```bash
docker build -t logtap .

docker run --rm \
  -v $(pwd)/logtap.toml:/app/logtap.toml \
  -v /path/to/your/app.log:/data/app.log \
  logtap
```

The image ships only the binary — `logtap.toml` and the log file it tails are expected to be mounted in at runtime. `source_path` inside `logtap.toml` should point at wherever the log file lands *inside* the container (`/data/app.log` above), not at its path on the host.

### Configuration (`logtap.toml`)

`Config` is deserializable from TOML via `serde`. Only `source_path` and `sink_url` are required — everything else has a default:

```toml
source_path = "app.log"
sink_url = "http://localhost:8080/logs"
batch_size = 100
flush_interval_secs = 5
channel_capacity = 1000
max_retries = 5
retry_backoff_initial_ms = 500
retry_backoff_max_secs = 30
mask_common_patterns = true
dead_letter_max_bytes = 1073741824
dead_letter_max_files = 5

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

| Field | Type | Default | Description |
|---|---|---|---|
| `source_path` | path | — (required) | File to tail. |
| `sink_url` | string | — (required) | HTTP endpoint the batches are `POST`ed to as a JSON array. |
| `batch_size` | integer | `50` | Max records buffered before a flush is triggered. |
| `flush_interval_secs` | integer | `5` | Max time between flushes, even if `batch_size` isn't reached. |
| `channel_capacity` | integer | `1000` | Bound applied to every internal channel — the backpressure knob. |
| `max_retries` | integer | `5` | Attempts per batch before giving up and writing it to the dead-letter file. |
| `retry_backoff_initial_ms` | integer | `500` | Backoff before the first retry; doubles on each subsequent attempt. |
| `retry_backoff_max_secs` | integer | `30` | Ceiling on the backoff, however many retries have piled up. |
| `mask_common_patterns` | boolean | `true` | Auto-mask emails, card numbers, and `sk-...`-style API keys, independent of `filter_rules`. |
| `dead_letter_max_bytes` | integer | `1073741824` (1 GiB) | Size cap on `logtap.failed.jsonl` before it's rotated into `.1`, `.2`, etc. |
| `dead_letter_max_files` | integer | `5` | How many rotated dead-letter files to keep before the oldest is discarded. |
| `filter_rules` | array | `[]` | Ordered `FilterRule` entries (`field`, `op`, `value`, `action`). |

`op` accepts `equals`, `contains`, or `regex`. `action` accepts `drop` or `mask`.

### Reprocessing the dead-letter file

Batches that exhaust `max_retries` are appended to `logtap.failed.jsonl` instead of being lost — one JSON record per line, in the same shape `source` already produces, so it can be replayed through `logtap` itself rather than needing a separate tool:

```bash
touch replay.log
./target/release/logtap --config-path replay-logtap.toml &   # points source_path at replay.log
cat logtap.failed.jsonl >> replay.log
```

`replay-logtap.toml` is a throwaway config pointing `source_path` at the empty `replay.log` and `sink_url` at the real destination. Since `logtap` tails from the end of the file it opens, appending the dead-letter contents into `replay.log` after it's already running makes it pick them up exactly like live traffic. Keep `batch_size` small (or `flush_interval_secs` short) in that config — otherwise the replayed lines just sit buffered until a batch actually fills up or the interval ticks. Once the file stops growing and everything's been delivered, stop the process and delete `logtap.failed.jsonl` and `replay.log`.

## Development

```bash
cargo test --all      # unit + integration tests
cargo fmt --all       # formatting
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs all three on every pull request and on pushes to `main`.

Tests are organized by what they're proving, one file per concern under `tests/` (each file is its own binary, so this costs nothing to keep separate):

- `filter_test.rs`, `parser_test.rs`, `source_test.rs`, `sink_test.rs`, `notify_test.rs` — one stage or behavior at a time.
- `integration_test.rs`, `cli_test.rs` — the full pipeline wired together, the second one through the actual compiled binary.
- `stress_test.rs` — load/failure-shaped: proving an outcome (nothing lost) under conditions designed to trigger backpressure, not just checking one function's return value.

## Roadmap

### v1 acceptance criteria

v1 is done when all of the following hold:

- [x] No batch is ever lost silently — every failed send is retried or persisted locally.
- [x] The process survives log rotation without manual intervention (standard rename-then-create logrotate behavior; `copytruncate` isn't handled yet).
- [x] No channel grows unbounded under load — guaranteed by construction (every channel has a fixed `channel_capacity`), and backed by [`tests/stress_test.rs`](tests/stress_test.rs), which proves the *outcome* (a burst of logs during a prolonged outage is never lost) rather than measuring memory directly.
- [ ] Every config field is validated with a clear error message at startup — never fails silently at runtime. *(deferred to v1.1 — Config::load already rejects a missing/malformed file with a clean error, just not individual bad field values like `batch_size = 0`)*
- [x] Sensitive data is masked by default, even with no user-configured rules.
- [ ] External visibility (metrics) into what the pipeline is doing, without reading logtap's own stderr output. *(deferred to v1.1 — stderr logging already covers retries, dead-letter writes, and rotation events)*
- [x] End-to-end integration test covering the full pipeline, plus unit tests per stage.
- [x] Installing and running the project requires no source reading — README and example config are enough.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
