# Durable queue production measurements

Resource calibration completed on November 3, 2026. All 20 cases passed their correctness checks with no retries or omitted runs. The installed WAL threshold, production FULL/NORMAL comparison and final release checks are still pending.

## Resource protocol

Each case ran once in a fresh process through the production durable API on local ext4. The matrix covered 8 and 64 MiB cache settings, mmap disabled and 256 MiB, and both durability policies for maximum-group churn. Other cases used Synchronized. Logical limits were 4096 pending items, 16 MiB pending payload, 16,384 receipts, 256 producers and a 1 GiB main database cap. Group delay was zero for these serial resource cases.

Capacity filled and drained four waves without retirement, verified receipt-cap refusal, then retired all frontiers. Churn exercised exact 2 MiB encoded batches, mixed payloads and the 16 MiB payload limit over eight cycles on one connection. Lifecycle added Close, Inspect, bounded Maintain and reopen. The small-main case exercised real SQLite page-cap refusal and recovered an empty store.

The harness validated IDs, payloads, acknowledgments, retirement metadata, complete counts, store identity and reopened epochs. It sampled resources every 20 ms and at command boundaries. Sampling can miss peaks. RSS includes the harness, JSON encoding, input payloads and correctness bookkeeping. Go heap counters exclude SQLite native allocations. Native heap attribution was unavailable. No forced GC occurred.

## Observations

| Case | Cache / mmap (MiB) | Policy | Peak WAL (bytes) | Peak RSS (bytes) | Peak Go HeapAlloc (bytes) |
| --- | ---: | --- | ---: | ---: | ---: |
| capacity | 8 / 0 | synchronized | 19,800,752 | 62,406,656 | 39,635,896 |
| maximum-group | 8 / 0 | synchronized | 19,244,552 | 64,598,016 | 41,769,992 |
| lifecycle | 8 / 0 | synchronized | 19,158,032 | 68,440,064 | 41,380,576 |
| small-main | 8 / 0 | synchronized | 45,352 | 17,055,744 | 6,728,104 |
| maximum-group | 8 / 0 | process-crash | 19,236,312 | 64,782,336 | 41,872,400 |
| capacity | 8 / 256 | synchronized | 20,291,032 | 83,292,160 | 38,922,400 |
| maximum-group | 8 / 256 | synchronized | 19,158,032 | 82,563,072 | 41,809,176 |
| lifecycle | 8 / 256 | synchronized | 19,158,032 | 86,466,560 | 41,855,488 |
| small-main | 8 / 256 | synchronized | 45,352 | 18,583,552 | 8,164,168 |
| maximum-group | 8 / 256 | process-crash | 19,248,672 | 82,857,984 | 41,660,832 |
| capacity | 64 / 0 | synchronized | 20,946,112 | 78,725,120 | 39,147,024 |
| maximum-group | 64 / 0 | synchronized | 19,302,232 | 75,751,424 | 42,657,544 |
| lifecycle | 64 / 0 | synchronized | 19,158,032 | 81,108,992 | 42,166,360 |
| small-main | 64 / 0 | synchronized | 45,352 | 16,896,000 | 6,694,120 |
| maximum-group | 64 / 0 | process-crash | 18,696,592 | 75,018,240 | 42,013,392 |
| capacity | 64 / 256 | synchronized | 19,627,712 | 99,950,592 | 38,882,352 |
| maximum-group | 64 / 256 | synchronized | 19,158,032 | 92,549,120 | 41,926,784 |
| lifecycle | 64 / 256 | synchronized | 19,158,032 | 102,961,152 | 42,113,088 |
| small-main | 64 / 256 | synchronized | 45,352 | 16,842,752 | 6,725,392 |
| maximum-group | 64 / 256 | process-crash | 19,063,272 | 93,700,096 | 41,830,376 |

The largest sampled WAL was 20,946,112 bytes, 4,168,896 bytes above the 16 MiB trigger. Its allocated size was 20,951,040 bytes. The largest positive change between boundary samples was 4,812,160 bytes. This change is not exact transaction growth because cleanup and checkpoints can intervene. Every maximum-group run observed a 2,097,152-byte encoded group.

The largest main database length and allocation were 22,446,080 bytes. Churn reused a 17,629,184-byte main file without a per-cycle size increase. In each lifecycle case, maintenance reduced the main file from 17,629,184 to 831,488 to 53,248 bytes. Free pages fell from 4286 to 190 to zero. A third bounded call confirmed no further reduction. Logical state checks passed after maintenance and reopen.

Peak process RSS was 102,961,152 bytes. Peak Go HeapAlloc was 42,657,544 bytes, HeapInuse was 43,655,168 bytes, and HeapSys was 53,837,824 bytes. Completed cycle-boundary samples showed reuse rather than monotonic retained growth over eight cycles. These observations do not establish a universal memory bound.

## Operational provision

The selected WAL refusal threshold is 64 MiB (67,108,864 bytes), about 3.20 times the largest observed WAL. It allocates no storage in advance. Installation and retesting are pending. A crossing transaction can overshoot this threshold before observation stops subsequent writes. The checkpoint trigger, refusal threshold and available disk are separate quantities.

For one queue at the tested ceilings, start with 512 MiB of process memory plus application-owned work and exceptional diagnostic/path-resolution allowance, and 1.25 GiB available local disk at the 1 GiB main cap. Monitor the actual workload and filesystem. These are starting provisions with headroom, not hard RSS or filesystem guarantees, and they provide no perpetual acknowledgment reserve. The maximum cache and mmap settings alone can expose up to 320 MiB.

## Provenance and limits

The harness built against Go 1.27.3 on Linux amd64 with cgo, github.com/mattn/go-sqlite3 v1.14.52 and bundled SQLite 3.53.4. The production source hashes stayed unchanged during all 20 cases. Git HEAD advanced from fc0115e to 5060cba because a fault-test-only commit completed during calibration.

Raw JSONL, stderr, exact commands, per-run hashes and environment are retained locally in the sqlite-durable-autopilot-20261102/round10a-run evidence directory. The files are intentionally uncommitted. The harness source is in ../cmd/durable-resource.

These results qualify only the finite software workloads described here. They do not test physical power loss, arbitrary filesystem behavior, a universal WAL bound, or a hard process-memory limit. Earlier adapter benchmarks used different completion rules and are not directly comparable to this production workload. No production throughput comparison is reported yet.
