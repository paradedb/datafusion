# Empty-Partition Elision Microbenchmark: Sliced `RangePartitioning` (`#26509`) vs Stage-Wide `Range(N)` + `Exact(0)` `Statistics`

## Question being tested

In a distributed range-partitioned stage with `W` workers and `C = 16` partitions per worker (`N = W * C` stage-wide partitions; e.g. `N = 1,600` at `W = 100`), each worker holds data for only `16` partitions while the remaining `N - 16` partitions live on other workers.

Two ways to represent a single worker's local `ExecutionPlan` are under discussion:

1. _Sliced `RangePartitioning` (`apache/datafusion#26509` / `#26593`)_: add `start_bound` / `end_bound` to `RangePartitioning` so each worker trims its partitioning down to its `16` owned partitions (`0` empty partition slots locally), at the cost of splitting `Partitioning` equality, making range routing a partial function, and only representing a single contiguous slice.
2. _Stage-wide `Range(N)` + `Precision::Exact(0)` `Statistics`_: keep the full `N`-partition `RangePartitioning` on every worker (preserving `Partitioning` equality and total-function routing), and report `partition_statistics(Some(idx)).num_rows == Precision::Exact(0)` on the `N - 16` partitions owned by other workers.

The primary performance objection to option (2) is single-worker overhead: _if `N = 1,600`, does carrying `1,584` empty partition slots through a worker's `ExecutionPlan` make planning or execution slower than a `16`-partition sliced plan?_

This benchmark measures that overhead directly across `W = 1, 10, 50, 100` workers (`N = 16, 160, 800, 1600`, always with `16` active partitions of `4,096` rows per worker and `N - 16` empty partitions), comparing three configurations:

* `#26509` sliced baseline (`N = 16`, `0` empty partitions): the worker's plan only has `16` partition slots.
* Stage-wide `Range(N)` without elision (`no-elide`, current `main`): the worker's plan has `N` partition slots, and fan-in/exchange operators blindly initialize all `0..N` inputs.
* Stage-wide `Range(N)` with `Exact(0)` input elision (`elide`): the worker's plan has `N` partition slots, and fan-in/exchange operators (`CoalescePartitionsExec`, `RepartitionExec`) call `StatisticsContext::compute_partitions` and skip inputs with `num_rows == Precision::Exact(0)`.

## Workload and what each column measures

Each worker runs a 7-operator partitioned join tree (`16` active partitions of `4,096` rows per side placed non-contiguously at `p % (N / 16) == 0`, each with its own distinct `Arc<Statistics>` and `min`/`max` `ColumnStatistics`, plus `N - 16` empty `Exact(0)` partitions sharing a known-empty `Arc<Statistics>`):

```text
SlicedRangeSourceExec(N) -> FilterExec -> ProjectionExec \
                                                          -> HashJoinExec(Partitioned) -> [Terminal Exchange]
SlicedRangeSourceExec(N) -> FilterExec -> ProjectionExec /
```

Putting `FilterExec -> ProjectionExec -> HashJoinExec(Partitioned)` underneath the terminal exchange tests two things simultaneously:

1. Whether pull-based, stream-preserving operators pay per-partition execution overhead for unpulled empty partitions.
2. Whether bottom-up `StatisticsContext` evaluation across `0..N` stays near-zero through non-trivial operators (`FilterExec` selectivity logic and `HashJoinExec` cardinality estimation) even when active and empty partitions are interleaved.

For each `N in [16, 160, 800, 1600]`, `bench_empty_partition_sweep` measures:

* `Stats loop / batch (ms)`: standalone wall time at the `HashJoinExec` root to evaluate all `0..N` partition statistics via `N` individual `StatisticsContext::compute(..., Some(i))` calls (`loop`) vs a single `StatisticsContext::compute_partitions` walk (`batch`), verifying all `N - 16` empty partitions resolve to `Precision::Exact(0)`.
* `Coalesce N->1 (ms)`: wall time to execute and drain `CoalescePartitionsExec` (`N` join partitions -> `1` local output stream), comparing `no-elide` vs `elide`. Comparing `N = 16` (`0.70 ms`) against `N = 1600 elide` (`0.56 ms`) isolates the full cost vs `#26509` for local fan-in.
* `Repart N->16 (ms)`: wall time to execute and drain `RepartitionExec` (`Range(N)` -> `Hash(16)`), modeling repartitioning back to the worker's `16` local cores. Comparing `N = 16` (`1.47 ms`) against `N = 1600 elide` (`1.36 ms`) isolates the full cost vs `#26509` for intra-worker repartitioning.
* `Repart N->N (ms)`: wall time to execute and drain `RepartitionExec` (`Range(N)` -> `Range(N)`), modeling an outbound shuffle into a new `N`-partition distributed stage. Without elision, `RepartitionExec` allocates `I * O = N * N` sender handles and metrics (`1600 * 1600 = 2.56M` at `N = 1600`). With `Exact(0)` input elision, `ensure_input_streams_initialized` filters `streams_and_metrics` down to the `16` non-empty inputs before `consume_input_streams` runs, executing the exact same `16 * N` channel setup (`16 * 1600 = 25.6K`) that a `#26509` `16 -> 1600` outbound shuffle performs, plus `0.10 ms` of stats evaluation. (Note: row `N = 16` runs `16 -> 16` with `16` outputs, not `16 -> 1600`.)
* `Leaf .execute() calls`: total `SlicedRangeSourceExec::execute` calls across both join inputs (`16` active partitions x `2` join sides = `32` non-empty leaf streams).

## Prototype changes in `datafusion-physical-plan`

* `datafusion/physical-plan/src/execution_plan.rs` & `datafusion/physical-plan/src/statistics.rs`:
  * Switch `StatsCache` from `std::collections::HashMap` (`SipHash`) to `datafusion_common::HashMap` (`ahash`).
  * Add `PartitionStatsMode { PerPartitionIndex, ElementwiseFromChildren }` (defaulting to `PerPartitionIndex` on `ExecutionPlan::partition_stats_mode`, overridden to `ElementwiseFromChildren` on `FilterExec`, `ProjectionExec`, `CoalesceBatchesExec`, `AggregateExec`, `SortExec` when preserving partitioning, and `HashJoinExec` in `PartitionMode::Partitioned`).
  * Add `StatisticsContext::compute_partitions` / `compute_partitions_arc`, walking the plan tree once (`O(nodes)` instead of `O(nodes * N)`), caching a single `Arc<[Arc<Statistics>]>` per node, and memoizing elementwise evaluations by child `Arc<Statistics>` pointer identity so `1,584` non-contiguous empty partitions sharing an empty `Arc<Statistics>` evaluate once per operator (`17` evaluations instead of `1,600`).
* `datafusion/physical-plan/src/filter.rs`: short-circuit `FilterExec::statistics_helper` when `input_stats.num_rows == Precision::Exact(0)`.
* `datafusion/physical-plan/src/joins/utils.rs`: preserve `Precision::Exact(0)` in `estimate_join_statistics` when join inputs are known-empty for the `JoinType` (current `main` degrades `0` to `Precision::Inexact(0)`).
* `datafusion/physical-plan/src/coalesce_partitions.rs` & `datafusion/physical-plan/src/repartition/mod.rs`: skip `Precision::Exact(0)` input partitions via `StatisticsContext::compute_partitions` (toggled via `ELIDE_EMPTY_INPUT_PARTITIONS` so `no-elide` and `elide` run back-to-back in `bench_empty_partition_sweep`).

## Results

Measured on Apple Silicon (`release-nonlto`, `8` Tokio worker threads, median of `5` runs after warmup; `3` runs for `1600 -> 1600`):

| `N` (stage-wide) | Workers (`N/16`) | Empty / worker | `Stats` loop / batch (ms) | `Coalesce` `N->1` no-elide / elide (ms) | `Repart` `N->16` no-elide / elide (ms) | `Repart` `N->N` no-elide / elide (ms) | Leaf `.execute()` calls (no-elide -> elide) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `16` (`#26509` size) | `1` | `0` | `0.18` / `0.163` | `0.70` / `0.76` | `1.47` / `1.41` | `1.00` / `1.02` | `32 -> 32` |
| `160` | `10` | `144` | `0.27` / `0.078` | `1.19` / `0.46` | `2.68` / `1.20` | `9.10` / `1.88` | `320 -> 32` |
| `800` | `50` | `784` | `0.87` / `0.076` | `4.16` / `0.49` | `11.79` / `1.17` | `204.92` / `5.66` | `1600 -> 32` |
| `1600` | `100` | `1584` | `1.73` / `0.101` | `8.62` / `0.56` | `26.74` / `1.36` | `833.15` / `10.56` | `3200 -> 32` |

## Conclusions

1. _Zero empty stream execution (including non-contiguous slices)_: once `CoalescePartitionsExec` and `RepartitionExec` skip `Exact(0)` inputs, pull-based interior operators (`FilterExec`, `ProjectionExec`, `HashJoinExec(Partitioned)`) never pull or instantiate any of the `1,584` empty partitions (`32` leaf `.execute()` calls at `N = 1,600`, identical to `N = 16`).
2. _`0.10 ms` stats evaluation and indistinguishable end-to-end runtime vs `#26509`_: `StatisticsContext::compute_partitions` evaluates all `1,600` partitions of the 7-operator join tree in `0.101 ms` (`0.078 ms` at `N = 160` -> `0.101 ms` at `N = 1,600`), keeping local `Coalesce` (`0.56 ms` vs `0.70 ms` at `N = 16`) and `Repart Hash(16)` (`1.36 ms` vs `1.47 ms` at `N = 16`) within measurement noise of a `16`-partition sliced plan.
3. _Eliminates quadratic `I * O` shuffle setup_: for an outbound `1,600`-partition shuffle (`Repart 1600 -> 1600`), eliding `Exact(0)` inputs reduces `RepartitionExec` channel initialization from `1600 * 1600` (`833.15 ms`) to the `16 * 1600` active pairs (`10.56 ms`, `79x` faster), matching the channel topology of a `#26509` `16 -> 1600` shuffle.

## Reproduce

```bash
CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_NONLTO_STRIP=symbols \
  cargo test --profile release-nonlto -p datafusion-physical-plan --lib \
  bench_empty_partition_sweep -- --nocapture --test-threads=1
```
