# Design Proposal: SortMergeJoin Dynamic Filter Pushdown

## Objective
Enable `SortMergeJoinExec` to publish dynamic min/max filters to both the left and right input streams. This allows downstream data sources (like `ParquetExec`) to skip reading RowGroups or files that will never match the remaining join keys.

## Background
Currently, DataFusion supports dynamic filter pushdown for `HashJoinExec`. It works by fully materializing the build side, collecting the absolute min/max (and a membership filter like `InList`), and pushing it to the probe side. 
`SortMergeJoinExec` processes two *sorted* streams concurrently. This sorted property provides a unique opportunity: we don't need to wait for the entire stream to finish. As we read batches from either side, the "head" of the stream continuously forms a progressively tighter bound for the remaining data.

## Proposed Architecture

### 1. Scope & Eligibility
* Enable for `JoinType::Inner`, `JoinType::LeftSemi`, and `JoinType::RightSemi` when the session configuration `datafusion.optimizer.enable_join_dynamic_filter_pushdown` is `true`.
* Semi-joins share the same fundamental requirement as Inner joins: a row must have a match on the opposite side to be produced, making them safe for progressive pruning.
* Only the first join key is used to generate the bounds, adhering to the sort options of that key.

### 2. Physical Plan Modification
* **`SortMergeJoinExec` Fields:** Add two optional fields to store the dynamic filters:
  ```rust
  left_dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
  right_dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
  ```
* **Filter Pushdown Trait:** Update the `gather_filters_for_pushdown` and `handle_child_pushdown_result` implementations in `SortMergeJoinExec`. In `FilterPushdownPhase::Post`, generate the dynamic filters for the `left` and `right` sides and wrap the respective children using `.with_self_filter()`. 

### 3. Shared Cross-Partition Accumulator
Because DataFusion executes partitions concurrently (usually hash-partitioned), a single partition's progress does not provide global information. For an ascending join key, if Partition A has reached key 1000 but Partition B is still at key 10, a global scan cannot skip keys < 1000. 

The `SharedSortMergeBoundsAccumulator` implements **Consensus of the Slowest Progress**:
* **State tracking:** Maintains a `Mutex<Vec<Option<ScalarValue>>>` to track the current `head` value of each partition.
* **Initialization check:** A global bound is only safe to publish once *every* active partition has produced at least one row.
* **Global Bound Calculation:**
  * For an **Ascending** join key, the global bound is the **minimum** of all active partition heads.
  * For a **Descending** join key, the global bound is the **maximum** of all active partition heads.
* **Advantage over HashJoin:** While HashJoin must wait for the slowest partition to *finish* (100% completion), SortMergeJoin only waits for the slowest partition to *advance*. This allows for incremental, "rolling" pruning throughout the entire execution.

### 4. Progressive Bound Updates in `SortMergeJoinStream`
As `SortMergeJoinStream` processes batches from its `streamed` (left) and `buffered` (right) inputs:
1. **Initial Publish:** Upon reading the very first batch of a stream, the stream reports the first join key value to the accumulator. Once all partitions report in, the accumulator publishes the initial bounds (e.g., `R.key >= global_min_L`).
2. **Progressive Tightening:** As the stream advances, the local `head` value increases (for ascending). The stream frequently reports its new `head` to the accumulator. When the global bound increases, the accumulator calls `dynamic_filter.update()` with the tighter bound. Downstream Parquet readers checking the filter will automatically use the tighter bounds for subsequent RowGroups.
3. **Partition Exhaustion:** When a partition completes, it marks itself as exhausted in the accumulator. The global bound is then calculated only from the remaining active partitions, allowing it to advance.
4. **Complete Exhaustion:** If all partitions on one side are completely exhausted, the accumulator updates the opposite side's dynamic filter to `lit(false)`, terminating the scan entirely.

## Summary of Bound Logic (Ascending Sort)
* **Filter on Right Child:** `Right.key >= min({head(Left_p) | p in active_partitions})`
* **Filter on Left Child:** `Left.key >= min({head(Right_p) | p in active_partitions})`
