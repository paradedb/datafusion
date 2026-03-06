// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared bounds for Sort-Merge Join dynamic filter pushdown.

use arrow::compute::SortOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr, lit};
use parking_lot::Mutex;
use std::sync::Arc;

/// Coordinates dynamic filter updates for Sort-Merge Join across multiple partitions.
///
/// This accumulator implements "Consensus of the Slowest Progress". A global bound
/// is only published once all active partitions have reported at least one row,
/// and it advances based on the minimum (for ascending) or maximum (for descending)
/// of all current partition heads.
#[derive(Debug)]
pub(crate) struct SharedSortMergeBoundsAccumulator {
    /// Shared state for all partitions.
    state: Mutex<AccumulatorState>,
    /// Dynamic filter to update.
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Sort options of the join key.
    sort_options: SortOptions,
    /// Join key expression on the side being filtered.
    on_expr: PhysicalExprRef,
}

#[derive(Debug)]
struct AccumulatorState {
    /// Current head values for each partition.
    /// Index corresponds to the partition ID.
    heads: Vec<Option<ScalarValue>>,
    /// Whether each partition is exhausted.
    exhausted: Vec<bool>,
}

impl SharedSortMergeBoundsAccumulator {
    pub fn new(
        num_partitions: usize,
        sort_options: SortOptions,
        on_expr: PhysicalExprRef,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> Self {
        Self {
            state: Mutex::new(AccumulatorState {
                heads: vec![None; num_partitions],
                exhausted: vec![false; num_partitions],
            }),
            dynamic_filter,
            sort_options,
            on_expr,
        }
    }

    /// Report current head value from a partition.
    pub fn report_head(&self, partition_id: usize, head: ScalarValue) -> Result<()> {
        let mut state = self.state.lock();
        state.heads[partition_id] = Some(head.clone());

        self.update_filter(&state)
    }

    /// Mark a partition as exhausted.
    pub fn mark_exhausted(&self, partition_id: usize) -> Result<()> {
        let mut state = self.state.lock();
        state.exhausted[partition_id] = true;

        self.update_filter(&state)?;

        // If all partitions are exhausted, we can mark the filter as complete.
        if state.exhausted.iter().all(|&e| e) {
            self.dynamic_filter.mark_complete();
        }

        Ok(())
    }

    /// Update the dynamic filter based on current heads.
    fn update_filter(&self, state: &AccumulatorState) -> Result<()> {
        // We only publish a bound if all non-exhausted partitions have reported a head.
        let mut active_heads = Vec::new();
        let mut any_active = false;

        for (i, head) in state.heads.iter().enumerate() {
            if !state.exhausted[i] {
                any_active = true;
                if let Some(h) = head {
                    active_heads.push(h);
                } else {
                    // At least one active partition hasn't reported yet.
                    return Ok(());
                }
            }
        }

        if !any_active {
            // All partitions exhausted: sudden death.
            // No more matches are possible, so we can prune everything.
            return self.dynamic_filter.update(lit(false));
        }

        if active_heads.is_empty() {
            // This case is reachable if all partitions were marked exhausted
            // without ever reporting a head (e.g. empty inputs).
            return self.dynamic_filter.update(lit(false));
        }

        // Calculate the consensus bound.
        // For Ascending, it's the minimum of all active heads.
        // For Descending, it's the maximum of all active heads.
        let mut consensus = active_heads[0].clone();
        for head in &active_heads[1..] {
            if self.sort_options.descending {
                if *head > &consensus {
                    consensus = (*head).clone();
                }
            } else if *head < &consensus {
                consensus = (*head).clone();
            }
        }

        // Create the filter expression.
        // For Ascending: col >= consensus
        // For Descending: col <= consensus
        let op = if self.sort_options.descending {
            Operator::LtEq
        } else {
            Operator::GtEq
        };

        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::clone(&self.on_expr),
            op,
            lit(consensus.clone()),
        ));

        self.dynamic_filter.update(filter_expr)
    }
}
