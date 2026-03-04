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
    /// Current head values for each partition.
    /// Index corresponds to the partition ID.
    heads: Mutex<Vec<Option<ScalarValue>>>,
    /// Whether each partition is exhausted.
    exhausted: Mutex<Vec<bool>>,
    /// Dynamic filter to update.
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Sort options of the join key.
    sort_options: SortOptions,
    /// Join key expression on the side being filtered.
    on_expr: PhysicalExprRef,
}

impl SharedSortMergeBoundsAccumulator {
    pub fn new(
        num_partitions: usize,
        sort_options: SortOptions,
        on_expr: PhysicalExprRef,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> Self {
        Self {
            heads: Mutex::new(vec![None; num_partitions]),
            exhausted: Mutex::new(vec![false; num_partitions]),
            dynamic_filter,
            sort_options,
            on_expr,
        }
    }

    /// Report current head value from a partition.
    pub fn report_head(&self, partition_id: usize, head: ScalarValue) -> Result<()> {
        let mut heads = self.heads.lock();
        heads[partition_id] = Some(head);

        self.update_filter(&heads)
    }

    /// Mark a partition as exhausted.
    pub fn mark_exhausted(&self, partition_id: usize) -> Result<()> {
        let mut exhausted = self.exhausted.lock();
        exhausted[partition_id] = true;

        // If all partitions are exhausted, we can mark the filter as complete.
        if exhausted.iter().all(|&e| e) {
            self.dynamic_filter.mark_complete();
            return Ok(());
        }

        let heads = self.heads.lock();
        self.update_filter(&heads)
    }

    /// Update the dynamic filter based on current heads.
    fn update_filter(&self, heads: &[Option<ScalarValue>]) -> Result<()> {
        let exhausted = self.exhausted.lock();

        // We only publish a bound if all non-exhausted partitions have reported a head.
        let mut active_heads = Vec::new();
        for (i, head) in heads.iter().enumerate() {
            if !exhausted[i] {
                if let Some(h) = head {
                    active_heads.push(h);
                } else {
                    // At least one active partition hasn't reported yet.
                    return Ok(());
                }
            }
        }

        if active_heads.is_empty() {
            // All partitions exhausted, but mark_exhausted already handles this.
            return Ok(());
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
            lit(consensus),
        ));

        self.dynamic_filter.update(filter_expr)
    }
}
