// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Statistics computation for physical plans.
//!
//! [`StatisticsArgs`] provides external context to
//! [`ExecutionPlan::statistics_from_inputs`].

use crate::ExecutionPlan;
use crate::displayable;
use crate::operator_statistics::{
    ExtendedStatistics, StatisticsRegistry, StatisticsResult,
};
use datafusion_common::extensions::Extensions;
use datafusion_common::stats::Precision;
use datafusion_common::{
    HashMap, Result, Statistics, assert_eq_or_internal_err, assert_or_internal_err,
};
use log::debug;
use parking_lot::Mutex;
use std::ptr::from_ref;
use std::sync::Arc;

type CacheKey = (usize, Option<usize>);

fn plan_addr(plan: &dyn ExecutionPlan) -> usize {
    from_ref::<dyn ExecutionPlan>(plan).cast::<()>() as usize
}

fn cache_key(plan: &dyn ExecutionPlan, partition: Option<usize>) -> CacheKey {
    (plan_addr(plan), partition)
}

/// Per-context memoization cache for statistics computation.
///
/// Keys are `(plan node pointer address, partition)` (or `plan node pointer
/// address` for all-partition slices). Every entry retains the `Arc` whose
/// pointer supplied its key, preventing a dropped plan's address from being
/// reused by a different plan while the entry exists.
///
/// Core statistics and provider extensions are cached separately: the
/// `statistics` map is the hot path (populated on every walk); the `extensions`
/// map is populated only when a provider returns non-empty extensions, so a walk
/// with no providers never touches it.
#[derive(Debug)]
struct CacheEntry<T> {
    _plan: Arc<dyn ExecutionPlan>,
    value: T,
}

#[derive(Debug, Default)]
struct StatsCache {
    statistics: HashMap<CacheKey, CacheEntry<Arc<Statistics>>>,
    partition_statistics: HashMap<usize, CacheEntry<Arc<[Arc<Statistics>]>>>,
    extensions: HashMap<CacheKey, CacheEntry<Extensions>>,
}

/// Arguments passed to [`ExecutionPlan::statistics_from_inputs`] carrying
/// external information that operators can use when computing their
/// statistics.
#[derive(Debug, Default, Clone)]
pub struct StatisticsArgs {
    partition: Option<usize>,
}

impl StatisticsArgs {
    /// Creates new statistics arguments.
    ///
    /// By default the partition is set to `None` (statistics should be computed
    /// for the entire plan).
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the partition to compute statistics
    ///
    /// * `None` means statistics should be computed for the entire plan.
    /// * `Some(idx)` means statistics should be computed for the specified
    ///   partition index.
    pub fn set_partition(&mut self, partition: Option<usize>) {
        self.partition = partition;
    }

    /// Builder Style API for [`Self::set_partition`]
    pub fn with_partition(mut self, partition: Option<usize>) -> Self {
        self.set_partition(partition);
        self
    }

    /// Return the partition to compute statistics
    pub fn partition(&self) -> Option<usize> {
        self.partition
    }
}

/// Applies `fetch` to the `input` statistics of an operator that stops *each*
/// of its `n_partitions` output partitions after `fetch` rows, such as
/// `LocalLimitExec` or a `SortExec` with a fetch that preserves partitioning.
///
/// `input` must cover what `args` asks for: one partition's rows when `args`
/// names a partition, and the rows of every partition otherwise.
///
/// For a single partition this is [`Statistics::with_fetch`]. Overall, though,
/// every partition can emit up to `fetch` rows, so the output can reach
/// `fetch * n_partitions` rows, and never more than the input. How the input
/// rows are spread over the partitions is unknown, so the result is inexact
/// unless an exact input count shows that `fetch` cannot drop any row.
pub(crate) fn with_per_partition_fetch(
    input: Statistics,
    fetch: Option<usize>,
    n_partitions: usize,
    args: &StatisticsArgs,
) -> Result<Statistics> {
    let Some(fetch) = fetch else {
        return Ok(input);
    };
    if args.partition().is_some() || n_partitions <= 1 {
        return input.with_fetch(Some(fetch), 0, 1);
    }
    // No partition can hold more than `fetch` rows, so none is dropped. Only
    // an exact count proves that: an estimate below `fetch` may be wrong.
    if matches!(input.num_rows, Precision::Exact(num_rows) if num_rows <= fetch) {
        return Ok(input);
    }
    Ok(input
        .with_fetch(Some(fetch.saturating_mul(n_partitions)), 0, 1)?
        .to_inexact())
}

/// Directive returned by [`ExecutionPlan::child_stats_requests`] describing
/// how the [`StatisticsContext`] should obtain each child's statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildStats {
    /// Compute the child's statistics at this partition (`None` = overall).
    At(Option<usize>),
    /// Skip this child; the parent does not need its statistics. A placeholder
    /// [`Statistics::new_unknown`] is supplied in its slot.
    Skip,
}

/// Declares how [`StatisticsContext::compute_partitions`] may evaluate a plan
/// node's per-partition statistics across `0..partition_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatsMode {
    /// Resolve each partition via [`ExecutionPlan::child_stats_requests`] and
    /// invoke [`ExecutionPlan::statistics_from_inputs`] per partition index.
    PerPartitionIndex,
    /// Output partition `i` reads partition `i` of every child and its
    /// [`ExecutionPlan::statistics_from_inputs`] depends only on `input_stats`
    /// and `args.partition().is_some()` (never the numeric value of `i`),
    /// allowing batch child resolution and pointer-based deduplication across
    /// partitions sharing identical child `Arc<Statistics>`.
    ElementwiseFromChildren,
}

/// Owns the bottom-up traversal and memoization cache for statistics
/// computation. Call [`StatisticsContext::compute`] to walk a plan tree.
///
/// The cache lives as long as the context and may be shared across walks and
/// plan rewrites. Each entry holds a strong reference to the plan node it was
/// computed for, so cached nodes (and their per-partition statistics) stay
/// alive until [`Self::reset_cache`] is called or the context is dropped. Reset
/// a long-lived context at a lifecycle boundary to bound its memory (see
/// [`Self::reset_cache`]).
///
/// Cached statistics are served for as long as the context lives, so a plan
/// node must not change its statistics in place (for example through interior
/// mutability) while a context holds it: the cache would return stale values.
/// Optimizer rules satisfy this because they replace nodes instead of changing
/// them.
///
/// An optional [`StatisticsRegistry`] plugs providers into the walk: at each node
/// they are consulted before the operator's built-in
/// [`ExecutionPlan::statistics_from_inputs`]. An empty registry is the built-in
/// computation.
///
/// The walk carries [`ExtendedStatistics`]. A node has extensions only if a
/// provider `Computed` them for it; a node that falls back to the built-in
/// [`ExecutionPlan::statistics_from_inputs`] has none. So extensions propagate
/// upward only through an unbroken chain of provider-handled nodes: a single
/// built-in node yields no extensions and hides those of everything beneath it.
/// [`Self::compute_extended`] observes extensions; [`Self::compute`] returns core
/// [`Statistics`] only.
pub struct StatisticsContext {
    cache: Mutex<StatsCache>,
    registry: StatisticsRegistry,
}

impl Default for StatisticsContext {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsContext {
    /// Creates a context with an empty cache and no statistics providers.
    pub fn new() -> Self {
        Self::new_with_registry(StatisticsRegistry::new())
    }

    /// Creates a context whose walk consults `registry`'s provider chain.
    pub fn new_with_registry(registry: StatisticsRegistry) -> Self {
        Self {
            cache: Mutex::new(StatsCache::default()),
            registry,
        }
    }

    /// Clears the memoization cache and releases its retained plan nodes.
    ///
    /// Resetting is optional for correctness: each cache entry retains the plan
    /// node that supplied its pointer key. Use it to bound memory at a
    /// lifecycle boundary, such as the end of a query's physical optimization.
    /// Do not reset a context that several optimizer rules share between those
    /// rules: that discards the statistics that later rules would reuse.
    pub fn reset_cache(&self) {
        let mut cache = self.cache.lock();
        cache.statistics.clear();
        cache.partition_statistics.clear();
        cache.extensions.clear();
    }

    /// Computes the core [`Statistics`] for `plan`, discarding any
    /// provider-supplied extensions (see [`Self::compute_extended`]).
    ///
    /// The root's own statistics are not memoized, because a borrowed plan
    /// cannot be retained by the cache; its descendants are. Use
    /// [`Self::compute_arc`] to also memoize the root.
    ///
    /// A borrowed root is still looked up in the cache by address, so it must
    /// be a standalone plan node (normally the pointee of an
    /// `Arc<dyn ExecutionPlan>`), not a plan stored inline within another
    /// cached node.
    ///
    /// With no providers registered this is the plain built-in walk: only the
    /// `statistics` cache is touched, so it carries no extension overhead.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion_common::Statistics;
    /// # use datafusion_common::stats::Precision;
    /// # use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    /// # use datafusion_physical_plan::test::exec::StatisticsExec;
    ///
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let overall_stats =
    ///     Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(100));
    /// let partition_stats = vec![
    ///     Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(60)),
    ///     Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(40)),
    /// ];
    /// let plan = StatisticsExec::new(overall_stats, schema)
    ///     .with_partition_statistics(partition_stats);
    ///
    /// let context = StatisticsContext::new();
    ///
    /// // Statistics for the whole plan (all partitions combined).
    /// let overall = context.compute(&plan, &StatisticsArgs::new())?;
    /// assert_eq!(overall.num_rows, Precision::Exact(100));
    ///
    /// // Statistics for a single partition.
    /// let args = StatisticsArgs::new().with_partition(Some(0));
    /// let per_partition = context.compute(&plan, &args)?;
    /// assert_eq!(per_partition.num_rows, Precision::Exact(60));
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn compute(
        &self,
        plan: &dyn ExecutionPlan,
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.compute_base(plan, None, args, false)
            .map(|(statistics, _)| statistics)
    }

    /// Like [`Self::compute`], but the cache retains `plan` so the root's own
    /// statistics are memoized as well as its descendants'.
    ///
    /// Prefer this when sharing one context across repeated calls on the same
    /// nodes, such as an optimizer pass.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion_common::Statistics;
    /// # use datafusion_common::stats::Precision;
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// # use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    /// # use datafusion_physical_plan::test::exec::StatisticsExec;
    ///
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let stats = Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(100));
    /// let plan: Arc<dyn ExecutionPlan> = Arc::new(StatisticsExec::new(stats, schema));
    ///
    /// let context = StatisticsContext::new();
    /// let first = context.compute_arc(&plan, &StatisticsArgs::new())?;
    /// let second = context.compute_arc(&plan, &StatisticsArgs::new())?;
    ///
    /// // The second call is a cache hit for the root.
    /// assert!(Arc::ptr_eq(&first, &second));
    /// assert_eq!(first.num_rows, Precision::Exact(100));
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn compute_arc(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.compute_base(plan.as_ref(), Some(plan), args, false)
            .map(|(statistics, _)| statistics)
    }

    /// Computes core [`Statistics`] for all `0..partition_count` output
    /// partitions of `plan` in a single bottom-up walk.
    ///
    /// Operators returning [`PartitionStatsMode::ElementwiseFromChildren`] from
    /// [`ExecutionPlan::partition_stats_mode`] resolve their children once and
    /// reuse outputs across partitions whose child `Arc<Statistics>` pointers
    /// match (for example when many partitions share a known-empty `Exact(0)`
    /// [`Statistics`] allocation).
    ///
    /// As with [`Self::compute`], the borrowed root is not memoized; use
    /// [`Self::compute_partitions_arc`] to also memoize the root slice.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion_common::Statistics;
    /// # use datafusion_common::stats::Precision;
    /// # use datafusion_physical_plan::statistics::StatisticsContext;
    /// # use datafusion_physical_plan::test::exec::StatisticsExec;
    ///
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let overall = Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(60));
    /// let per_part = vec![
    ///     Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(60)),
    ///     Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(0)),
    /// ];
    /// let plan = StatisticsExec::new(overall, schema).with_partition_statistics(per_part);
    ///
    /// let parts = StatisticsContext::new().compute_partitions(&plan)?;
    /// assert_eq!(parts.len(), 2);
    /// assert_eq!(parts[0].num_rows, Precision::Exact(60));
    /// assert_eq!(parts[1].num_rows, Precision::Exact(0));
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn compute_partitions(
        &self,
        plan: &dyn ExecutionPlan,
    ) -> Result<Arc<[Arc<Statistics>]>> {
        self.compute_partitions_base(plan, None)
    }

    /// Like [`Self::compute_partitions`], retaining `plan` so the root's
    /// per-partition slice is memoized as well as its descendants'.
    pub fn compute_partitions_arc(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<[Arc<Statistics>]>> {
        self.compute_partitions_base(plan.as_ref(), Some(plan))
    }

    /// Computes the [`ExtendedStatistics`] for `plan`: the core statistics plus
    /// any extensions a provider attached to this node (see the type-level docs
    /// for how extensions propagate up the tree). As with [`Self::compute`],
    /// the root is not memoized and must be a standalone plan node; see
    /// [`Self::compute_extended_arc`].
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use datafusion_common::{ColumnStatistics, Result, Statistics};
    /// # use datafusion_common::stats::Precision;
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// # use datafusion_physical_plan::operator_statistics::{
    /// #     ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
    /// # };
    /// # use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    /// # use datafusion_physical_plan::test::exec::StatisticsExec;
    ///
    /// // Pearson correlation between two columns, identified by schema index.
    /// #[derive(Debug, Clone)]
    /// struct ColumnCorrelation {
    ///     columns: (usize, usize),
    ///     coefficient: f64,
    /// }
    ///
    /// #[derive(Debug)]
    /// struct CorrelationProvider;
    ///
    /// impl StatisticsProvider for CorrelationProvider {
    ///     fn matches(&self, plan: &dyn ExecutionPlan) -> bool {
    ///         plan.downcast_ref::<StatisticsExec>().is_some()
    ///     }
    ///
    ///     fn compute_statistics(
    ///         &self,
    ///         plan: &dyn ExecutionPlan,
    ///         _child_stats: &[ExtendedStatistics],
    ///     ) -> Result<StatisticsResult> {
    ///         let base = plan.statistics_from_inputs(&[], &StatisticsArgs::new())?;
    ///         let mut extended = ExtendedStatistics::new_arc(base);
    ///         extended.set_extension(ColumnCorrelation {
    ///             columns: (0, 1),
    ///             coefficient: 0.92,
    ///         });
    ///         Ok(StatisticsResult::Computed(extended))
    ///     }
    /// }
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("city", DataType::Utf8, false),
    ///     Field::new("zip_code", DataType::Utf8, false),
    /// ]);
    /// let stats = Statistics {
    ///     num_rows: Precision::Exact(1000),
    ///     total_byte_size: Precision::Absent,
    ///     column_statistics: vec![ColumnStatistics::new_unknown(); 2],
    /// };
    /// let plan = StatisticsExec::new(stats, schema);
    ///
    /// let mut registry = StatisticsRegistry::new();
    /// registry.register(Arc::new(CorrelationProvider));
    /// let context = StatisticsContext::new_with_registry(registry);
    ///
    /// let extended = context.compute_extended(&plan, &StatisticsArgs::new())?;
    /// assert_eq!(extended.base().num_rows, Precision::Exact(1000));
    ///
    /// let correlation = extended.get_extension::<ColumnCorrelation>().unwrap();
    /// assert_eq!(correlation.columns, (0, 1));
    /// assert_eq!(correlation.coefficient, 0.92);
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn compute_extended(
        &self,
        plan: &dyn ExecutionPlan,
        args: &StatisticsArgs,
    ) -> Result<Arc<ExtendedStatistics>> {
        self.compute_extended_base(plan, None, args)
    }

    /// Like [`Self::compute_extended`], but the cache retains `plan` so the
    /// root's own statistics and extensions are memoized as well.
    pub fn compute_extended_arc(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        args: &StatisticsArgs,
    ) -> Result<Arc<ExtendedStatistics>> {
        self.compute_extended_base(plan.as_ref(), Some(plan), args)
    }

    fn compute_extended_base(
        &self,
        plan: &dyn ExecutionPlan,
        retained_plan: Option<&Arc<dyn ExecutionPlan>>,
        args: &StatisticsArgs,
    ) -> Result<Arc<ExtendedStatistics>> {
        let (statistics, extensions) =
            self.compute_base(plan, retained_plan, args, true)?;
        Ok(Arc::new(ExtendedStatistics::new_with_extensions(
            statistics,
            extensions.unwrap_or_default(),
        )))
    }

    /// Bottom-up walk producing the node's core statistics, resolving children
    /// first and consulting the provider chain before the operator's built-in
    /// [`ExecutionPlan::statistics_from_inputs`].
    ///
    /// Also returns the extensions a provider attached to this node, so callers
    /// never need to read the node's own extensions back out of the cache.
    /// Extensions are still recorded in the cache so parents can consume them.
    /// A freshly computed provider result always returns its extensions (they
    /// are moved, not cloned); on a cache hit they are cloned out of the cache
    /// only when `read_cached_extensions` is set, so child lookups that discard
    /// them pay nothing.
    ///
    /// When `args.partition()` is `Some(idx)`, `idx` is validated against the
    /// plan's partition count.
    fn compute_base(
        &self,
        plan: &dyn ExecutionPlan,
        retained_plan: Option<&Arc<dyn ExecutionPlan>>,
        args: &StatisticsArgs,
        read_cached_extensions: bool,
    ) -> Result<(Arc<Statistics>, Option<Extensions>)> {
        let partition = args.partition();

        if let Some(idx) = partition {
            let partition_count = plan.properties().partitioning.partition_count();
            assert_or_internal_err!(
                idx < partition_count,
                "Invalid partition index: {}, the partition count is {}",
                idx,
                partition_count
            );
        }

        if let Some(cached) = self.cached_statistics(plan, partition) {
            // Only providers store extensions, so with an empty registry the
            // extension cache is never touched.
            let extensions =
                if read_cached_extensions && !self.registry.providers().is_empty() {
                    self.cached_extensions(plan, partition)
                } else {
                    None
                };
            return Ok((cached, extensions));
        }

        let children = plan.children();
        // Try providers before resolving the operator's own children, so a
        // provider that overrides this node is not blocked by the fallback walk.
        let (statistics, extensions) =
            match self.try_provider_stats(plan, retained_plan, &children, args)? {
                Some(computed) => computed,
                None => {
                    let requests = plan.child_stats_requests(partition);
                    self.validate_child_requests(plan, &children, &requests)?;
                    let child_statistics =
                        self.resolve_children(plan, &children, &requests)?;
                    (plan.statistics_from_inputs(&child_statistics, args)?, None)
                }
            };
        if let Some(owner) = retained_plan {
            self.store_statistics(owner, partition, Arc::clone(&statistics));
        }
        Ok((statistics, extensions))
    }

    /// Validates child stat `requests` against `plan`'s children: the count must
    /// match, and each `At(Some(idx))` must be a valid partition of that child.
    fn validate_child_requests(
        &self,
        plan: &dyn ExecutionPlan,
        children: &[&Arc<dyn ExecutionPlan>],
        requests: &[ChildStats],
    ) -> Result<()> {
        assert_eq_or_internal_err!(
            requests.len(),
            children.len(),
            "{} child_stats_requests returned {} entries for {} children",
            plan.name(),
            requests.len(),
            children.len()
        );
        for (child, directive) in children.iter().zip(requests) {
            if let ChildStats::At(Some(idx)) = directive {
                let count = child.properties().partitioning.partition_count();
                assert_or_internal_err!(
                    *idx < count,
                    "{} requested invalid partition {idx} for child {} with {count} partitions",
                    plan.name(),
                    child.name()
                );
            }
        }
        Ok(())
    }

    /// Resolves each child's core statistics per `requests`: computes the child
    /// at the requested partition (memoized), or supplies a
    /// [`Statistics::new_unknown`] placeholder for [`ChildStats::Skip`]. Callers
    /// must validate `requests` via [`Self::validate_child_requests`] first.
    fn resolve_children(
        &self,
        plan: &dyn ExecutionPlan,
        children: &[&Arc<dyn ExecutionPlan>],
        requests: &[ChildStats],
    ) -> Result<Vec<Arc<Statistics>>> {
        children
            .iter()
            .zip(requests)
            .enumerate()
            .map(|(i, (child, directive))| match directive {
                ChildStats::At(p) => self
                    .compute_base(
                        child.as_ref(),
                        Some(child),
                        &StatisticsArgs::new().with_partition(*p),
                        false,
                    )
                    .map(|(statistics, _)| statistics)
                    .map_err(|e| {
                        e.context(format!(
                            "computing statistics for child {i} ({}) of {} at partition {p:?}",
                            child.name(),
                            plan.name()
                        ))
                    }),
                ChildStats::Skip => {
                    Ok(Arc::new(Statistics::new_unknown(child.schema().as_ref())))
                }
            })
            .collect()
    }

    /// Runs the provider chain, returning the first `Computed` result's core
    /// statistics and non-empty extensions (also recording the extensions in
    /// the cache), or `None` if the chain is empty or all delegate. A
    /// partition-blind provider applies only to overall stats (its default
    /// `compute_statistics_with_args` delegates per partition).
    ///
    /// Each provider's child statistics come from its own
    /// [`child_stats_requests`](crate::operator_statistics::StatisticsProvider::child_stats_requests)
    /// and are memoized, so a walk with no providers pays nothing.
    fn try_provider_stats(
        &self,
        plan: &dyn ExecutionPlan,
        retained_plan: Option<&Arc<dyn ExecutionPlan>>,
        children: &[&Arc<dyn ExecutionPlan>],
        args: &StatisticsArgs,
    ) -> Result<Option<(Arc<Statistics>, Option<Extensions>)>> {
        let providers = self.registry.providers();
        if providers.is_empty() {
            return Ok(None);
        }
        let partition = args.partition();
        for provider in providers {
            if !provider.matches(plan) {
                continue;
            }
            let requests = provider.child_stats_requests(plan, partition);
            self.validate_child_requests(plan, children, &requests)?;
            // A provider's child walk is speculative: on failure, skip the provider
            // so a later one or the operator fallback can handle the node. Not
            // error-swallowing, whoever genuinely needs the child resolves it again
            // and the error resurfaces there; a matched provider's own `compute`
            // error below stays fatal.
            let child_statistics = match self.resolve_children(plan, children, &requests)
            {
                Ok(child_statistics) => child_statistics,
                Err(e) => {
                    debug!(
                        "Statistics provider {provider:?} skipped for {}: child statistics resolution failed: {e}",
                        displayable(plan).one_line().to_string().trim_end()
                    );
                    continue;
                }
            };
            let child_extended =
                self.child_extended_stats(children, &requests, &child_statistics);
            if let StatisticsResult::Computed(computed) =
                provider.compute_statistics_with_args(plan, &child_extended, args)?
            {
                let (statistics, extensions) = computed.into_parts();
                let extensions = if extensions.is_empty() {
                    None
                } else {
                    if let Some(owner) = retained_plan {
                        self.store_extensions(owner, partition, extensions.clone());
                    }
                    Some(extensions)
                };
                return Ok(Some((statistics, extensions)));
            }
        }
        Ok(None)
    }

    /// Pairs each child's core statistics with any extensions cached for it,
    /// producing the [`ExtendedStatistics`] the provider chain consumes. Called
    /// only when providers exist, so an empty registry pays no extension cost.
    fn child_extended_stats(
        &self,
        children: &[&Arc<dyn ExecutionPlan>],
        requests: &[ChildStats],
        child_statistics: &[Arc<Statistics>],
    ) -> Vec<ExtendedStatistics> {
        children
            .iter()
            .zip(requests)
            .zip(child_statistics)
            .map(|((child, directive), statistics)| {
                let extensions = match directive {
                    ChildStats::At(p) => self.cached_extensions(child.as_ref(), *p),
                    ChildStats::Skip => None,
                };
                match extensions {
                    Some(extensions) => ExtendedStatistics::new_with_extensions(
                        Arc::clone(statistics),
                        extensions,
                    ),
                    None => ExtendedStatistics::new_arc(Arc::clone(statistics)),
                }
            })
            .collect()
    }

    fn compute_partitions_base(
        &self,
        plan: &dyn ExecutionPlan,
        retained_plan: Option<&Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<[Arc<Statistics>]>> {
        if let Some(cached) = self.cached_partition_statistics(plan) {
            return Ok(cached);
        }

        let n = plan.properties().partitioning.partition_count();
        if n == 0 {
            let empty: Arc<[Arc<Statistics>]> = Arc::from([]);
            if let Some(owner) = retained_plan {
                self.store_partition_statistics(owner, Arc::clone(&empty));
            }
            return Ok(empty);
        }

        let children = plan.children();
        let out = if !self.registry.providers().is_empty() {
            let mut out = Vec::with_capacity(n);
            let mut args = StatisticsArgs::new();
            for i in 0..n {
                args.set_partition(Some(i));
                let (s, _) = self.compute_base(plan, None, &args, false)?;
                out.push(s);
            }
            out
        } else if children.is_empty() {
            let mut out = Vec::with_capacity(n);
            let mut args = StatisticsArgs::new();
            for i in 0..n {
                args.set_partition(Some(i));
                out.push(plan.statistics_from_inputs(&[], &args)?);
            }
            out
        } else if plan.partition_stats_mode()
            == PartitionStatsMode::ElementwiseFromChildren
        {
            let child_parts: Vec<Arc<[Arc<Statistics>]>> = children
                .iter()
                .enumerate()
                .map(|(i, child)| {
                    self.compute_partitions_base(child.as_ref(), Some(child))
                        .map_err(|e| {
                            e.context(format!(
                                "computing partition statistics for child {i} ({}) of {}",
                                child.name(),
                                plan.name()
                            ))
                        })
                })
                .collect::<Result<_>>()?;
            for (child, parts) in children.iter().zip(&child_parts) {
                assert_eq_or_internal_err!(
                    parts.len(),
                    n,
                    "{} declared ElementwiseFromChildren ({n} partitions) for child {} with {} partitions",
                    plan.name(),
                    child.name(),
                    parts.len()
                );
            }
            Self::compute_elementwise_partitions(plan, &child_parts, n)?
        } else {
            let mut out = Vec::with_capacity(n);
            let mut args = StatisticsArgs::new();
            for i in 0..n {
                args.set_partition(Some(i));
                let (s, _) = self.compute_base(plan, None, &args, false)?;
                out.push(s);
            }
            out
        };

        let result: Arc<[Arc<Statistics>]> = out.into();
        if let Some(owner) = retained_plan {
            self.store_partition_statistics(owner, Arc::clone(&result));
        }
        Ok(result)
    }

    fn compute_elementwise_partitions(
        plan: &dyn ExecutionPlan,
        child_parts: &[Arc<[Arc<Statistics>]>],
        n: usize,
    ) -> Result<Vec<Arc<Statistics>>> {
        let args = StatisticsArgs::new().with_partition(Some(0));
        let mut out = Vec::with_capacity(n);

        // `child_parts` keeps every child `Arc<Statistics>` alive across this
        // call, so `Arc::as_ptr` addresses uniquely identify distinct `Arc`s.
        match child_parts {
            [c0] => {
                let mut memo: HashMap<usize, Arc<Statistics>> = HashMap::default();
                let mut prev_ptr = 0usize;
                let mut prev_out: Option<Arc<Statistics>> = None;
                for s0 in c0.iter() {
                    let ptr = Arc::as_ptr(s0) as usize;
                    if ptr == prev_ptr
                        && let Some(prev) = &prev_out
                    {
                        out.push(Arc::clone(prev));
                        continue;
                    }
                    let computed = match memo.get(&ptr) {
                        Some(existing) => Arc::clone(existing),
                        None => {
                            let res = plan.statistics_from_inputs(
                                std::slice::from_ref(s0),
                                &args,
                            )?;
                            memo.insert(ptr, Arc::clone(&res));
                            res
                        }
                    };
                    prev_ptr = ptr;
                    prev_out = Some(Arc::clone(&computed));
                    out.push(computed);
                }
            }
            [c0, c1] => {
                let mut memo: HashMap<(usize, usize), Arc<Statistics>> =
                    HashMap::default();
                let mut prev_key = (0usize, 0usize);
                let mut prev_out: Option<Arc<Statistics>> = None;
                for (s0, s1) in c0.iter().zip(c1.iter()) {
                    let key = (Arc::as_ptr(s0) as usize, Arc::as_ptr(s1) as usize);
                    if key == prev_key
                        && let Some(prev) = &prev_out
                    {
                        out.push(Arc::clone(prev));
                        continue;
                    }
                    let computed = match memo.get(&key) {
                        Some(existing) => Arc::clone(existing),
                        None => {
                            let inputs = [Arc::clone(s0), Arc::clone(s1)];
                            let res = plan.statistics_from_inputs(&inputs, &args)?;
                            memo.insert(key, Arc::clone(&res));
                            res
                        }
                    };
                    prev_key = key;
                    prev_out = Some(Arc::clone(&computed));
                    out.push(computed);
                }
            }
            _ => {
                let mut memo: HashMap<Vec<usize>, Arc<Statistics>> = HashMap::default();
                let mut inputs = Vec::with_capacity(child_parts.len());
                for i in 0..n {
                    let key: Vec<usize> = child_parts
                        .iter()
                        .map(|c| Arc::as_ptr(&c[i]) as usize)
                        .collect();
                    if let Some(existing) = memo.get(&key) {
                        out.push(Arc::clone(existing));
                        continue;
                    }
                    inputs.clear();
                    inputs.extend(child_parts.iter().map(|c| Arc::clone(&c[i])));
                    let res = plan.statistics_from_inputs(&inputs, &args)?;
                    memo.insert(key, Arc::clone(&res));
                    out.push(res);
                }
            }
        }
        Ok(out)
    }

    fn cached_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<Arc<Statistics>> {
        let cache = self.cache.lock();
        if let Some(entry) = cache.statistics.get(&cache_key(plan, partition)) {
            return Some(Arc::clone(&entry.value));
        }
        if let Some(idx) = partition
            && let Some(entry) = cache.partition_statistics.get(&plan_addr(plan))
        {
            return entry.value.get(idx).cloned();
        }
        None
    }

    fn cached_partition_statistics(
        &self,
        plan: &dyn ExecutionPlan,
    ) -> Option<Arc<[Arc<Statistics>]>> {
        self.cache
            .lock()
            .partition_statistics
            .get(&plan_addr(plan))
            .map(|entry| Arc::clone(&entry.value))
    }

    fn store_partition_statistics(
        &self,
        owner: &Arc<dyn ExecutionPlan>,
        statistics: Arc<[Arc<Statistics>]>,
    ) {
        self.cache.lock().partition_statistics.insert(
            plan_addr(owner.as_ref()),
            CacheEntry {
                _plan: Arc::clone(owner),
                value: statistics,
            },
        );
    }

    /// Inserts `value` keyed by `owner`'s pointer, retaining `owner` so the
    /// key's address cannot be reused while the entry exists.
    fn store_cache_entry<T>(
        cache: &mut HashMap<CacheKey, CacheEntry<T>>,
        owner: &Arc<dyn ExecutionPlan>,
        partition: Option<usize>,
        value: T,
    ) {
        cache.insert(
            cache_key(owner.as_ref(), partition),
            CacheEntry {
                _plan: Arc::clone(owner),
                value,
            },
        );
    }

    fn store_statistics(
        &self,
        owner: &Arc<dyn ExecutionPlan>,
        partition: Option<usize>,
        statistics: Arc<Statistics>,
    ) {
        Self::store_cache_entry(
            &mut self.cache.lock().statistics,
            owner,
            partition,
            statistics,
        );
    }

    fn cached_extensions(
        &self,
        plan: &dyn ExecutionPlan,
        partition: Option<usize>,
    ) -> Option<Extensions> {
        self.cache
            .lock()
            .extensions
            .get(&cache_key(plan, partition))
            .map(|entry| entry.value.clone())
    }

    fn store_extensions(
        &self,
        owner: &Arc<dyn ExecutionPlan>,
        partition: Option<usize>,
        extensions: Extensions,
    ) {
        Self::store_cache_entry(
            &mut self.cache.lock().extensions,
            owner,
            partition,
            extensions,
        );
    }
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::operator_statistics::StatisticsProvider;
    use crate::test::exec::StatisticsExec;
    use crate::union::UnionExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, stats::Precision};

    /// Overall-only provider: sets a fixed row count for any node.
    #[derive(Debug)]
    struct FixedRowCountProvider(usize);
    impl StatisticsProvider for FixedRowCountProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            _child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(self.0);
            Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
        }
    }

    /// Partition-aware provider: encodes the requested partition in the row count.
    #[derive(Debug)]
    struct PartitionRowCountProvider;
    impl StatisticsProvider for PartitionRowCountProvider {
        fn compute_statistics_with_args(
            &self,
            plan: &dyn ExecutionPlan,
            _child_stats: &[ExtendedStatistics],
            args: &StatisticsArgs,
        ) -> Result<StatisticsResult> {
            let marker = 700 + args.partition().map_or(0, |p| p + 1);
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(marker);
            Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct Tag(u32);

    /// Leaf provider: sets a row count and attaches a `Tag` extension.
    #[derive(Debug)]
    struct TagLeafProvider {
        rows: usize,
        tag: u32,
    }
    impl StatisticsProvider for TagLeafProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            if !child_stats.is_empty() {
                return Ok(StatisticsResult::Delegate);
            }
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(self.rows);
            let mut extended = ExtendedStatistics::new(stats);
            extended.set_extension(Tag(self.tag));
            Ok(StatisticsResult::Computed(extended))
        }
    }

    /// Non-leaf provider: re-emits a `Tag` doubled from the first child's `Tag`,
    /// proving the child's extension reached this provider.
    #[derive(Debug)]
    struct TagDoublingProvider;
    impl StatisticsProvider for TagDoublingProvider {
        fn compute_statistics(
            &self,
            plan: &dyn ExecutionPlan,
            child_stats: &[ExtendedStatistics],
        ) -> Result<StatisticsResult> {
            let Some(Tag(v)) = child_stats.first().and_then(|c| c.get_extension::<Tag>())
            else {
                return Ok(StatisticsResult::Delegate);
            };
            let mut extended =
                ExtendedStatistics::new(Statistics::new_unknown(&plan.schema()));
            extended.set_extension(Tag(v * 2));
            Ok(StatisticsResult::Computed(extended))
        }
    }

    fn ctx_with(provider: Arc<dyn StatisticsProvider>) -> StatisticsContext {
        StatisticsContext::new_with_registry(StatisticsRegistry::with_providers(vec![
            provider,
        ]))
    }

    fn make_stats_leaf(num_rows: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let col_stats = vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }];
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: col_stats,
            },
            schema,
        ))
    }

    #[test]
    fn coalesce_returns_overall_stats_for_any_partition() {
        let leaf = make_stats_leaf(100);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf));

        let ctx = StatisticsContext::new();
        let stats = ctx
            .compute(
                plan.as_ref(),
                &StatisticsArgs::new().with_partition(Some(0)),
            )
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(100));

        let stats_none = ctx.compute(plan.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(stats_none.num_rows, Precision::Exact(100));
    }

    #[test]
    fn context_caches_within_walk() {
        let leaf = make_stats_leaf(42);
        let ctx = StatisticsContext::new();
        let args = StatisticsArgs::new();

        let s1 = ctx.compute_arc(&leaf, &args).unwrap();
        assert!(!ctx.cache.lock().statistics.is_empty());

        let s2 = ctx.compute_arc(&leaf, &args).unwrap();
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn context_cache_retains_plan_lifetime() {
        let leaf = make_stats_leaf(10);
        let weak = Arc::downgrade(&leaf);
        let ctx = StatisticsContext::new();

        let _ = ctx.compute_arc(&leaf, &StatisticsArgs::new()).unwrap();
        drop(leaf);
        assert!(weak.upgrade().is_some());

        ctx.reset_cache();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn owned_parent_and_leaf_are_released_by_reset() {
        let leaf = make_stats_leaf(10);
        let leaf_weak = Arc::downgrade(&leaf);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let parent_weak = Arc::downgrade(&parent);
        let ctx = StatisticsContext::new();

        let _ = ctx.compute_arc(&parent, &StatisticsArgs::new()).unwrap();
        drop(parent);
        drop(leaf);
        assert!(parent_weak.upgrade().is_some());
        assert!(leaf_weak.upgrade().is_some());

        ctx.reset_cache();
        assert!(parent_weak.upgrade().is_none());
        assert!(leaf_weak.upgrade().is_none());
    }

    #[test]
    fn owned_parent_and_leaf_are_released_when_context_drops() {
        let leaf = make_stats_leaf(10);
        let leaf_weak = Arc::downgrade(&leaf);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let parent_weak = Arc::downgrade(&parent);

        {
            let ctx = StatisticsContext::new();
            let _ = ctx.compute_arc(&parent, &StatisticsArgs::new()).unwrap();
            drop(parent);
            drop(leaf);
            assert!(parent_weak.upgrade().is_some());
            assert!(leaf_weak.upgrade().is_some());
        }

        assert!(parent_weak.upgrade().is_none());
        assert!(leaf_weak.upgrade().is_none());
    }

    #[test]
    fn borrowed_parent_is_not_cached_but_retains_children() {
        let leaf = make_stats_leaf(10);
        let weak = Arc::downgrade(&leaf);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let parent_key = cache_key(parent.as_ref(), None);
        let ctx = StatisticsContext::new();

        let _ = ctx
            .compute(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert!(
            !ctx.cache.lock().statistics.contains_key(&parent_key),
            "borrowed roots must not be memoized"
        );

        drop(parent);
        drop(leaf);
        assert!(weak.upgrade().is_some());

        ctx.reset_cache();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn borrowed_compute_hits_root_cached_by_compute_arc() {
        let leaf = make_stats_leaf(10);
        let ctx = StatisticsContext::new();
        let args = StatisticsArgs::new();

        let owned = ctx.compute_arc(&leaf, &args).unwrap();
        let borrowed = ctx.compute(leaf.as_ref(), &args).unwrap();
        assert!(Arc::ptr_eq(&owned, &borrowed));
    }

    #[test]
    fn extension_cache_retains_plan_lifetime() {
        let leaf = make_stats_leaf(10);
        let weak = Arc::downgrade(&leaf);
        let ctx = ctx_with(Arc::new(TagLeafProvider { rows: 10, tag: 7 }));

        let _ = ctx
            .compute_extended_arc(&leaf, &StatisticsArgs::new())
            .unwrap();
        ctx.cache.lock().statistics.clear();
        drop(leaf);
        assert!(weak.upgrade().is_some());

        ctx.reset_cache();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn reset_cache_clears_entries() {
        let leaf = make_stats_leaf(10);
        let ctx = StatisticsContext::new();
        let _ = ctx.compute_arc(&leaf, &StatisticsArgs::new()).unwrap();
        assert!(!ctx.cache.lock().statistics.is_empty());
        ctx.reset_cache();
        assert!(ctx.cache.lock().statistics.is_empty());
    }

    #[test]
    fn partition_aware_provider_applies_per_partition() {
        let leaf = make_stats_leaf(10);
        let ctx = ctx_with(Arc::new(PartitionRowCountProvider));

        let per_part = ctx
            .compute(
                leaf.as_ref(),
                &StatisticsArgs::new().with_partition(Some(0)),
            )
            .unwrap();
        assert_eq!(per_part.num_rows, Precision::Exact(701));
    }

    #[test]
    fn extensions_reach_parent_provider() {
        let leaf = make_stats_leaf(100);
        let parent: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(leaf));
        let ctx = StatisticsContext::new_with_registry(
            StatisticsRegistry::with_providers(vec![
                Arc::new(TagLeafProvider { rows: 100, tag: 7 }),
                Arc::new(TagDoublingProvider),
            ]),
        );
        let extended = ctx
            .compute_extended(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(extended.get_extension::<Tag>(), Some(&Tag(14)));
    }

    #[test]
    fn cached_node_keeps_extensions() {
        let leaf = make_stats_leaf(100);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let ctx = ctx_with(Arc::new(TagLeafProvider { rows: 100, tag: 7 }));

        // Walking the parent caches the leaf as a child ...
        let _ = ctx
            .compute(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        // ... so computing the leaf directly is a cache hit that must still
        // return the extensions its provider attached.
        let first = ctx
            .compute_extended(leaf.as_ref(), &StatisticsArgs::new())
            .unwrap();
        let second = ctx
            .compute_extended(leaf.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(first.get_extension::<Tag>(), Some(&Tag(7)));
        assert_eq!(second.get_extension::<Tag>(), Some(&Tag(7)));
        assert!(Arc::ptr_eq(first.base_arc(), second.base_arc()));
    }

    /// The walk returns a freshly computed node's extensions from the provider
    /// result itself, not from the cache, so `compute_extended` does not depend
    /// on the root having a cache entry. On a cache hit they are read only on
    /// request.
    #[test]
    fn walk_returns_provider_extensions_directly() {
        let leaf = make_stats_leaf(100);
        let ctx = ctx_with(Arc::new(TagLeafProvider { rows: 100, tag: 7 }));
        let args = StatisticsArgs::new();

        // Fresh computation: returned even though no cached read was requested,
        // and still recorded for parents.
        let (_, extensions) = ctx
            .compute_base(leaf.as_ref(), Some(&leaf), &args, false)
            .unwrap();
        assert_eq!(extensions.unwrap().get::<Tag>(), Some(&Tag(7)));
        assert!(
            ctx.cache
                .lock()
                .extensions
                .contains_key(&cache_key(leaf.as_ref(), None))
        );

        // Cache hit: extensions are cloned out of the cache only on request.
        let (_, extensions) = ctx
            .compute_base(leaf.as_ref(), Some(&leaf), &args, false)
            .unwrap();
        assert!(extensions.is_none());
        let (_, extensions) = ctx
            .compute_base(leaf.as_ref(), Some(&leaf), &args, true)
            .unwrap();
        assert_eq!(extensions.unwrap().get::<Tag>(), Some(&Tag(7)));
    }

    #[test]
    fn builtin_fallback_drops_extensions() {
        let leaf = make_stats_leaf(100);
        let parent: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(&leaf)));
        let ctx = ctx_with(Arc::new(TagLeafProvider { rows: 100, tag: 7 }));

        let leaf_extended = ctx
            .compute_extended(leaf.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(leaf_extended.get_extension::<Tag>(), Some(&Tag(7)));

        let parent_extended = ctx
            .compute_extended(parent.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(parent_extended.get_extension::<Tag>(), None);
        assert_eq!(parent_extended.base().num_rows, Precision::Exact(100));
    }

    #[test]
    fn per_partition_union_with_registry_no_out_of_bounds() {
        // Two 2-partition inputs -> 4 output partitions. Union owns output
        // partition 3 via its second input (owning_input(3) = (1, 1)); the first
        // input is Skipped, so the walk supplies a placeholder for it (never
        // resolving it at partition 3, which is out of that input's 0..2 range).
        // An overall-only provider delegates for a specific partition, so p3 keeps
        // the operator's honest per-partition answer while the overall request
        // picks up the provider's row count.
        let union =
            UnionExec::try_new(vec![make_stats_leaf(10), make_stats_leaf(20)]).unwrap();
        let ctx = ctx_with(Arc::new(FixedRowCountProvider(999)));

        let p3 = ctx
            .compute(
                union.as_ref(),
                &StatisticsArgs::new().with_partition(Some(3)),
            )
            .unwrap();
        assert_eq!(p3.num_rows, Precision::Absent);

        let overall = ctx.compute(union.as_ref(), &StatisticsArgs::new()).unwrap();
        assert_eq!(overall.num_rows, Precision::Exact(999));
    }

    /// Only an exact row count can show that a fetch on each of four partitions
    /// drops no row. An estimate cannot, so the result becomes inexact, column
    /// statistics included.
    #[test]
    fn per_partition_fetch_keeps_exactness_only_for_exact_counts() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let fetch_10_of_4 = |num_rows| {
            let mut input = Statistics::new_unknown(&schema);
            input.num_rows = num_rows;
            input.column_statistics[0].null_count = Precision::Exact(3);
            with_per_partition_fetch(input, Some(10), 4, &StatisticsArgs::new())
        };

        let exact = fetch_10_of_4(Precision::Exact(8))?;
        assert_eq!(exact.num_rows, Precision::Exact(8));
        assert_eq!(exact.column_statistics[0].null_count, Precision::Exact(3));

        let estimate = fetch_10_of_4(Precision::Inexact(8))?;
        assert_eq!(estimate.num_rows, Precision::Inexact(8));
        assert_eq!(
            estimate.column_statistics[0].null_count,
            Precision::Inexact(3)
        );
        Ok(())
    }

    /// Row counts under a fetch that applies to each of four partitions.
    #[test]
    fn per_partition_fetch_counts_every_partition() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let overall = StatisticsArgs::new();
        let first_partition = StatisticsArgs::new().with_partition(Some(0));
        let num_rows = |input_rows, fetch, n_partitions, args: &StatisticsArgs| {
            let mut input = Statistics::new_unknown(&schema);
            input.num_rows = input_rows;
            with_per_partition_fetch(input, fetch, n_partitions, args)
                .map(|stats| stats.num_rows)
        };

        // Every partition keeps up to 10 rows. How the 400 rows are spread over
        // the partitions is unknown, so the count is an estimate.
        assert_eq!(
            num_rows(Precision::Exact(400), Some(10), 4, &overall)?,
            Precision::Inexact(40)
        );
        // The output never has more rows than the input.
        assert_eq!(
            num_rows(Precision::Exact(25), Some(10), 4, &overall)?,
            Precision::Inexact(25)
        );
        // A fetch that cannot drop any row leaves the input exact.
        assert_eq!(
            num_rows(Precision::Exact(8), Some(10), 4, &overall)?,
            Precision::Exact(8)
        );
        // Without an input estimate, the fetch still bounds every partition.
        assert_eq!(
            num_rows(Precision::Absent, Some(10), 4, &overall)?,
            Precision::Inexact(40)
        );
        // A single partition, or a single output partition, keeps `fetch` rows.
        assert_eq!(
            num_rows(Precision::Exact(100), Some(10), 4, &first_partition)?,
            Precision::Exact(10)
        );
        assert_eq!(
            num_rows(Precision::Exact(400), Some(10), 1, &overall)?,
            Precision::Exact(10)
        );
        // Without a fetch, nothing changes.
        assert_eq!(
            num_rows(Precision::Exact(400), None, 4, &overall)?,
            Precision::Exact(400)
        );
        Ok(())
    }
}

#[cfg(test)]
mod partition_stats_tests {
    use super::*;
    use crate::operator_statistics::StatisticsProvider;
    use crate::union::UnionExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, stats::Precision};

    #[derive(Debug)]
    struct PartitionRowCountProvider;
    impl StatisticsProvider for PartitionRowCountProvider {
        fn compute_statistics_with_args(
            &self,
            plan: &dyn ExecutionPlan,
            _child_stats: &[ExtendedStatistics],
            args: &StatisticsArgs,
        ) -> Result<StatisticsResult> {
            let marker = 700 + args.partition().map_or(0, |p| p + 1);
            let mut stats = Statistics::new_unknown(&plan.schema());
            stats.num_rows = Precision::Exact(marker);
            Ok(StatisticsResult::Computed(ExtendedStatistics::new(stats)))
        }
    }

    #[test]
    fn compute_partitions_matches_per_partition_and_deduplicates() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::DisplayAs;
        use crate::DisplayFormatType;
        use crate::PlanProperties;
        use crate::execution_plan::{Boundedness, EmissionType};
        use crate::filter::FilterExec;
        use crate::joins::{HashJoinExec, PartitionMode};
        use crate::projection::ProjectionExec;
        use crate::repartition::RepartitionExec;
        use datafusion_common::tree_node::TreeNodeRecursion;
        use datafusion_common::{NullEquality, ScalarValue};
        use datafusion_execution::{SendableRecordBatchStream, TaskContext};
        use datafusion_expr::{JoinType, Operator};
        use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
        use datafusion_physical_expr::{
            EquivalenceProperties, Partitioning, PhysicalExpr,
        };

        #[derive(Debug)]
        struct SharedEmptyLeafExec {
            partition_stats: Vec<Arc<Statistics>>,
            overall_stats: Arc<Statistics>,
            cache: Arc<PlanProperties>,
        }

        impl DisplayAs for SharedEmptyLeafExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "SharedEmptyLeafExec")
            }
        }

        impl ExecutionPlan for SharedEmptyLeafExec {
            fn name(&self) -> &'static str {
                "SharedEmptyLeafExec"
            }

            fn properties(&self) -> &Arc<PlanProperties> {
                &self.cache
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![]
            }

            fn apply_expressions(
                &self,
                _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
            ) -> Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }

            fn with_new_children(
                self: Arc<Self>,
                _children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }

            fn execute(
                &self,
                _partition: usize,
                _context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                unimplemented!()
            }

            fn statistics_from_inputs(
                &self,
                _input_stats: &[Arc<Statistics>],
                args: &StatisticsArgs,
            ) -> Result<Arc<Statistics>> {
                match args.partition() {
                    Some(idx) => Ok(Arc::clone(&self.partition_stats[idx])),
                    None => Ok(Arc::clone(&self.overall_stats)),
                }
            }
        }

        #[derive(Debug)]
        struct CountingElementwiseExec {
            input: Arc<dyn ExecutionPlan>,
            calls: Arc<AtomicUsize>,
        }

        impl DisplayAs for CountingElementwiseExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "CountingElementwiseExec")
            }
        }

        impl ExecutionPlan for CountingElementwiseExec {
            fn name(&self) -> &'static str {
                "CountingElementwiseExec"
            }

            fn properties(&self) -> &Arc<PlanProperties> {
                self.input.properties()
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![&self.input]
            }

            fn apply_expressions(
                &self,
                _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
            ) -> Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }

            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(Self {
                    input: Arc::clone(&children[0]),
                    calls: Arc::clone(&self.calls),
                }))
            }

            fn execute(
                &self,
                partition: usize,
                context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                self.input.execute(partition, context)
            }

            fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
                vec![ChildStats::At(partition)]
            }

            fn partition_stats_mode(&self) -> PartitionStatsMode {
                PartitionStatsMode::ElementwiseFromChildren
            }

            fn statistics_from_inputs(
                &self,
                input_stats: &[Arc<Statistics>],
                _args: &StatisticsArgs,
            ) -> Result<Arc<Statistics>> {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Ok(Arc::clone(&input_stats[0]))
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let n_partitions = 20usize;
        let empty = Arc::new(
            Statistics::new_unknown(&schema)
                .with_num_rows(Precision::Exact(0))
                .with_total_byte_size(Precision::Exact(0)),
        );
        // Interleave 4 distinct non-empty partitions at indices 0, 5, 10, 15
        // with 16 empty partitions sharing `empty`.
        let make_leaf = || -> Arc<dyn ExecutionPlan> {
            let partition_stats: Vec<Arc<Statistics>> = (0..n_partitions)
                .map(|i| {
                    if i % 5 == 0 {
                        let rows = 100 + i * 10;
                        Arc::new(Statistics {
                            num_rows: Precision::Exact(rows),
                            total_byte_size: Precision::Exact(rows * 16),
                            column_statistics: vec![
                                ColumnStatistics {
                                    null_count: Precision::Exact(0),
                                    min_value: Precision::Exact(ScalarValue::Int64(
                                        Some(i as i64 * 1000),
                                    )),
                                    max_value: Precision::Exact(ScalarValue::Int64(
                                        Some(i as i64 * 1000 + 99),
                                    )),
                                    ..ColumnStatistics::new_unknown()
                                },
                                ColumnStatistics::new_unknown(),
                            ],
                        })
                    } else {
                        Arc::clone(&empty)
                    }
                })
                .collect();
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(n_partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Arc::new(SharedEmptyLeafExec {
                partition_stats,
                overall_stats: Arc::new(
                    Statistics::new_unknown(&schema).with_num_rows(Precision::Exact(700)),
                ),
                cache,
            })
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let build_branch =
            |counter: Option<&Arc<AtomicUsize>>| -> Result<Arc<dyn ExecutionPlan>> {
                let mut node = make_leaf();
                if let Some(c) = counter {
                    node = Arc::new(CountingElementwiseExec {
                        input: node,
                        calls: Arc::clone(c),
                    });
                }
                let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                    col("b", &schema)?,
                    Operator::Gt,
                    lit(0i64),
                ));
                let filter: Arc<dyn ExecutionPlan> =
                    Arc::new(FilterExec::try_new(pred, node)?);
                Ok(Arc::new(ProjectionExec::try_new(
                    vec![
                        (col("a", &schema)?, "a".to_string()),
                        (col("b", &schema)?, "b".to_string()),
                    ],
                    filter,
                )?))
            };

        let left = build_branch(Some(&calls))?;
        let right = build_branch(None)?;
        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            vec![(col("a", &schema)?, col("a", &schema)?)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?);

        // 1. Elementwise tree: 4 distinct active + 1 shared empty => 5 evaluations (not 20),
        //    and every partition matches single-partition `compute`.
        calls.store(0, Ordering::SeqCst);
        let batch_ctx = StatisticsContext::new();
        let batch = batch_ctx.compute_partitions_arc(&join)?;
        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert_eq!(batch.len(), n_partitions);

        let single_ctx = StatisticsContext::new();
        for (i, got) in batch.iter().enumerate() {
            let expected = single_ctx.compute(
                join.as_ref(),
                &StatisticsArgs::new().with_partition(Some(i)),
            )?;
            assert_eq!(got.as_ref(), expected.as_ref(), "partition {i} mismatch");
            // Subsequent single-partition lookup on `batch_ctx` hits the cached slice.
            let from_slice_cache = batch_ctx.compute(
                join.as_ref(),
                &StatisticsArgs::new().with_partition(Some(i)),
            )?;
            assert!(Arc::ptr_eq(got, &from_slice_cache));
        }

        // 2. PerPartitionIndex operators (`UnionExec` and `RepartitionExec`) + providers.
        let union = UnionExec::try_new(vec![left, right])?;
        let union_batch = StatisticsContext::new().compute_partitions(union.as_ref())?;
        assert_eq!(union_batch.len(), n_partitions * 2);
        for (i, got) in union_batch.iter().enumerate() {
            let expected = StatisticsContext::new().compute(
                union.as_ref(),
                &StatisticsArgs::new().with_partition(Some(i)),
            )?;
            assert_eq!(got.as_ref(), expected.as_ref(), "union partition {i}");
        }

        let repart: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            Arc::clone(&join),
            Partitioning::RoundRobinBatch(4),
        )?);
        let repart_batch =
            StatisticsContext::new().compute_partitions(repart.as_ref())?;
        assert_eq!(repart_batch.len(), 4);
        for (i, got) in repart_batch.iter().enumerate() {
            let expected = StatisticsContext::new().compute(
                repart.as_ref(),
                &StatisticsArgs::new().with_partition(Some(i)),
            )?;
            assert_eq!(got.as_ref(), expected.as_ref(), "repart partition {i}");
        }

        let provider_ctx = StatisticsContext::new_with_registry(
            StatisticsRegistry::with_providers(vec![Arc::new(PartitionRowCountProvider)]),
        );
        let prov_batch = provider_ctx.compute_partitions(join.as_ref())?;
        for (i, got) in prov_batch.iter().enumerate() {
            assert_eq!(got.num_rows, Precision::Exact(701 + i));
        }

        Ok(())
    }
}
