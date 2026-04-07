//! Rule-based logical plan rewrites.

mod constant_folding;
mod predicate_pushdown;
mod projection_pruning;

use crate::error::Result;
use crate::planner::LogicalPlan;

/// Collects distinct column keys referenced by a logical [`crate::planner::Expr`].
pub use projection_pruning::collect_columns;

/// Canonical logical column name (`qual.name` or `name`).
pub(crate) fn column_key(name: &str, relation: Option<&String>) -> String {
    match relation {
        Some(r) => format!("{r}.{name}"),
        None => name.to_string(),
    }
}

/// One rewrite pass over a [`LogicalPlan`].
pub trait OptimizerRule {
    /// Human-readable rule name (logging).
    fn name(&self) -> &str;
    /// Returns an equivalent optimized plan.
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

fn rule_trace(rule: &str, msg: &str) {
    if std::env::var_os("ARBOR_OPTIMIZER_TRACE").is_some() {
        eprintln!("[optimizer::{rule}] {msg}");
    }
}

/// Applies predicate pushdown, projection pruning, then constant folding.
pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
    let pushdown = predicate_pushdown::PredicatePushdown;
    rule_trace(pushdown.name(), "start");
    let plan = pushdown.optimize(plan)?;
    let prune = projection_pruning::ProjectionPruning;
    rule_trace(prune.name(), "start");
    let plan = prune.optimize(plan)?;
    let fold = constant_folding::ConstantFolding;
    rule_trace(fold.name(), "start");
    fold.optimize(plan)
}
