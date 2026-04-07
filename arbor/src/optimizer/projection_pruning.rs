//! Prune scan columns and drop unnecessary projections.

use std::collections::HashSet;

use super::rule_trace;
use crate::error::Result;
use crate::planner::{Expr, LogicalPlan};

/// Pushes column pruning into scans and removes redundant projections.
pub struct ProjectionPruning;

impl super::OptimizerRule for ProjectionPruning {
    fn name(&self) -> &str {
        "projection_pruning"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let needed: HashSet<String> = plan
            .schema()
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect();
        prune_rec(plan, &needed)
    }
}

fn prune_rec(plan: LogicalPlan, parent_needs: &HashSet<String>) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan {
            table_name,
            schema,
            projection: _,
        } => {
            let n_fields = schema.fields.len();
            let mut idxs = Vec::new();
            for (i, f) in schema.fields.iter().enumerate() {
                if parent_needs.contains(&f.name) {
                    idxs.push(i);
                }
            }
            rule_trace(
                "projection_pruning",
                &format!("scan {table_name} indices {idxs:?}"),
            );
            let projection = if idxs.len() == n_fields && n_fields > 0 {
                None
            } else {
                Some(idxs)
            };
            Ok(LogicalPlan::Scan {
                table_name,
                schema,
                projection,
            })
        }
        LogicalPlan::Projection {
            exprs,
            schema,
            input,
        } => {
            let mut child_needs = HashSet::new();
            for (i, f) in schema.fields.iter().enumerate() {
                if parent_needs.contains(&f.name) {
                    child_needs.extend(collect_columns(&exprs[i]));
                }
            }
            let new_input = prune_rec(*input, &child_needs)?;
            if is_identity_projection(&exprs, new_input.schema()) {
                rule_trace("projection_pruning", "remove identity projection");
                return Ok(new_input);
            }
            Ok(LogicalPlan::Projection {
                exprs,
                schema,
                input: Box::new(new_input),
            })
        }
        LogicalPlan::Filter { predicate, input } => {
            let mut needs = parent_needs.clone();
            needs.extend(collect_columns(&predicate));
            Ok(LogicalPlan::Filter {
                predicate,
                input: Box::new(prune_rec(*input, &needs)?),
            })
        }
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            schema,
            input,
        } => {
            let mut needs = HashSet::new();
            for e in &group_by {
                needs.extend(collect_columns(e));
            }
            for e in &aggr_exprs {
                needs.extend(collect_columns(e));
            }
            Ok(LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                schema,
                input: Box::new(prune_rec(*input, &needs)?),
            })
        }
        LogicalPlan::Sort { exprs, input } => {
            let mut needs = parent_needs.clone();
            for se in &exprs {
                needs.extend(collect_columns(&se.expr));
            }
            Ok(LogicalPlan::Sort {
                exprs,
                input: Box::new(prune_rec(*input, &needs)?),
            })
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema: _,
        } => {
            let left_names: HashSet<String> = left
                .schema()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();
            let mut left_needs = HashSet::new();
            let mut right_needs = HashSet::new();
            for n in parent_needs {
                if left_names.contains(n) {
                    left_needs.insert(n.clone());
                } else {
                    right_needs.insert(n.clone());
                }
            }
            for (l, r) in &on {
                left_needs.extend(collect_columns(l));
                right_needs.extend(collect_columns(r));
            }
            let new_left = prune_rec(*left, &left_needs)?;
            let new_right = prune_rec(*right, &right_needs)?;
            let merged = crate::types::Schema {
                fields: new_left
                    .schema()
                    .fields
                    .iter()
                    .chain(new_right.schema().fields.iter())
                    .cloned()
                    .collect(),
            };
            Ok(LogicalPlan::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                on,
                join_type,
                schema: merged,
            })
        }
        LogicalPlan::Limit { n, input } => Ok(LogicalPlan::Limit {
            n,
            input: Box::new(prune_rec(*input, parent_needs)?),
        }),
        LogicalPlan::Empty { schema } => Ok(LogicalPlan::Empty { schema }),
    }
}

fn is_identity_projection(exprs: &[Expr], input_schema: &crate::types::Schema) -> bool {
    if exprs.len() != input_schema.fields.len() {
        return false;
    }
    for (e, f) in exprs.iter().zip(input_schema.fields.iter()) {
        match e {
            Expr::Column { name, relation } => {
                let k = super::column_key(name, relation.as_ref());
                if k != f.name {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Returns logical column names referenced by `expr` (including qualifiers).
pub fn collect_columns(expr: &Expr) -> HashSet<String> {
    let mut s = HashSet::new();
    collect_columns_rec(expr, &mut s);
    s
}

fn collect_columns_rec(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Column { name, relation } => {
            out.insert(super::column_key(name, relation.as_ref()));
        }
        Expr::BinaryExpr { left, op: _, right } => {
            collect_columns_rec(left, out);
            collect_columns_rec(right, out);
        }
        Expr::AggregateFunc { arg, .. } => {
            collect_columns_rec(arg, out);
        }
        Expr::Alias { expr, name: _ } => collect_columns_rec(expr, out),
        Expr::Literal(_) | Expr::Wildcard => {}
    }
}
