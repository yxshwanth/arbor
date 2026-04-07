//! Push filters toward scans and join inputs.

use super::projection_pruning::collect_columns;
use super::rule_trace;
use crate::error::Result;
use crate::planner::{BinaryOp, Expr, JoinType, LogicalPlan};

/// Pushes [`LogicalPlan::Filter`] nodes toward scans and join inputs where safe.
pub struct PredicatePushdown;

impl super::OptimizerRule for PredicatePushdown {
    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        optimize_rec(plan)
    }
}

fn optimize_rec(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = optimize_rec(*input)?;
            push_filter_or_merge(predicate, input)
        }
        LogicalPlan::Projection {
            exprs,
            schema,
            input,
        } => Ok(LogicalPlan::Projection {
            exprs,
            schema,
            input: Box::new(optimize_rec(*input)?),
        }),
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            schema,
            input,
        } => Ok(LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            schema,
            input: Box::new(optimize_rec(*input)?),
        }),
        LogicalPlan::Sort { exprs, input } => Ok(LogicalPlan::Sort {
            exprs,
            input: Box::new(optimize_rec(*input)?),
        }),
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => {
            let left = optimize_rec(*left)?;
            let right = optimize_rec(*right)?;
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                join_type,
                schema,
            })
        }
        LogicalPlan::Limit { n, input } => Ok(LogicalPlan::Limit {
            n,
            input: Box::new(optimize_rec(*input)?),
        }),
        LogicalPlan::Scan { .. } | LogicalPlan::Empty { .. } => Ok(plan),
    }
}

fn push_filter_or_merge(predicate: Expr, input: LogicalPlan) -> Result<LogicalPlan> {
    match input {
        LogicalPlan::Filter {
            predicate: inner_pred,
            input: inner,
        } => {
            rule_trace("predicate_pushdown", "merge nested Filter with AND");
            let merged = Expr::BinaryExpr {
                left: Box::new(predicate),
                op: BinaryOp::And,
                right: Box::new(inner_pred),
            };
            Ok(LogicalPlan::Filter {
                predicate: merged,
                input: inner,
            })
        }
        LogicalPlan::Projection {
            exprs,
            schema,
            input: child,
        } => {
            if let Some(rewritten) =
                try_rewrite_filter_below_projection(&predicate, &exprs, &schema, child.as_ref())
            {
                rule_trace("predicate_pushdown", "push Filter below Projection");
                return Ok(LogicalPlan::Projection {
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                    input: Box::new(LogicalPlan::Filter {
                        predicate: rewritten,
                        input: child,
                    }),
                });
            }
            Ok(LogicalPlan::Filter {
                predicate,
                input: Box::new(LogicalPlan::Projection {
                    exprs,
                    schema,
                    input: child,
                }),
            })
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => push_predicates_into_join(predicate, *left, *right, on, join_type, schema),
        other => Ok(LogicalPlan::Filter {
            predicate,
            input: Box::new(other),
        }),
    }
}

fn push_predicates_into_join(
    predicate: Expr,
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: crate::types::Schema,
) -> Result<LogicalPlan> {
    let left_width = left.schema().fields.len();
    let left_names: std::collections::HashSet<String> = schema
        .fields
        .iter()
        .take(left_width)
        .map(|f| f.name.clone())
        .collect();
    let right_names: std::collections::HashSet<String> = schema
        .fields
        .iter()
        .skip(left_width)
        .map(|f| f.name.clone())
        .collect();
    let conjuncts = flatten_and(predicate);
    let mut above = Vec::new();
    let mut left_preds = Vec::new();
    let mut mut_right_preds = Vec::new();
    for c in conjuncts {
        let cols = collect_columns(&c);
        let touches_left = cols.iter().any(|n| left_names.contains(n));
        let touches_right = cols.iter().any(|n| right_names.contains(n));
        if touches_left && touches_right {
            above.push(c);
        } else if touches_left {
            left_preds.push(c);
        } else if touches_right {
            mut_right_preds.push(c);
        } else {
            above.push(c);
        }
    }
    let mut new_left = left;
    if !left_preds.is_empty() {
        rule_trace("predicate_pushdown", "push predicates to join left");
        new_left = LogicalPlan::Filter {
            predicate: and_all(left_preds),
            input: Box::new(new_left),
        };
    }
    let mut new_right = right;
    if !mut_right_preds.is_empty() {
        rule_trace("predicate_pushdown", "push predicates to join right");
        new_right = LogicalPlan::Filter {
            predicate: and_all(mut_right_preds),
            input: Box::new(new_right),
        };
    }
    let join = LogicalPlan::Join {
        left: Box::new(optimize_rec(new_left)?),
        right: Box::new(optimize_rec(new_right)?),
        on,
        join_type,
        schema,
    };
    if above.is_empty() {
        Ok(join)
    } else {
        Ok(LogicalPlan::Filter {
            predicate: and_all(above),
            input: Box::new(join),
        })
    }
}

fn try_rewrite_filter_below_projection(
    pred: &Expr,
    proj_exprs: &[Expr],
    proj_schema: &crate::types::Schema,
    _child: &LogicalPlan,
) -> Option<Expr> {
    let mut map: std::collections::HashMap<String, Expr> = std::collections::HashMap::new();
    for (i, e) in proj_exprs.iter().enumerate() {
        let key = proj_schema.fields.get(i)?.name.clone();
        let inner = match e {
            Expr::Alias { expr, .. } => expr.as_ref().clone(),
            o => o.clone(),
        };
        map.insert(key, inner);
    }
    Some(substitute_columns(pred, &map))
}

fn substitute_columns(expr: &Expr, map: &std::collections::HashMap<String, Expr>) -> Expr {
    match expr {
        Expr::Column { name, relation } => {
            let k = super::column_key(name, relation.as_ref());
            if let Some(rep) = map.get(&k) {
                rep.clone()
            } else {
                expr.clone()
            }
        }
        Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
            left: Box::new(substitute_columns(left, map)),
            op: *op,
            right: Box::new(substitute_columns(right, map)),
        },
        Expr::AggregateFunc { func, arg } => Expr::AggregateFunc {
            func: *func,
            arg: Box::new(substitute_columns(arg, map)),
        },
        Expr::Alias { expr, name } => Expr::Alias {
            expr: Box::new(substitute_columns(expr, map)),
            name: name.clone(),
        },
        _ => expr.clone(),
    }
}

fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut v = flatten_and(*left);
            v.extend(flatten_and(*right));
            v
        }
        other => vec![other],
    }
}

fn and_all(exprs: Vec<Expr>) -> Expr {
    let mut iter = exprs.into_iter();
    let first = match iter.next() {
        Some(e) => e,
        None => Expr::Literal(crate::types::ScalarValue::Boolean(true)),
    };
    iter.fold(first, |acc, e| Expr::BinaryExpr {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(e),
    })
}
