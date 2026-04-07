//! Evaluate constant expressions and drop always-true filters.

use super::rule_trace;
use crate::error::Result;
use crate::planner::{BinaryOp, Expr, LogicalPlan};
use crate::types::ScalarValue;

/// Folds constant sub-expressions and removes trivial filters.
pub struct ConstantFolding;

impl super::OptimizerRule for ConstantFolding {
    fn name(&self) -> &str {
        "constant_folding"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        fold_plan(plan)
    }
}

fn fold_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = fold_plan(*input)?;
            let p = fold_expr(&predicate);
            if let Expr::Literal(ScalarValue::Boolean(true)) = p {
                rule_trace("constant_folding", "remove always-true filter");
                return Ok(input);
            }
            if let Expr::Literal(ScalarValue::Boolean(false)) = p {
                rule_trace("constant_folding", "replace always-false filter with Empty");
                return Ok(LogicalPlan::Empty {
                    schema: input.schema().clone(),
                });
            }
            Ok(LogicalPlan::Filter {
                predicate: p,
                input: Box::new(input),
            })
        }
        LogicalPlan::Projection {
            exprs,
            schema,
            input,
        } => Ok(LogicalPlan::Projection {
            exprs: exprs.iter().map(fold_expr).collect(),
            schema,
            input: Box::new(fold_plan(*input)?),
        }),
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            schema,
            input,
        } => Ok(LogicalPlan::Aggregate {
            group_by: group_by.iter().map(fold_expr).collect(),
            aggr_exprs: aggr_exprs.iter().map(fold_expr).collect(),
            schema,
            input: Box::new(fold_plan(*input)?),
        }),
        LogicalPlan::Sort { exprs, input } => Ok(LogicalPlan::Sort {
            exprs: exprs
                .into_iter()
                .map(|mut se| {
                    se.expr = fold_expr(&se.expr);
                    se
                })
                .collect(),
            input: Box::new(fold_plan(*input)?),
        }),
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            schema,
        } => Ok(LogicalPlan::Join {
            left: Box::new(fold_plan(*left)?),
            right: Box::new(fold_plan(*right)?),
            on: on
                .into_iter()
                .map(|(l, r)| (fold_expr(&l), fold_expr(&r)))
                .collect(),
            join_type,
            schema,
        }),
        LogicalPlan::Limit { n, input } => Ok(LogicalPlan::Limit {
            n,
            input: Box::new(fold_plan(*input)?),
        }),
        scan @ LogicalPlan::Scan { .. } => Ok(scan),
        empty @ LogicalPlan::Empty { .. } => Ok(empty),
    }
}

fn fold_expr(expr: &Expr) -> Expr {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            let l = fold_expr(left);
            let r = fold_expr(right);
            if let (Expr::Literal(a), Expr::Literal(b)) = (&l, &r) {
                if let Some(v) = try_fold_binary(a, *op, b) {
                    return Expr::Literal(v);
                }
            }
            Expr::BinaryExpr {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            }
        }
        Expr::AggregateFunc { func, arg } => Expr::AggregateFunc {
            func: *func,
            arg: Box::new(fold_expr(arg)),
        },
        Expr::Alias { name, expr } => Expr::Alias {
            expr: Box::new(fold_expr(expr)),
            name: name.clone(),
        },
        other => other.clone(),
    }
}

fn try_fold_binary(a: &ScalarValue, op: BinaryOp, b: &ScalarValue) -> Option<ScalarValue> {
    use ScalarValue::*;
    match op {
        BinaryOp::Eq => Some(Boolean(a == b)),
        BinaryOp::Neq => Some(Boolean(a != b)),
        BinaryOp::Lt => Some(Boolean(compare_scalars(a, b)? == std::cmp::Ordering::Less)),
        BinaryOp::Gt => Some(Boolean(
            compare_scalars(a, b)? == std::cmp::Ordering::Greater,
        )),
        BinaryOp::LtEq => Some(Boolean(matches!(
            compare_scalars(a, b)?,
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal
        ))),
        BinaryOp::GtEq => Some(Boolean(matches!(
            compare_scalars(a, b)?,
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ))),
        BinaryOp::And => Some(Boolean(scalar_as_bool(a)? && scalar_as_bool(b)?)),
        BinaryOp::Or => Some(Boolean(scalar_as_bool(a)? || scalar_as_bool(b)?)),
        BinaryOp::Plus => fold_arith(a, b, |x, y| x + y, |x, y| x + y),
        BinaryOp::Minus => fold_arith(a, b, |x, y| x - y, |x, y| x - y),
        BinaryOp::Mul => fold_arith(a, b, |x, y| x * y, |x, y| x * y),
        BinaryOp::Div => fold_arith(a, b, |x, y| x / y, |x, y| x / y),
    }
}

fn scalar_as_bool(s: &ScalarValue) -> Option<bool> {
    match s {
        ScalarValue::Boolean(b) => Some(*b),
        _ => None,
    }
}

fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    use ScalarValue::*;
    match (a, b) {
        (Int64(x), Int64(y)) => Some(x.cmp(y)),
        (Float64(x), Float64(y)) => x.partial_cmp(y),
        (Utf8(x), Utf8(y)) => Some(x.cmp(y)),
        (Boolean(x), Boolean(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn fold_arith(
    a: &ScalarValue,
    b: &ScalarValue,
    fi: fn(i64, i64) -> i64,
    ff: fn(f64, f64) -> f64,
) -> Option<ScalarValue> {
    use ScalarValue::*;
    match (a, b) {
        (Int64(x), Int64(y)) => Some(Int64(fi(*x, *y))),
        (Float64(x), Float64(y)) => Some(Float64(ff(*x, *y))),
        (Int64(x), Float64(y)) => Some(Float64(ff(*x as f64, *y))),
        (Float64(x), Int64(y)) => Some(Float64(ff(*x, *y as f64))),
        _ => None,
    }
}
