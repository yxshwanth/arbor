//! Scalar/boolean expression evaluation over a single `RecordBatch`.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::compute::kernels::cast;
use arrow::compute::kernels::{boolean, cmp, numeric};
use arrow::datatypes::DataType;

use crate::error::{ArborError, Result};
use crate::executor::batch_column_index;
use crate::planner::{BinaryOp, Expr};
use crate::types::ScalarValue;

/// Evaluates `expr` for every row of `batch`, returning a new array.
pub fn evaluate_expr(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        Expr::Column { name, relation } => {
            let idx = batch_column_index(batch.schema().as_ref(), name, relation.as_deref())?;
            Ok(batch.column(idx).clone())
        }
        Expr::Literal(s) => scalar_to_array(s, batch.num_rows()),
        Expr::BinaryExpr { left, op, right } => {
            let l = evaluate_expr(left, batch)?;
            let r = evaluate_expr(right, batch)?;
            eval_binary_array(&l, *op, &r)
        }
        Expr::AggregateFunc { .. } => Err(ArborError::Execution(
            "aggregate expression not supported in row-wise eval".into(),
        )),
        Expr::Alias { expr, .. } => evaluate_expr(expr, batch),
        Expr::Wildcard => Err(ArborError::Execution(
            "wildcard cannot be evaluated row-wise".into(),
        )),
    }
}

fn scalar_to_array(s: &ScalarValue, n: usize) -> Result<ArrayRef> {
    Ok(match s {
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; n])),
        ScalarValue::Float64(v) => Arc::new(arrow::array::Float64Array::from(vec![*v; n])),
        ScalarValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str(); n])),
        ScalarValue::Boolean(v) => Arc::new(arrow::array::BooleanArray::from(vec![*v; n])),
        ScalarValue::Null => {
            let mut b = arrow::array::BooleanBuilder::new();
            for _ in 0..n {
                b.append_null();
            }
            Arc::new(b.finish())
        }
    })
}

fn eval_binary_array(left: &ArrayRef, op: BinaryOp, right: &ArrayRef) -> Result<ArrayRef> {
    use BinaryOp::*;
    Ok(match op {
        Eq => Arc::new(cmp::eq(left, right).map_err(ArborError::from)?),
        Neq => Arc::new(cmp::neq(left, right).map_err(ArborError::from)?),
        Lt => Arc::new(cmp::lt(left, right).map_err(ArborError::from)?),
        Gt => Arc::new(cmp::gt(left, right).map_err(ArborError::from)?),
        LtEq => Arc::new(cmp::lt_eq(left, right).map_err(ArborError::from)?),
        GtEq => Arc::new(cmp::gt_eq(left, right).map_err(ArborError::from)?),
        And => {
            let l = cast::cast(left, &DataType::Boolean).map_err(ArborError::from)?;
            let r = cast::cast(right, &DataType::Boolean).map_err(ArborError::from)?;
            let lb = l
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ArborError::Execution("AND lhs".into()))?;
            let rb = r
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ArborError::Execution("AND rhs".into()))?;
            Arc::new(boolean::and(lb, rb).map_err(ArborError::from)?)
        }
        Or => {
            let l = cast::cast(left, &DataType::Boolean).map_err(ArborError::from)?;
            let r = cast::cast(right, &DataType::Boolean).map_err(ArborError::from)?;
            let lb = l
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ArborError::Execution("OR lhs".into()))?;
            let rb = r
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ArborError::Execution("OR rhs".into()))?;
            Arc::new(boolean::or(lb, rb).map_err(ArborError::from)?)
        }
        Plus => numeric::add(left, right).map_err(ArborError::from)?,
        Minus => numeric::sub(left, right).map_err(ArborError::from)?,
        Mul => numeric::mul(left, right).map_err(ArborError::from)?,
        Div => numeric::div(left, right).map_err(ArborError::from)?,
    })
}

#[cfg(test)]
mod tests {
    use super::evaluate_expr;
    use crate::planner::{BinaryOp, Expr};
    use crate::types::ScalarValue;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("age", DataType::Int64, true),
            Field::new("city", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![20, 30, 40])),
                Arc::new(StringArray::from(vec!["NYC", "LA", "NYC"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn col_and_predicates() {
        let b = test_batch();
        let age = evaluate_expr(
            &Expr::Column {
                name: "age".into(),
                relation: None,
            },
            &b,
        )
        .unwrap();
        assert_eq!(age.len(), 3);
        let pred = evaluate_expr(
            &Expr::BinaryExpr {
                left: Box::new(Expr::Column {
                    name: "age".into(),
                    relation: None,
                }),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(ScalarValue::Int64(25))),
            },
            &b,
        )
        .unwrap();
        let ba = pred
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!ba.value(0));
        assert!(ba.value(1));
    }
}
