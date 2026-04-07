//! Integration-style tests for the logical optimizer.

use arbor::optimizer::{self, collect_columns};
use arbor::planner::{BinaryOp, Expr, LogicalPlan};
use arbor::types::{Field, Schema};
use arrow::datatypes::DataType;

fn scan5(name: &str) -> LogicalPlan {
    let fields: Vec<Field> = (0..5)
        .map(|i| Field {
            name: format!("c{i}"),
            data_type: DataType::Int64,
        })
        .collect();
    LogicalPlan::Scan {
        table_name: name.to_string(),
        schema: Schema { fields },
        projection: None,
    }
}

#[test]
fn filter_pushed_below_projection() {
    let s = scan5("t");
    let proj = LogicalPlan::Projection {
        exprs: vec![
            Expr::Column {
                name: "c0".into(),
                relation: None,
            },
            Expr::Column {
                name: "c1".into(),
                relation: None,
            },
        ],
        schema: Schema {
            fields: vec![
                Field {
                    name: "c0".into(),
                    data_type: DataType::Int64,
                },
                Field {
                    name: "c1".into(),
                    data_type: DataType::Int64,
                },
            ],
        },
        input: Box::new(s),
    };
    let pred = Expr::BinaryExpr {
        left: Box::new(Expr::Column {
            name: "c0".into(),
            relation: None,
        }),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(arbor::types::ScalarValue::Int64(0))),
    };
    let plan = LogicalPlan::Filter {
        predicate: pred,
        input: Box::new(proj),
    };
    let out = optimizer::optimize(plan).unwrap();
    assert!(
        matches!(out, LogicalPlan::Projection { .. }),
        "expected Projection root, got {out}"
    );
    let LogicalPlan::Projection { input, .. } = out else {
        unreachable!();
    };
    assert!(
        matches!(input.as_ref(), LogicalPlan::Filter { .. }),
        "expected Filter under Projection"
    );
}

#[test]
fn projection_prunes_scan_columns() {
    let scan = scan5("t");
    let proj = LogicalPlan::Projection {
        exprs: vec![
            Expr::Column {
                name: "c0".into(),
                relation: None,
            },
            Expr::Column {
                name: "c4".into(),
                relation: None,
            },
        ],
        schema: Schema {
            fields: vec![
                Field {
                    name: "c0".into(),
                    data_type: DataType::Int64,
                },
                Field {
                    name: "c4".into(),
                    data_type: DataType::Int64,
                },
            ],
        },
        input: Box::new(scan),
    };
    let plan = optimizer::optimize(proj).unwrap();
    match plan {
        LogicalPlan::Projection { input, .. } => match input.as_ref() {
            LogicalPlan::Scan { projection, .. } => {
                assert_eq!(projection.as_ref(), Some(&vec![0, 4]));
            }
            _ => panic!("expected scan child"),
        },
        _ => panic!("expected projection root"),
    }
}

#[test]
fn constant_filter_removed() {
    let scan = scan5("t");
    let pred = Expr::BinaryExpr {
        left: Box::new(Expr::Literal(arbor::types::ScalarValue::Int64(1))),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(arbor::types::ScalarValue::Int64(1))),
    };
    let plan = LogicalPlan::Filter {
        predicate: pred,
        input: Box::new(scan),
    };
    let out = optimizer::optimize(plan).unwrap();
    assert!(
        matches!(out, LogicalPlan::Scan { .. }),
        "expected Filter removed, got {out}"
    );
}

#[test]
fn collect_columns_qualified() {
    let e = Expr::Column {
        name: "id".into(),
        relation: Some("u".into()),
    };
    let c = collect_columns(&e);
    assert!(c.contains("u.id"));
}
