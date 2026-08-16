//! Logical query plan and expression tree.

use std::fmt;

use crate::types::{ScalarValue, Schema};

/// Top-level logical operator tree.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a named table (Parquet file stem).
    Scan {
        /// Table/catalog name.
        table_name: String,
        /// Output row layout (file column names, possibly narrowed).
        schema: Schema,
        /// Optional column indices projection (subset of file columns).
        projection: Option<Vec<usize>>,
    },
    /// Filter rows matching a boolean expression.
    Filter {
        /// Predicate expression.
        predicate: Expr,
        /// Child plan.
        input: Box<LogicalPlan>,
    },
    /// Project expressions with explicit output schema.
    Projection {
        /// Expressions to evaluate.
        exprs: Vec<Expr>,
        /// Result schema (names and types for output columns).
        schema: Schema,
        /// Child plan.
        input: Box<LogicalPlan>,
    },
    /// Hash aggregate: grouping expressions and aggregate expressions.
    Aggregate {
        /// GROUP BY expressions.
        group_by: Vec<Expr>,
        /// Aggregate expressions (e.g. SUM, COUNT).
        aggr_exprs: Vec<Expr>,
        /// Output schema (group keys then aggregates).
        schema: Schema,
        /// Input plan (pre-aggregation rows).
        input: Box<LogicalPlan>,
    },
    /// Sort by sort keys.
    Sort {
        /// Sort key expressions (with ASC/DESC).
        exprs: Vec<SortExpr>,
        /// Child plan.
        input: Box<LogicalPlan>,
    },
    /// Join two plans on equi predicates.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Equi-join pairs (left key, right key).
        on: Vec<(Expr, Expr)>,
        /// Join algorithm choice (logical).
        join_type: JoinType,
        /// Output schema (concatenated columns).
        schema: Schema,
    },
    /// Limit row count.
    Limit {
        /// Maximum rows to emit.
        n: usize,
        /// Child plan.
        input: Box<LogicalPlan>,
    },
    /// Produces zero rows (e.g. constant-false predicate).
    Empty {
        /// Column layout (types only; no data).
        schema: Schema,
    },
}

/// Scalar / boolean expression in the logical plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Column reference (optional qualifier from SQL).
    Column {
        /// Column base name (not including qualifier).
        name: String,
        /// Table/alias qualifier from SQL, if any.
        relation: Option<String>,
    },
    /// Constant value.
    Literal(ScalarValue),
    /// Binary operation.
    BinaryExpr {
        /// Left sub-expression.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOp,
        /// Right sub-expression.
        right: Box<Expr>,
    },
    /// Aggregate function over an argument expression.
    AggregateFunc {
        /// Which aggregate.
        func: AggFunc,
        /// Argument (`Wildcard` used for `COUNT(*)`).
        arg: Box<Expr>,
    },
    /// Rename expression (output label).
    Alias {
        /// Inner expression.
        expr: Box<Expr>,
        /// Output name.
        name: String,
    },
    /// `*` in `COUNT(*)` or projection wildcard.
    Wildcard,
}

/// Binary operators supported by Arbor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    /// Equality.
    Eq,
    /// Inequality.
    Neq,
    /// Less than.
    Lt,
    /// Greater than.
    Gt,
    /// Less than or equal.
    LtEq,
    /// Greater than or equal.
    GtEq,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    /// Addition.
    Plus,
    /// Subtraction.
    Minus,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
}

/// Built-in aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    /// SUM.
    Sum,
    /// COUNT.
    Count,
    /// AVG.
    Avg,
    /// MIN.
    Min,
    /// MAX.
    Max,
}

/// JOIN kind (minimal set).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    /// Inner join.
    Inner,
    /// Left outer join.
    Left,
}

/// One ORDER BY key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortExpr {
    /// Key expression.
    pub expr: Expr,
    /// Ascending if true.
    pub asc: bool,
}

impl LogicalPlan {
    /// Returns the schema of rows produced by this operator.
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Empty { schema } => schema,
        }
    }
}

fn indent_fmt(f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
    for _ in 0..depth {
        write!(f, "  ")?;
    }
    Ok(())
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { name, relation } => {
                if let Some(r) = relation {
                    write!(f, "{r}.{name}")
                } else {
                    write!(f, "{name}")
                }
            }
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "(")?;
                write!(f, "{left}")?;
                let sop = match op {
                    BinaryOp::Eq => "=",
                    BinaryOp::Neq => "!=",
                    BinaryOp::Lt => "<",
                    BinaryOp::Gt => ">",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Plus => "+",
                    BinaryOp::Minus => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                };
                write!(f, " {sop} ")?;
                write!(f, "{right}")?;
                write!(f, ")")
            }
            Expr::AggregateFunc { func, arg } => {
                let name = match func {
                    AggFunc::Sum => "SUM",
                    AggFunc::Count => "COUNT",
                    AggFunc::Avg => "AVG",
                    AggFunc::Min => "MIN",
                    AggFunc::Max => "MAX",
                };
                write!(f, "{name}({arg})")
            }
            Expr::Alias { expr, name } => write!(f, "{expr} AS {name}"),
            Expr::Wildcard => write!(f, "*"),
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn walk(f: &mut fmt::Formatter<'_>, plan: &LogicalPlan, depth: usize) -> fmt::Result {
            match plan {
                LogicalPlan::Scan {
                    table_name,
                    schema,
                    projection,
                } => {
                    indent_fmt(f, depth)?;
                    writeln!(
                        f,
                        "Scan(table={table_name}, cols={}, projection={projection:?})",
                        schema.fields.len()
                    )?;
                }
                LogicalPlan::Filter { predicate, input } => {
                    indent_fmt(f, depth)?;
                    writeln!(f, "Filter({predicate})")?;
                    walk(f, input, depth + 1)?;
                }
                LogicalPlan::Projection {
                    exprs,
                    schema: _,
                    input,
                } => {
                    indent_fmt(f, depth)?;
                    write!(f, "Projection(")?;
                    for (i, e) in exprs.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{e}")?;
                    }
                    writeln!(f, ")")?;
                    walk(f, input, depth + 1)?;
                }
                LogicalPlan::Aggregate {
                    group_by,
                    aggr_exprs,
                    schema: _,
                    input,
                } => {
                    indent_fmt(f, depth)?;
                    write!(f, "Aggregate(groups=")?;
                    for (i, e) in group_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{e}")?;
                    }
                    write!(f, ", aggr=")?;
                    for (i, e) in aggr_exprs.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{e}")?;
                    }
                    writeln!(f, ")")?;
                    walk(f, input, depth + 1)?;
                }
                LogicalPlan::Sort { exprs, input } => {
                    indent_fmt(f, depth)?;
                    write!(f, "Sort(")?;
                    for (i, se) in exprs.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{} {}", se.expr, if se.asc { "ASC" } else { "DESC" })?;
                    }
                    writeln!(f, ")")?;
                    walk(f, input, depth + 1)?;
                }
                LogicalPlan::Join {
                    left,
                    right,
                    on,
                    join_type,
                    schema: _,
                } => {
                    indent_fmt(f, depth)?;
                    let jt = match join_type {
                        JoinType::Inner => "Inner",
                        JoinType::Left => "Left",
                    };
                    write!(f, "Join({jt}, on=")?;
                    for (i, (l, r)) in on.iter().enumerate() {
                        if i > 0 {
                            write!(f, " AND ")?;
                        }
                        write!(f, "{l} = {r}")?;
                    }
                    writeln!(f, ")")?;
                    walk(f, left, depth + 1)?;
                    walk(f, right, depth + 1)?;
                }
                LogicalPlan::Limit { n, input } => {
                    indent_fmt(f, depth)?;
                    writeln!(f, "Limit({n})")?;
                    walk(f, input, depth + 1)?;
                }
                LogicalPlan::Empty { schema } => {
                    indent_fmt(f, depth)?;
                    writeln!(f, "Empty(cols={})", schema.fields.len())?;
                }
            }
            Ok(())
        }
        walk(f, self, 0)
    }
}
