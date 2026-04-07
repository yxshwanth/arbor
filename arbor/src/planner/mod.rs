//! Build logical plans from sqlparser AST.

mod logical_plan;

/// Logical plan and expression types ([`LogicalPlan`], [`Expr`], [`BinaryOp`], etc.).
pub use logical_plan::*;

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};

use crate::error::{ArborError, Result};
use crate::types::{Catalog, Field, ScalarValue, Schema};
use arrow::datatypes::DataType;

/// Builds a [`LogicalPlan`] from a parsed `SELECT` [`Statement`].
pub fn plan_query(statement: &Statement, catalog: &Catalog) -> Result<LogicalPlan> {
    let query = match statement {
        Statement::Query(q) => q.as_ref(),
        _ => return Err(ArborError::Plan("expected SELECT statement".into())),
    };
    plan_query_body(query, catalog)
}

fn plan_query_body(query: &Query, catalog: &Catalog) -> Result<LogicalPlan> {
    if query.with.is_some() {
        return Err(ArborError::Plan("WITH / CTE is not supported".into()));
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(ArborError::Plan(
                "only simple SELECT bodies are supported".into(),
            ))
        }
    };
    if select.into.is_some() {
        return Err(ArborError::Plan("INTO is not supported".into()));
    }
    if select.distinct.is_some() {
        return Err(ArborError::Plan("DISTINCT is not supported".into()));
    }
    if select.having.is_some() {
        return Err(ArborError::Plan("HAVING is not supported".into()));
    }
    if !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.top.is_some()
    {
        return Err(ArborError::Plan("unsupported SELECT clause".into()));
    }
    if select.from.len() != 1 {
        return Err(ArborError::Plan("exactly one FROM item is required".into()));
    }
    let from = &select.from[0];
    let (from_plan, from_schema) = plan_table_with_joins(from, catalog)?;
    let group_exprs: Vec<SqlExpr> = match &select.group_by {
        GroupByExpr::Expressions(e) => e.clone(),
        GroupByExpr::All => {
            return Err(ArborError::Plan("GROUP BY ALL is not supported".into()));
        }
    };
    let has_agg = select_items_contain_aggregate(&select.projection)?;
    let is_agg = has_agg || !group_exprs.is_empty();

    let mut plan = if is_agg {
        let mut p = from_plan;
        if let Some(sel) = &select.selection {
            let pred = convert_expr(sel, &from_schema)?;
            p = LogicalPlan::Filter {
                predicate: pred,
                input: Box::new(p),
            };
        }
        let group_by: Vec<Expr> = group_exprs
            .iter()
            .map(|e| convert_expr(e, &from_schema))
            .collect::<Result<_>>()?;
        let group_set: std::collections::HashSet<Expr> = group_by.iter().cloned().collect();
        let (aggr_exprs, out_schema) =
            build_aggregate_outputs(&select.projection, &group_by, &group_set, &from_schema)?;
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            schema: out_schema,
            input: Box::new(p),
        }
    } else {
        let proj = build_simple_projection(&select.projection, &from_schema)?;
        let mut p = LogicalPlan::Projection {
            schema: proj.schema,
            exprs: proj.exprs,
            input: Box::new(from_plan),
        };
        if let Some(sel) = &select.selection {
            let pred = convert_expr(sel, &from_schema)?;
            p = LogicalPlan::Filter {
                predicate: pred,
                input: Box::new(p),
            };
        }
        p
    };

    if !query.order_by.is_empty() {
        let sort_exprs = convert_order_by(&query.order_by, plan.schema())?;
        plan = LogicalPlan::Sort {
            exprs: sort_exprs,
            input: Box::new(plan),
        };
    }

    if let Some(lim) = &query.limit {
        let n = parse_limit(lim)?;
        plan = LogicalPlan::Limit {
            n,
            input: Box::new(plan),
        };
    }

    Ok(plan)
}

struct ProjectionBuild {
    exprs: Vec<Expr>,
    schema: Schema,
}

fn build_simple_projection(items: &[SelectItem], from_schema: &Schema) -> Result<ProjectionBuild> {
    let mut exprs = Vec::new();
    let mut fields = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let ex = convert_expr(e, from_schema)?;
                let name = default_output_name(&ex)?;
                let dt = infer_expr_type(&ex, from_schema)?;
                fields.push(Field {
                    name,
                    data_type: dt,
                });
                exprs.push(ex);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let ex = convert_expr(expr, from_schema)?;
                let name = alias.value.clone();
                let dt = infer_expr_type(&ex, from_schema)?;
                fields.push(Field {
                    name,
                    data_type: dt,
                });
                exprs.push(ex);
            }
            SelectItem::Wildcard(_) => {
                for f in &from_schema.fields {
                    exprs.push(Expr::Column {
                        name: f.name.clone(),
                        relation: None,
                    });
                    fields.push(f.clone());
                }
            }
            SelectItem::QualifiedWildcard(prefix, _) => {
                let rel = object_name_last(prefix);
                for f in &from_schema.fields {
                    if qualified_field_belongs(&f.name, rel.as_str()) {
                        exprs.push(parse_qualified_column(&f.name, rel.as_str())?);
                        fields.push(f.clone());
                    }
                }
            }
        }
    }
    Ok(ProjectionBuild {
        exprs,
        schema: Schema { fields },
    })
}

fn qualified_field_belongs(field_name: &str, relation: &str) -> bool {
    field_name.starts_with(&format!("{relation}."))
}

fn parse_qualified_column(field_name: &str, relation: &str) -> Result<Expr> {
    let prefix = format!("{relation}.");
    if !field_name.starts_with(&prefix) {
        return Err(ArborError::Plan(format!(
            "expected column '{field_name}' to start with '{prefix}'"
        )));
    }
    let name = field_name[prefix.len()..].to_string();
    Ok(Expr::Column {
        name,
        relation: Some(relation.to_string()),
    })
}

fn build_aggregate_outputs(
    items: &[SelectItem],
    group_by: &[Expr],
    group_set: &std::collections::HashSet<Expr>,
    from_schema: &Schema,
) -> Result<(Vec<Expr>, Schema)> {
    let mut non_agg_selected = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(e) if !is_aggregate_sql_expr(e) => {
                non_agg_selected.push(convert_expr(e, from_schema)?);
            }
            SelectItem::ExprWithAlias { expr, .. } if !is_aggregate_sql_expr(expr) => {
                return Err(ArborError::Plan(
                    "non-aggregate SELECT items must be ungrouped columns without AS".into(),
                ));
            }
            _ => {}
        }
    }
    let nset: std::collections::HashSet<Expr> = non_agg_selected.iter().cloned().collect();
    if nset.len() != non_agg_selected.len() {
        return Err(ArborError::Plan(
            "duplicate GROUP BY column in SELECT".into(),
        ));
    }
    if nset != *group_set {
        return Err(ArborError::Plan(
            "SELECT must list exactly the GROUP BY columns (non-aggregate)".into(),
        ));
    }

    let mut out_fields: Vec<Field> = Vec::new();
    for g in group_by {
        out_fields.push(Field {
            name: default_output_name(g)?,
            data_type: infer_expr_type(g, from_schema)?,
        });
    }

    let mut aggr_exprs = Vec::new();
    for item in items {
        match item {
            SelectItem::UnnamedExpr(e) if is_aggregate_sql_expr(e) => {
                let ex = convert_expr(e, from_schema)?;
                let name = default_output_name(&ex)?;
                let dt = infer_expr_type(&ex, from_schema)?;
                out_fields.push(Field {
                    name,
                    data_type: dt,
                });
                aggr_exprs.push(ex);
            }
            SelectItem::ExprWithAlias { expr, alias } if is_aggregate_sql_expr(expr) => {
                let inner = convert_expr(expr, from_schema)?;
                let dt = infer_expr_type(&inner, from_schema)?;
                out_fields.push(Field {
                    name: alias.value.clone(),
                    data_type: dt,
                });
                aggr_exprs.push(Expr::Alias {
                    expr: Box::new(inner),
                    name: alias.value.clone(),
                });
            }
            SelectItem::UnnamedExpr(e) if !is_aggregate_sql_expr(e) => {}
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(ArborError::Plan(
                    "wildcard not allowed with GROUP BY".into(),
                ));
            }
            _ => {}
        }
    }
    if aggr_exprs.is_empty() {
        return Err(ArborError::Plan(
            "aggregation query requires at least one aggregate function".into(),
        ));
    }
    Ok((aggr_exprs, Schema { fields: out_fields }))
}

fn is_aggregate_sql_expr(e: &SqlExpr) -> bool {
    match e {
        SqlExpr::Function(f) => function_is_aggregate(f),
        SqlExpr::Nested(inner) | SqlExpr::Cast { expr: inner, .. } => is_aggregate_sql_expr(inner),
        _ => false,
    }
}

fn function_is_aggregate(f: &Function) -> bool {
    let name = function_name_upper(f);
    matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX")
}

fn function_name_upper(f: &Function) -> String {
    f.name
        .0
        .last()
        .map(|i| i.value.to_uppercase())
        .unwrap_or_default()
}

fn select_items_contain_aggregate(items: &[SelectItem]) -> Result<bool> {
    for item in items {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                if is_aggregate_sql_expr(e) {
                    return Ok(true);
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    Ok(false)
}

fn convert_order_by(order: &[OrderByExpr], schema: &Schema) -> Result<Vec<SortExpr>> {
    let mut out = Vec::new();
    for o in order {
        let asc = o.asc.unwrap_or(true);
        let ex = convert_expr(&o.expr, schema)?;
        out.push(SortExpr { expr: ex, asc });
    }
    Ok(out)
}

fn parse_limit(e: &SqlExpr) -> Result<usize> {
    match e {
        SqlExpr::Value(v) => match v {
            Value::Number(s, _) => s
                .parse::<usize>()
                .map_err(|_| ArborError::Plan(format!("invalid LIMIT value '{s}'"))),
            _ => Err(ArborError::Plan("LIMIT must be a numeric literal".into())),
        },
        _ => Err(ArborError::Plan("LIMIT must be a simple literal".into())),
    }
}

fn plan_table_with_joins(twj: &TableWithJoins, catalog: &Catalog) -> Result<(LogicalPlan, Schema)> {
    let join_chain = !twj.joins.is_empty();
    let (mut plan, mut schema) = plan_table_factor(&twj.relation, catalog, join_chain)?;
    for join in &twj.joins {
        let (right_plan, right_schema) = plan_table_factor(&join.relation, catalog, join_chain)?;
        let (join_type, on_expr) = join_operator_to_parts(&join.join_operator)?;
        let join_schema = merge_for_join(&schema, &right_schema)?;
        let on_pairs = flatten_join_quals(&on_expr, &join_schema)?;
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            on: on_pairs,
            join_type,
            schema: join_schema.clone(),
        };
        schema = join_schema;
    }
    Ok((plan, schema))
}

fn join_operator_to_parts(op: &JoinOperator) -> Result<(JoinType, SqlExpr)> {
    match op {
        JoinOperator::Inner(JoinConstraint::On(e)) => Ok((JoinType::Inner, e.clone())),
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => Ok((JoinType::Left, e.clone())),
        JoinOperator::Inner(_)
        | JoinOperator::LeftOuter(_)
        | JoinOperator::RightOuter(_)
        | JoinOperator::FullOuter(_)
        | JoinOperator::CrossJoin => Err(ArborError::Plan(
            "only INNER/LEFT JOIN ... ON supported".into(),
        )),
        _ => Err(ArborError::Plan("unsupported JOIN form".into())),
    }
}

fn merge_for_join(left: &Schema, right: &Schema) -> Result<Schema> {
    let mut used: std::collections::HashSet<String> =
        left.fields.iter().map(|f| f.name.clone()).collect();
    let mut fields = left.fields.clone();
    for rf in &right.fields {
        let mut name = rf.name.clone();
        if used.contains(&name) {
            name = format!("{}_dup", name);
            if used.contains(&name) {
                return Err(ArborError::Plan(
                    "duplicate column names in JOIN; rename in SQL".into(),
                ));
            }
        }
        used.insert(name.clone());
        fields.push(Field {
            name,
            data_type: rf.data_type.clone(),
        });
    }
    Ok(Schema { fields })
}

fn flatten_join_quals(expr: &SqlExpr, schema: &Schema) -> Result<Vec<(Expr, Expr)>> {
    let mut conjuncts = Vec::new();
    collect_and(expr, &mut conjuncts);
    let mut pairs = Vec::new();
    for c in conjuncts {
        match c {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                pairs.push((
                    convert_expr(left.as_ref(), schema)?,
                    convert_expr(right.as_ref(), schema)?,
                ));
            }
            _ => {
                return Err(ArborError::Plan(
                    "only equi-join AND conjunctions are supported".into(),
                ));
            }
        }
    }
    if pairs.is_empty() {
        return Err(ArborError::Plan(
            "JOIN requires at least one equality predicate".into(),
        ));
    }
    Ok(pairs)
}

fn collect_and(expr: &SqlExpr, out: &mut Vec<SqlExpr>) {
    match expr {
        SqlExpr::Nested(inner) => collect_and(inner.as_ref(), out),
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            collect_and(left.as_ref(), out);
            collect_and(right.as_ref(), out);
        }
        _ => out.push(expr.clone()),
    }
}

fn plan_table_factor(
    factor: &TableFactor,
    catalog: &Catalog,
    join_chain: bool,
) -> Result<(LogicalPlan, Schema)> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => {
            let table_key = object_name_last(&ObjectName(name.0.clone()));
            let base_schema = catalog
                .get(&table_key)
                .ok_or_else(|| ArborError::Plan(format!("unknown table '{table_key}'")))?;
            let logical_schema = if join_chain {
                let a = alias
                    .as_ref()
                    .map(|x| x.name.value.as_str())
                    .ok_or_else(|| {
                        ArborError::Plan("table alias is required in JOIN chains".into())
                    })?;
                prefix_schema(base_schema, a)
            } else {
                base_schema.clone()
            };
            let plan = LogicalPlan::Scan {
                table_name: table_key.to_string(),
                schema: logical_schema.clone(),
                projection: None,
            };
            Ok((plan, logical_schema))
        }
        _ => Err(ArborError::Plan(
            "unsupported table factor (subquery, TVF, etc.)".into(),
        )),
    }
}

fn prefix_schema(base: &Schema, alias: &str) -> Schema {
    Schema {
        fields: base
            .fields
            .iter()
            .map(|f| Field {
                name: format!("{alias}.{}", f.name),
                data_type: f.data_type.clone(),
            })
            .collect(),
    }
}

fn object_name_last(name: &ObjectName) -> String {
    name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

/// Converts a sqlparser expression into a planner [`Expr`] against `input_schema`.
pub fn convert_expr(sql_expr: &SqlExpr, input_schema: &Schema) -> Result<Expr> {
    match sql_expr {
        SqlExpr::Identifier(id) => resolve_unqualified_column(&id.value, input_schema),
        SqlExpr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return Err(ArborError::Plan(
                    "qualified identifiers must have one qualifier".into(),
                ));
            }
            let rel = parts[0].value.clone();
            let name = parts[1].value.clone();
            let qualified = format!("{rel}.{name}");
            if input_schema.field_by_name(&qualified).is_some() {
                return Ok(Expr::Column {
                    name,
                    relation: Some(rel),
                });
            }
            if input_schema.field_by_name(&name).is_some() {
                return Ok(Expr::Column {
                    name,
                    relation: Some(rel),
                });
            }
            Err(ArborError::Plan(format!("unknown column {rel}.{name}")))
        }
        SqlExpr::Value(v) => Ok(Expr::Literal(sql_value_to_scalar(v)?)),
        SqlExpr::UnaryOp { op, .. } => Err(ArborError::Plan(format!(
            "unsupported unary operator: {op}"
        ))),
        SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryExpr {
            left: Box::new(convert_expr(left, input_schema)?),
            op: map_binary_op(op)?,
            right: Box::new(convert_expr(right, input_schema)?),
        }),
        SqlExpr::Nested(inner) => convert_expr(inner, input_schema),
        SqlExpr::Function(f) => convert_sql_function(f, input_schema),
        SqlExpr::Cast { .. } => Err(ArborError::Plan("CAST is not supported in planner".into())),
        _ => Err(ArborError::Plan(format!(
            "unsupported expression in planner: {sql_expr:?}"
        ))),
    }
}

fn convert_sql_function(f: &Function, input_schema: &Schema) -> Result<Expr> {
    let upper = function_name_upper(f);
    let func = match upper.as_str() {
        "SUM" => AggFunc::Sum,
        "COUNT" => AggFunc::Count,
        "AVG" => AggFunc::Avg,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        _ => {
            return Err(ArborError::Plan(format!("unsupported function '{upper}'")));
        }
    };
    let arg = match f.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => Expr::Wildcard,
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => convert_expr(e, input_schema)?,
        _ => {
            return Err(ArborError::Plan(
                "only single-argument aggregates (or COUNT(*)) supported".into(),
            ));
        }
    };
    Ok(Expr::AggregateFunc {
        func,
        arg: Box::new(arg),
    })
}

fn resolve_unqualified_column(name: &str, input_schema: &Schema) -> Result<Expr> {
    if let Some((_, _)) = input_schema.field_by_name(name) {
        return Ok(Expr::Column {
            name: name.to_string(),
            relation: None,
        });
    }
    let suffix_matches: Vec<usize> = input_schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name == name || f.name.ends_with(&format!(".{name}")))
        .map(|(i, _)| i)
        .collect();
    if suffix_matches.len() == 1 {
        let f = &input_schema.fields[suffix_matches[0]];
        if let Some(dot) = f.name.find('.') {
            let rel = f.name[..dot].to_string();
            let base = f.name[dot + 1..].to_string();
            return Ok(Expr::Column {
                name: base,
                relation: Some(rel),
            });
        }
        return Ok(Expr::Column {
            name: name.to_string(),
            relation: None,
        });
    }
    if suffix_matches.is_empty() {
        return Err(ArborError::Plan(format!("unknown column '{name}'")));
    }
    Err(ArborError::Plan(format!("ambiguous column '{name}'")))
}

fn sql_value_to_scalar(v: &Value) -> Result<ScalarValue> {
    match v {
        Value::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                return Ok(ScalarValue::Int64(i));
            }
            if let Ok(f) = s.parse::<f64>() {
                return Ok(ScalarValue::Float64(f));
            }
            Err(ArborError::Plan(format!("invalid number literal '{s}'")))
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(ScalarValue::Utf8(s.clone()))
        }
        Value::Boolean(b) => Ok(ScalarValue::Boolean(*b)),
        Value::Null => Ok(ScalarValue::Null),
        _ => Err(ArborError::Plan(format!("unsupported literal: {v}"))),
    }
}

fn map_binary_op(op: &BinaryOperator) -> Result<BinaryOp> {
    match op {
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => Ok(BinaryOp::Neq),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        BinaryOperator::Plus => Ok(BinaryOp::Plus),
        BinaryOperator::Minus => Ok(BinaryOp::Minus),
        BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        BinaryOperator::Divide => Ok(BinaryOp::Div),
        _ => Err(ArborError::Plan(format!(
            "unsupported binary operator: {op}"
        ))),
    }
}

fn default_output_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Column {
            name,
            relation: Some(r),
        } => Ok(format!("{r}.{name}")),
        Expr::Column {
            name,
            relation: None,
        } => Ok(name.clone()),
        Expr::Alias { name, .. } => Ok(name.clone()),
        Expr::AggregateFunc { func, .. } => Ok(match func {
            AggFunc::Sum => "sum",
            AggFunc::Count => "count",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
        .into()),
        _ => Ok("expr".into()),
    }
}

fn infer_expr_type(e: &Expr, input_schema: &Schema) -> Result<DataType> {
    match e {
        Expr::Column { name, relation } => {
            let key = match relation {
                Some(r) => format!("{r}.{name}"),
                None => name.clone(),
            };
            let idx = input_schema
                .index_of(&key)
                .or_else(|_| input_schema.index_of(name))?;
            Ok(input_schema.fields[idx].data_type.clone())
        }
        Expr::Literal(s) => Ok(match s {
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Null => DataType::Null,
        }),
        Expr::AggregateFunc { func, arg } => Ok(match func {
            AggFunc::Count => DataType::Int64,
            AggFunc::Avg => DataType::Float64,
            AggFunc::Sum => {
                let arg_t = infer_expr_type(arg.as_ref(), input_schema)?;
                match arg_t {
                    DataType::Int64 => DataType::Int64,
                    _ => DataType::Float64,
                }
            }
            AggFunc::Min | AggFunc::Max => infer_expr_type(arg.as_ref(), input_schema)?,
        }),
        Expr::Alias { expr, .. } => infer_expr_type(expr, input_schema),
        Expr::BinaryExpr { left, op, right } => match op {
            BinaryOp::Eq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Gt
            | BinaryOp::LtEq
            | BinaryOp::GtEq
            | BinaryOp::And
            | BinaryOp::Or => Ok(DataType::Boolean),
            _ => {
                let lt = infer_expr_type(left, input_schema)?;
                let _rt = infer_expr_type(right, input_schema)?;
                Ok(lt)
            }
        },
        Expr::Wildcard => Ok(DataType::Int64),
    }
}

#[cfg(test)]
mod tests {
    use super::plan_query;
    use crate::parser::parse_sql;
    use crate::planner::LogicalPlan;
    use crate::types::{Catalog, Field, Schema};
    use arrow::datatypes::DataType;

    fn test_catalog() -> Catalog {
        let schema = Schema {
            fields: vec![
                Field {
                    name: "a".into(),
                    data_type: DataType::Int64,
                },
                Field {
                    name: "b".into(),
                    data_type: DataType::Int64,
                },
            ],
        };
        let mut c = Catalog::new();
        c.insert("t".into(), schema);
        c
    }

    #[test]
    fn plan_simple_select_where() {
        let cat = test_catalog();
        let stmt = parse_sql("SELECT a, b FROM t WHERE a > 10").unwrap();
        let plan = plan_query(&stmt, &cat).unwrap();
        assert!(matches!(plan, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = &plan {
            match input.as_ref() {
                LogicalPlan::Projection { input: inner, .. } => {
                    assert!(matches!(inner.as_ref(), LogicalPlan::Scan { .. }));
                }
                _ => panic!("expected projection under filter"),
            }
        }
    }

    #[test]
    fn plan_aggregate_group_by() {
        let mut cat = Catalog::new();
        cat.insert(
            "t".into(),
            Schema {
                fields: vec![Field {
                    name: "city".into(),
                    data_type: DataType::Utf8,
                }],
            },
        );
        let stmt = parse_sql("SELECT city, COUNT(*) FROM t GROUP BY city").unwrap();
        let plan = plan_query(&stmt, &cat).unwrap();
        assert!(matches!(plan, LogicalPlan::Aggregate { .. }));
    }
}
