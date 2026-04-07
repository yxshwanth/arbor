//! Hash-based grouped aggregation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::{concat_batches, max, min, sum};
use arrow::datatypes::{DataType, SchemaRef};

use crate::error::{ArborError, Result};
use crate::executor::{evaluate_expr, PhysicalPlan, BATCH_SIZE};
use crate::planner::{AggFunc, Expr};
use crate::types::ScalarValue;

/// Drains input, aggregates in a hash map, then emits one or more output batches.
pub struct HashAggregateExec {
    child: Box<dyn PhysicalPlan>,
    group_by: Vec<Expr>,
    aggr_exprs: Vec<Expr>,
    output_schema: SchemaRef,
    out_batches: Option<Vec<RecordBatch>>,
    next_i: usize,
}

impl HashAggregateExec {
    /// Constructs a hash aggregate operator as a boxed [`PhysicalPlan`].
    #[allow(clippy::new_ret_no_self)] // ergonomic constructor for trait object
    pub fn new(
        child: Box<dyn PhysicalPlan>,
        group_by: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        types_schema: crate::types::Schema,
    ) -> Result<Box<dyn PhysicalPlan>> {
        let output_schema: SchemaRef = Arc::new(types_schema.into());
        Ok(Box::new(HashAggregateExec {
            child,
            group_by,
            aggr_exprs,
            output_schema,
            out_batches: None,
            next_i: 0,
        }))
    }
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.out_batches.is_none() {
            let mut batches = Vec::new();
            while let Some(b) = self.child.next_batch()? {
                batches.push(b);
            }
            self.out_batches = Some(if batches.is_empty() {
                vec![]
            } else if self.group_by.is_empty() {
                vec![eval_full_table_agg(
                    &batches,
                    &self.aggr_exprs,
                    &self.output_schema,
                )?]
            } else {
                build_grouped_batches(
                    &batches,
                    &self.group_by,
                    &self.aggr_exprs,
                    &self.output_schema,
                )?
            });
        }
        let bs = self
            .out_batches
            .as_ref()
            .ok_or_else(|| ArborError::Execution("aggregate internal state missing".into()))?;
        if self.next_i >= bs.len() {
            return Ok(None);
        }
        let b = bs[self.next_i].clone();
        self.next_i += 1;
        Ok(Some(b))
    }
}

fn scalar_at(arr: &ArrayRef, row: usize) -> Result<ScalarValue> {
    if !arr.is_valid(row) {
        return Ok(ScalarValue::Null);
    }
    Ok(if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        ScalarValue::Int64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        ScalarValue::Float64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        ScalarValue::Utf8(a.value(row).to_string())
    } else if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        ScalarValue::Boolean(a.value(row))
    } else {
        return Err(ArborError::Execution(format!(
            "unsupported array type for group key: {}",
            arr.data_type()
        )));
    })
}

enum Acc {
    Count(i64),
    SumF64 { s: f64, n: i64 },
    SumI64(i64),
    Min(ScalarValue),
    Max(ScalarValue),
    Avg { sum: f64, n: i64 },
}

impl Acc {
    fn update(&mut self, v: ScalarValue) -> Result<()> {
        match (self, v) {
            (Acc::Count(c), _) => *c += 1,
            (Acc::SumI64(s), ScalarValue::Int64(x)) => *s += x,
            (Acc::SumF64 { s, n }, ScalarValue::Int64(x)) => {
                *s += x as f64;
                *n += 1;
            }
            (Acc::SumF64 { s, n }, ScalarValue::Float64(x)) => {
                *s += x;
                *n += 1;
            }
            (Acc::Avg { sum, n }, ScalarValue::Int64(x)) => {
                *sum += x as f64;
                *n += 1;
            }
            (Acc::Avg { sum, n }, ScalarValue::Float64(x)) => {
                *sum += x;
                *n += 1;
            }
            (Acc::Min(m), v) => {
                if *m == ScalarValue::Null
                    || compare_key_slices(std::slice::from_ref(&v), std::slice::from_ref(m))
                        == std::cmp::Ordering::Less
                {
                    *m = v;
                }
            }
            (Acc::Max(m), v) => {
                if *m == ScalarValue::Null
                    || compare_key_slices(std::slice::from_ref(&v), std::slice::from_ref(m))
                        == std::cmp::Ordering::Greater
                {
                    *m = v;
                }
            }
            _ => {
                return Err(ArborError::Execution("accumulator type mismatch".into()));
            }
        }
        Ok(())
    }

    fn finish(&self) -> ScalarValue {
        match self {
            Acc::Count(c) => ScalarValue::Int64(*c),
            Acc::SumI64(s) => ScalarValue::Int64(*s),
            Acc::SumF64 { s, .. } => ScalarValue::Float64(*s),
            Acc::Min(m) | Acc::Max(m) => m.clone(),
            Acc::Avg { sum, n } if *n > 0 => ScalarValue::Float64(*sum / *n as f64),
            Acc::Avg { .. } => ScalarValue::Null,
        }
    }
}

fn acc_for_func(func: AggFunc, arg_sample: &ScalarValue) -> Result<Acc> {
    Ok(match func {
        AggFunc::Count => Acc::Count(0),
        AggFunc::Sum => match arg_sample {
            ScalarValue::Int64(_) => Acc::SumI64(0),
            _ => Acc::SumF64 { s: 0.0, n: 0 },
        },
        AggFunc::Avg => Acc::Avg { sum: 0.0, n: 0 },
        AggFunc::Min => Acc::Min(ScalarValue::Null),
        AggFunc::Max => Acc::Max(ScalarValue::Null),
    })
}

fn agg_arg_expr(e: &Expr) -> Result<&Expr> {
    match e {
        Expr::Alias { expr, .. } => agg_arg_expr(expr),
        Expr::AggregateFunc { arg, .. } => Ok(arg.as_ref()),
        _ => Err(ArborError::Execution("expected aggregate expr".into())),
    }
}

fn agg_func(e: &Expr) -> Result<AggFunc> {
    match e {
        Expr::Alias { expr, .. } => agg_func(expr),
        Expr::AggregateFunc { func, .. } => Ok(*func),
        _ => Err(ArborError::Execution("expected aggregate func".into())),
    }
}

fn build_grouped_batches(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggr_exprs: &[Expr],
    out_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let schema = batches[0].schema();
    let all = concat_batches(&schema, batches).map_err(ArborError::from)?;
    let mut map: HashMap<Vec<ScalarValue>, Vec<Acc>> = HashMap::new();
    let n = all.num_rows();
    for row in 0..n {
        let mut key = Vec::new();
        for g in group_by {
            let col = evaluate_expr(g, &all)?;
            key.push(scalar_at(&col, row)?);
        }
        use std::collections::hash_map::Entry;
        let entry = match map.entry(key) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let init: Vec<Acc> = aggr_exprs
                    .iter()
                    .map(|e| {
                        let func = agg_func(e)?;
                        let arg_e = agg_arg_expr(e)?;
                        let sample = if matches!(arg_e, Expr::Wildcard) {
                            ScalarValue::Int64(0)
                        } else {
                            let c = evaluate_expr(arg_e, &all)?;
                            scalar_at(&c, row)?
                        };
                        acc_for_func(func, &sample)
                    })
                    .collect::<Result<_>>()?;
                v.insert(init)
            }
        };
        for (i, ae) in aggr_exprs.iter().enumerate() {
            let func = agg_func(ae)?;
            let arg_e = agg_arg_expr(ae)?;
            let val = if matches!(func, AggFunc::Count) && matches!(arg_e, Expr::Wildcard) {
                ScalarValue::Int64(1)
            } else {
                let c = evaluate_expr(arg_e, &all)?;
                scalar_at(&c, row)?
            };
            entry[i].update(val)?;
        }
    }
    emit_batches_from_map(map, group_by.len(), out_schema)
}

fn emit_batches_from_map(
    map: HashMap<Vec<ScalarValue>, Vec<Acc>>,
    n_groups: usize,
    out_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut keys: Vec<_> = map.into_iter().collect();
    keys.sort_by(|(ka, _), (kb, _)| compare_key_slices(ka, kb));
    let ncols = out_schema.fields().len();
    let mut col_builders: Vec<Vec<ScalarValue>> = (0..ncols).map(|_| Vec::new()).collect();
    for (k, accs) in keys {
        for (i, sk) in k.into_iter().enumerate() {
            col_builders[i].push(sk);
        }
        for (j, acc) in accs.iter().enumerate() {
            col_builders[n_groups + j].push(acc.finish());
        }
    }
    batch_from_scalar_columns(&col_builders, out_schema, BATCH_SIZE)
}

fn batch_from_scalar_columns(
    cols: &[Vec<ScalarValue>],
    out_schema: &SchemaRef,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let n = cols.first().map(|c| c.len()).unwrap_or(0);
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < n {
        let take = (n - offset).min(batch_size);
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (ci, f) in out_schema.fields().iter().enumerate() {
            let slice = &cols[ci][offset..offset + take];
            arrays.push(scalars_to_array(slice, f.data_type())?);
        }
        out.push(RecordBatch::try_new(out_schema.clone(), arrays).map_err(ArborError::from)?);
        offset += take;
    }
    Ok(out)
}

fn scalars_to_array(vals: &[ScalarValue], dt: &DataType) -> Result<ArrayRef> {
    use ScalarValue::*;
    Ok(match dt {
        DataType::Int64 => {
            let mut b = Int64Array::builder(vals.len());
            for v in vals {
                match v {
                    Int64(i) => b.append_value(*i),
                    Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Array::builder(vals.len());
            for v in vals {
                match v {
                    Float64(f) => b.append_value(*f),
                    Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            use arrow::array::StringBuilder;
            let mut b = StringBuilder::new();
            for v in vals {
                match v {
                    Utf8(s) => b.append_value(s),
                    Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = arrow::array::BooleanBuilder::new();
            for v in vals {
                match v {
                    Boolean(x) => b.append_value(*x),
                    Null => b.append_null(),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            return Err(ArborError::Execution(format!(
                "unsupported output type {dt}"
            )));
        }
    })
}

fn eval_full_table_agg(
    batches: &[RecordBatch],
    aggr_exprs: &[Expr],
    out_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let schema = batches[0].schema();
    let all = concat_batches(&schema, batches).map_err(ArborError::from)?;
    let mut scalars = Vec::new();
    for ae in aggr_exprs {
        let func = agg_func(ae)?;
        let arg_e = agg_arg_expr(ae)?;
        match func {
            AggFunc::Count if matches!(arg_e, Expr::Wildcard) => {
                scalars.push(ScalarValue::Int64(all.num_rows() as i64));
            }
            AggFunc::Count => {
                let c = evaluate_expr(arg_e, &all)?;
                scalars.push(ScalarValue::Int64(c.len() as i64 - c.null_count() as i64));
            }
            AggFunc::Sum => {
                let c = evaluate_expr(arg_e, &all)?;
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    let s = sum(a).unwrap_or(0);
                    scalars.push(ScalarValue::Int64(s));
                } else if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                    let s = sum(a).unwrap_or(0.0);
                    scalars.push(ScalarValue::Float64(s));
                } else {
                    return Err(ArborError::Execution("SUM type".into()));
                }
            }
            AggFunc::Min => {
                let c = evaluate_expr(arg_e, &all)?;
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    let m = min(a);
                    scalars.push(match m {
                        None => ScalarValue::Null,
                        Some(v) => ScalarValue::Int64(v),
                    });
                } else if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                    let m = min(a);
                    scalars.push(match m {
                        None => ScalarValue::Null,
                        Some(v) => ScalarValue::Float64(v),
                    });
                } else {
                    return Err(ArborError::Execution("MIN type".into()));
                }
            }
            AggFunc::Max => {
                let c = evaluate_expr(arg_e, &all)?;
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    let m = max(a);
                    scalars.push(match m {
                        None => ScalarValue::Null,
                        Some(v) => ScalarValue::Int64(v),
                    });
                } else if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                    let m = max(a);
                    scalars.push(match m {
                        None => ScalarValue::Null,
                        Some(v) => ScalarValue::Float64(v),
                    });
                } else {
                    return Err(ArborError::Execution("MAX type".into()));
                }
            }
            AggFunc::Avg => {
                let c = evaluate_expr(arg_e, &all)?;
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    let s = sum(a).unwrap_or(0) as f64;
                    let n = (a.len() - a.null_count()) as f64;
                    scalars.push(ScalarValue::Float64(if n > 0.0 { s / n } else { 0.0 }));
                } else if let Some(a) = c.as_any().downcast_ref::<Float64Array>() {
                    let s = sum(a).unwrap_or(0.0);
                    let n = (a.len() - a.null_count()) as f64;
                    scalars.push(ScalarValue::Float64(if n > 0.0 { s / n } else { 0.0 }));
                } else {
                    return Err(ArborError::Execution("AVG type".into()));
                }
            }
        }
    }
    let cols: Vec<ArrayRef> = out_schema
        .fields()
        .iter()
        .zip(scalars.iter())
        .map(|(f, s)| scalars_to_array(std::slice::from_ref(s), f.data_type()))
        .collect::<Result<_>>()?;
    RecordBatch::try_new(out_schema.clone(), cols).map_err(ArborError::from)
}

fn compare_key_slices(a: &[ScalarValue], b: &[ScalarValue]) -> std::cmp::Ordering {
    use ScalarValue::*;
    let mut i = 0;
    loop {
        match (a.get(i), b.get(i)) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => match (x, y) {
                (Null, Null) => i += 1,
                (Null, _) => return std::cmp::Ordering::Less,
                (_, Null) => return std::cmp::Ordering::Greater,
                (Int64(p), Int64(q)) => {
                    let o = p.cmp(q);
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                    i += 1;
                }
                (Float64(p), Float64(q)) => {
                    let o = p.partial_cmp(q).unwrap_or(std::cmp::Ordering::Equal);
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                    i += 1;
                }
                (Utf8(p), Utf8(q)) => {
                    let o = p.cmp(q);
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                    i += 1;
                }
                (Boolean(p), Boolean(q)) => {
                    let o = p.cmp(q);
                    if o != std::cmp::Ordering::Equal {
                        return o;
                    }
                    i += 1;
                }
                _ => return std::cmp::Ordering::Equal,
            },
        }
    }
}
