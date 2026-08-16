//! Arbor CLI: run SQL over Parquet files in `data/`.

use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use arrow::util::pretty;

use arbor::error::{ArborError, Result};
use arbor::types::Catalog;
use arbor::{executor, optimizer, parser, planner, storage};

fn print_plan(title: &str, plan: &planner::LogicalPlan) {
    println!("=== {title} ===");
    println!("{plan}");
}

fn run_pipeline(
    sql: &str,
    catalog: &Catalog,
    data_dir: &Path,
    use_optimizer: bool,
) -> Result<Vec<arrow::array::RecordBatch>> {
    let stmt = parser::parse_sql(sql)?;
    let plan = planner::plan_query(&stmt, catalog)?;
    let optimized = if use_optimizer {
        optimizer::optimize(plan)?
    } else {
        plan
    };
    let mut phys = executor::create_physical_plan(&optimized, data_dir)?;
    executor::collect(&mut *phys)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let explain = args.iter().any(|a| a == "--explain");
    let sql_args: Vec<&str> = args
        .iter()
        .filter(|a| *a != "--explain")
        .map(|s| s.as_str())
        .collect();

    let data_dir = Path::new("data");
    let catalog = storage::build_catalog(data_dir)?;

    if !sql_args.is_empty() {
        let sql = sql_args.join(" ");
        if explain {
            let stmt = parser::parse_sql(&sql)?;
            let plan = planner::plan_query(&stmt, &catalog)?;
            print_plan("logical (before optimize)", &plan);
            let opt = optimizer::optimize(plan.clone())?;
            print_plan("logical (after optimize)", &opt);
            return Ok(());
        }
        let start = Instant::now();
        let batches = run_pipeline(&sql, &catalog, data_dir, true)?;
        pretty::print_batches(&batches)?;
        eprintln!("elapsed: {:?}", start.elapsed());
        return Ok(());
    }

    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("> ");
        io::stdout()
            .flush()
            .map_err(|e| ArborError::Execution(e.to_string()))?;
        line.clear();
        let n = stdin
            .read_line(&mut line)
            .map_err(|e| ArborError::Execution(e.to_string()))?;
        if n == 0 {
            break;
        }
        let sql = line.trim();
        if sql.is_empty() {
            continue;
        }
        if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
            break;
        }
        let start = Instant::now();
        match run_pipeline(sql, &catalog, data_dir, true) {
            Ok(batches) => {
                let _ = pretty::print_batches(&batches);
                eprintln!("elapsed: {:?}", start.elapsed());
            }
            Err(e) => eprintln!("error: {e}"),
        }
    }
    Ok(())
}
