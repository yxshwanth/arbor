//! Parse SQL strings into sqlparser statements (SELECT only).

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{ArborError, Result};

/// Parses `sql` and returns the first statement if it is a `SELECT` query.
pub fn parse_sql(sql: &str) -> Result<Statement> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(crate::error::ArborError::from)?;
    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| ArborError::Parse("no statements in SQL".to_string()))?;
    match stmt {
        Statement::Query(_) => Ok(stmt),
        _ => Err(ArborError::Parse(
            "Only SELECT queries are supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_sql;
    use crate::error::ArborError;
    use sqlparser::ast::Statement;

    #[test]
    fn parses_valid_select() {
        let stmt = parse_sql("SELECT a, b FROM t").expect("parse");
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn insert_errors() {
        let err = parse_sql("INSERT INTO t VALUES (1)").unwrap_err();
        match err {
            ArborError::Parse(msg) => assert!(msg.contains("Only SELECT")),
            _ => panic!("expected Parse error"),
        }
    }

    #[test]
    fn empty_string_errors() {
        assert!(parse_sql("").is_err());
        assert!(parse_sql("   ").is_err());
    }
}
