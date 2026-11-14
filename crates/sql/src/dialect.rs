//! The dialect transforms parse with: sqlparser's Postgres dialect, but with
//! `IS [NOT] DISTINCT FROM` binding as tightly as Postgres and DuckDB bind
//! it. sqlparser 0.62 parses its right side as a whole expression, so
//! `a IS DISTINCT FROM b OR c` became `a IS DISTINCT FROM (b OR c)`.

use std::any::TypeId;

use datafusion_sql::sqlparser::ast::Expr;
use datafusion_sql::sqlparser::dialect::{Dialect, PostgreSqlDialect, Precedence};
use datafusion_sql::sqlparser::keywords::Keyword;
use datafusion_sql::sqlparser::parser::{Parser, ParserError};

#[derive(Debug)]
pub(crate) struct Postgres;

const PG: PostgreSqlDialect = PostgreSqlDialect {};

impl Dialect for Postgres {
    /// The parser's checks for Postgres hold for this dialect too.
    fn dialect(&self) -> TypeId {
        TypeId::of::<PostgreSqlDialect>()
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        let not = if parser.parse_keywords(&[Keyword::IS, Keyword::DISTINCT, Keyword::FROM]) {
            false
        } else if parser.parse_keywords(&[
            Keyword::IS,
            Keyword::NOT,
            Keyword::DISTINCT,
            Keyword::FROM,
        ]) {
            true
        } else {
            return None;
        };
        let (left, right) = match parser.parse_subexpr(precedence) {
            Ok(right) => (Box::new(expr.clone()), Box::new(right)),
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(if not {
            Expr::IsNotDistinctFrom(left, right)
        } else {
            Expr::IsDistinctFrom(left, right)
        }))
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PG.identifier_quote_style(identifier)
    }
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        PG.is_delimited_identifier_start(ch)
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        PG.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        PG.is_identifier_part(ch)
    }
    fn supports_unicode_string_literal(&self) -> bool {
        PG.supports_unicode_string_literal()
    }
    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        PG.is_reserved_for_identifier(kw)
    }
    fn is_custom_operator_part(&self, ch: char) -> bool {
        PG.is_custom_operator_part(ch)
    }
    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        PG.get_next_precedence(parser)
    }
    fn supports_filter_during_aggregation(&self) -> bool {
        PG.supports_filter_during_aggregation()
    }
    fn supports_group_by_expr(&self) -> bool {
        PG.supports_group_by_expr()
    }
    fn prec_value(&self, prec: Precedence) -> u8 {
        PG.prec_value(prec)
    }
    fn allow_extract_custom(&self) -> bool {
        PG.allow_extract_custom()
    }
    fn allow_extract_single_quotes(&self) -> bool {
        PG.allow_extract_single_quotes()
    }
    fn supports_create_index_with_clause(&self) -> bool {
        PG.supports_create_index_with_clause()
    }
    fn supports_explain_with_utility_options(&self) -> bool {
        PG.supports_explain_with_utility_options()
    }
    fn supports_listen_notify(&self) -> bool {
        PG.supports_listen_notify()
    }
    fn supports_factorial_operator(&self) -> bool {
        PG.supports_factorial_operator()
    }
    fn supports_bitwise_shift_operators(&self) -> bool {
        PG.supports_bitwise_shift_operators()
    }
    fn supports_comment_on(&self) -> bool {
        PG.supports_comment_on()
    }
    fn supports_load_extension(&self) -> bool {
        PG.supports_load_extension()
    }
    fn supports_named_fn_args_with_colon_operator(&self) -> bool {
        PG.supports_named_fn_args_with_colon_operator()
    }
    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        PG.supports_named_fn_args_with_expr_name()
    }
    fn supports_empty_projections(&self) -> bool {
        PG.supports_empty_projections()
    }
    fn supports_nested_comments(&self) -> bool {
        PG.supports_nested_comments()
    }
    fn supports_string_escape_constant(&self) -> bool {
        PG.supports_string_escape_constant()
    }
    fn supports_numeric_literal_underscores(&self) -> bool {
        PG.supports_numeric_literal_underscores()
    }
    fn supports_array_typedef_with_brackets(&self) -> bool {
        PG.supports_array_typedef_with_brackets()
    }
    fn supports_geometric_types(&self) -> bool {
        PG.supports_geometric_types()
    }
    fn supports_set_names(&self) -> bool {
        PG.supports_set_names()
    }
    fn supports_alter_column_type_using(&self) -> bool {
        PG.supports_alter_column_type_using()
    }
    fn supports_notnull_operator(&self) -> bool {
        PG.supports_notnull_operator()
    }
    fn supports_interval_options(&self) -> bool {
        PG.supports_interval_options()
    }
    fn supports_insert_table_alias(&self) -> bool {
        PG.supports_insert_table_alias()
    }
    fn supports_create_table_like_parenthesized(&self) -> bool {
        PG.supports_create_table_like_parenthesized()
    }
    fn supports_select_wildcard_with_alias(&self) -> bool {
        PG.supports_select_wildcard_with_alias()
    }
    fn supports_comma_separated_trim(&self) -> bool {
        PG.supports_comma_separated_trim()
    }
    fn supports_xml_expressions(&self) -> bool {
        PG.supports_xml_expressions()
    }
    fn supports_comment_optimizer_hint(&self) -> bool {
        PG.supports_comment_optimizer_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use datafusion_sql::sqlparser::ast::{Statement, visit_expressions_mut};

    use super::*;

    fn parse(dialect: &dyn Dialect, sql: &str) -> Statement {
        let mut s = Parser::parse_sql(dialect, &format!("SELECT {sql}"))
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(s.len(), 1, "{sql}");
        s.remove(0)
    }

    /// The statement with every parenthesised expression replaced by what's
    /// inside the parentheses.
    fn bare(mut s: Statement) -> Statement {
        let _ = visit_expressions_mut(&mut s, |e| {
            while let Expr::Nested(inner) = e {
                *e = (**inner).clone();
            }
            ControlFlow::<()>::Continue(())
        });
        s
    }

    /// Each expression parses as its parenthesised form does, which is how
    /// Postgres and DuckDB read it.
    #[test]
    fn operators_bind_as_postgres_binds_them() {
        let cases = [
            ("a IS DISTINCT FROM b OR c", "(a IS DISTINCT FROM b) OR c"),
            (
                "a IS NOT DISTINCT FROM b AND c",
                "(a IS NOT DISTINCT FROM b) AND c",
            ),
            (
                "x AND a IS DISTINCT FROM b = c OR d AND e",
                "(x AND (a IS DISTINCT FROM (b = c))) OR (d AND e)",
            ),
            ("a = b IS DISTINCT FROM c", "(a = b) IS DISTINCT FROM c"),
            ("a IS DISTINCT FROM b + c", "a IS DISTINCT FROM (b + c)"),
            (
                "a IS DISTINCT FROM b LIKE c",
                "a IS DISTINCT FROM (b LIKE c)",
            ),
            (
                "a IS DISTINCT FROM b BETWEEN c AND d AND e",
                "(a IS DISTINCT FROM (b BETWEEN c AND d)) AND e",
            ),
            (
                "a IS DISTINCT FROM b IN (c) OR d",
                "(a IS DISTINCT FROM (b IN (c))) OR d",
            ),
            ("NOT a IS DISTINCT FROM b", "NOT (a IS DISTINCT FROM b)"),
            (
                "a IS DISTINCT FROM NOT b AND c",
                "(a IS DISTINCT FROM (NOT b)) AND c",
            ),
            (
                "a IS NOT DISTINCT FROM b::INT OR c",
                "(a IS NOT DISTINCT FROM (b::INT)) OR c",
            ),
            (
                "CASE WHEN a IS DISTINCT FROM b OR c THEN d END",
                "CASE WHEN (a IS DISTINCT FROM b) OR c THEN d END",
            ),
            (
                "f(a IS NOT DISTINCT FROM b AND c, d)",
                "f((a IS NOT DISTINCT FROM b) AND c, d)",
            ),
            ("a IS NULL OR b", "(a IS NULL) OR b"),
            ("NOT a IS NULL", "NOT (a IS NULL)"),
            ("a = b IS NULL", "(a = b) IS NULL"),
            ("a = b IS NOT TRUE AND c", "((a = b) IS NOT TRUE) AND c"),
            ("NOT a IS FALSE OR b", "(NOT (a IS FALSE)) OR b"),
            ("a IS UNKNOWN OR b", "(a IS UNKNOWN) OR b"),
            ("a BETWEEN b AND c AND d", "(a BETWEEN b AND c) AND d"),
            ("a NOT BETWEEN b AND c OR d", "(a NOT BETWEEN b AND c) OR d"),
            ("a BETWEEN b AND c = d", "(a BETWEEN b AND c) = d"),
            ("a BETWEEN b AND c + d", "a BETWEEN b AND (c + d)"),
            ("a IN (b, c) = d", "(a IN (b, c)) = d"),
            ("a NOT IN (b) AND c", "(a NOT IN (b)) AND c"),
            ("a LIKE b = c", "(a LIKE b) = c"),
            ("a = b LIKE c", "a = (b LIKE c)"),
            ("a LIKE b || c", "a LIKE (b || c)"),
            ("a NOT ILIKE b OR c", "(a NOT ILIKE b) OR c"),
            ("a SIMILAR TO b OR c", "(a SIMILAR TO b) OR c"),
            ("a SIMILAR TO b = c", "(a SIMILAR TO b) = c"),
            ("a AT TIME ZONE b + c", "(a AT TIME ZONE b) + c"),
            ("a AT TIME ZONE b = c", "(a AT TIME ZONE b) = c"),
        ];
        for (sql, explicit) in cases {
            assert_eq!(
                bare(parse(&Postgres, sql)),
                bare(parse(&Postgres, explicit)),
                "{sql}"
            );
        }
    }

    /// A right side in parentheses stays whole.
    #[test]
    fn a_right_side_in_parentheses_stays_whole() {
        assert_ne!(
            bare(parse(&Postgres, "a IS DISTINCT FROM (b OR c)")),
            bare(parse(&Postgres, "a IS DISTINCT FROM b OR c"))
        );
    }

    /// Fails once sqlparser parses `IS [NOT] DISTINCT FROM` right, when this
    /// dialect should give way to its Postgres dialect.
    #[test]
    fn sqlparser_still_takes_and_and_or_into_the_right_side() {
        assert_eq!(
            bare(parse(&PG, "a IS DISTINCT FROM b OR c")),
            bare(parse(&PG, "a IS DISTINCT FROM (b OR c)")),
            "sqlparser's Postgres dialect now parses IS DISTINCT FROM as Postgres does: parse with it, and remove dialect.rs"
        );
    }
}
