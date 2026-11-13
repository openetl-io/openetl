//! A transform's statements: parsed once, then planned against a catalog as
//! often as the streams' schemas change.

use std::collections::{BTreeSet, HashSet};
use std::ops::ControlFlow;

use datafusion_expr::LogicalPlan;
use datafusion_sql::sqlparser::ast::{self, Spanned};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::Span;

use crate::catalog::written;
use crate::functions::FUNCTIONS;
use crate::plan::{Operator, Plan, Statement, View};
use crate::{Catalog, Class, Error, ErrorKind, Errors, Note, Position, bind, lower};

/// A transform's SQL, parsed.
#[derive(Debug)]
pub struct Sql {
    items: Vec<Item>,
}

#[derive(Debug)]
enum Item {
    View {
        /// As SQL folds it: an unquoted name in lower case.
        name: String,
        ident: ast::Ident,
        query: ast::Query,
    },
    Insert {
        at: Position,
        target: Vec<ast::Ident>,
        query: ast::Query,
    },
}

/// Plans a transform's SQL against the streams it reads.
pub fn plan(sql: &str, catalog: &Catalog) -> Result<Plan, Errors> {
    Sql::parse(sql)?.plan(catalog)
}

impl Sql {
    /// Parses `INSERT INTO … SELECT` and `CREATE VIEW … AS SELECT`
    /// statements, separated by semicolons. Names aren't resolved until
    /// [`Sql::plan`].
    pub fn parse(text: &str) -> Result<Self, Errors> {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, text).map_err(syntax)?;
        let mut items = Vec::with_capacity(parsed.len());
        let mut errors = Vec::new();
        for s in parsed {
            match item(s) {
                Ok(i) => items.push(i),
                Err(e) => errors.push(e),
            }
        }
        let mut views: Vec<&ast::Ident> = Vec::new();
        for i in &items {
            if let Item::View { name, ident, .. } = i {
                if views.iter().any(|v| same(&fold(v), name)) {
                    errors.push(Error::at(
                        ErrorKind::Name,
                        pos(ident.span),
                        format!("there's already a view {name}"),
                    ));
                }
                views.push(ident);
            }
        }
        if errors.is_empty() && !items.iter().any(|i| matches!(i, Item::Insert { .. })) {
            errors.push(Error::new(
                ErrorKind::Unsupported,
                None,
                "a transform writes at least one output, with INSERT INTO … SELECT",
            ));
        }
        if !errors.is_empty() {
            return Err(Errors::new(errors));
        }
        Ok(Sql { items })
    }

    /// Plans every statement and view against the streams' schemas. Each
    /// statement that can't be planned gives its errors, and a statement
    /// that reads a view that can't be planned gives none of its own for it.
    pub fn plan(&self, catalog: &Catalog) -> Result<Plan, Errors> {
        let mut errors = Vec::new();
        let mut notes = Vec::new();

        let outputs = self.outputs(catalog, &mut errors, &mut notes);
        let output_names: Vec<&str> = outputs.iter().flatten().map(|(n, _)| n.as_str()).collect();
        let view_names: Vec<(usize, &str, &ast::Ident)> = self
            .items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| match item {
                Item::View { name, ident, .. } => Some((i, name.as_str(), ident)),
                Item::Insert { .. } => None,
            })
            .collect();
        for (_, name, ident) in &view_names {
            let taken = if !catalog.fitting(&[ast::Ident::new(*name)]).is_empty() {
                Some("a source stream's")
            } else if output_names.iter().any(|o| same(o, name)) {
                Some("an output's")
            } else {
                None
            };
            if let Some(whose) = taken {
                errors.push(Error::at(
                    ErrorKind::Name,
                    pos(ident.span),
                    format!("view {name} has {whose} name: a view needs a name of its own"),
                ));
            }
        }

        let mut views: Vec<Defined> = Vec::new();
        let mut statements = Vec::new();
        let mut planned_views = Vec::new();
        let mut output_index = 0;
        for (index, item) in self.items.iter().enumerate() {
            let later: Vec<&str> = view_names
                .iter()
                .filter(|(i, ..)| *i > index)
                .map(|(_, n, _)| *n)
                .collect();
            match item {
                Item::View { name, ident, query } => {
                    let at = pos(ident.span);
                    let mut check = Check::new(catalog, &views, &later, &output_names, at);
                    let mut query = query.clone();
                    let lineage = check.query(&mut query, true);
                    notes.append(&mut check.notes);
                    let mut defined = Defined {
                        name: name.clone(),
                        reads: check.views.clone(),
                        inputs: check.inputs.clone(),
                        nondeterministic: lineage,
                        key: Vec::new(),
                        plan: None,
                    };
                    if check.errors.is_empty() && check.views_planned() {
                        match check.lower(&query, at) {
                            Ok((l, plan)) => {
                                defined.key = l.key.clone();
                                defined.plan = Some(plan);
                                planned_views.push(View {
                                    name: name.clone(),
                                    at,
                                    inputs: defined.inputs.clone(),
                                    schema: l.schema,
                                    key: l.key,
                                });
                            }
                            Err(e) => errors.push(e),
                        }
                    }
                    errors.append(&mut check.errors);
                    views.push(defined);
                }
                Item::Insert { at, query, .. } => {
                    let output = outputs[output_index].clone();
                    output_index += 1;
                    let mut check = Check::new(catalog, &views, &later, &output_names, *at);
                    let mut query = query.clone();
                    check.query(&mut query, true);
                    notes.append(&mut check.notes);
                    if check.errors.is_empty() && check.views_planned() {
                        match check.lower(&query, *at) {
                            Ok((l, _)) => {
                                if let Some((output, replaces)) = output {
                                    statements.push(Statement {
                                        at: *at,
                                        output,
                                        replaces,
                                        class: Class::RowWise,
                                        inputs: check.inputs.clone(),
                                        views: check
                                            .views
                                            .iter()
                                            .map(|&v| views[v].name.clone())
                                            .collect(),
                                        schema: l.schema,
                                        key: l.key,
                                        operator: Operator::RowWise(l.operator),
                                    });
                                }
                            }
                            Err(e) => errors.push(e),
                        }
                    }
                    errors.append(&mut check.errors);
                }
            }
        }
        if !errors.is_empty() {
            return Err(Errors::new(errors));
        }
        notes.sort_by_key(|n| n.at);
        notes.dedup();
        Ok(Plan {
            statements,
            views: planned_views,
            notes,
        })
    }

    /// Each statement's output, and whether it replaces a source stream, in
    /// the order they're written; `None` where it doesn't resolve.
    fn outputs(
        &self,
        catalog: &Catalog,
        errors: &mut Vec<Error>,
        notes: &mut Vec<Note>,
    ) -> Vec<Option<(String, bool)>> {
        let mut outputs: Vec<Option<(String, bool)>> = Vec::new();
        for item in &self.items {
            let Item::Insert { target, .. } = item else {
                continue;
            };
            let at = pos(target.iter().fold(Span::empty(), |s, i| s.union(&i.span)));
            let resolved = output(target, catalog, at, notes);
            let resolved = resolved.and_then(|(name, replaces)| {
                if outputs.iter().flatten().any(|(o, _)| *o == name) {
                    return Err(Error::at(
                        ErrorKind::Name,
                        at,
                        format!("{name} is written by an earlier statement: one statement writes each output"),
                    ));
                }
                Ok((name, replaces))
            });
            match resolved {
                Ok(o) => outputs.push(Some(o)),
                Err(e) => {
                    errors.push(e);
                    outputs.push(None);
                }
            }
        }
        outputs
    }
}

/// What the parser's error says, at the position it names.
fn syntax(e: datafusion_sql::sqlparser::parser::ParserError) -> Errors {
    let message = e.to_string();
    let message = message
        .strip_prefix("sql parser error: ")
        .unwrap_or(&message);
    let (message, at) = match message.rsplit_once(" at Line: ") {
        Some((m, rest)) => {
            let at = rest.split_once(", Column: ").and_then(|(l, c)| {
                Some(Position {
                    line: l.trim().parse().ok()?,
                    column: c.trim().parse().ok()?,
                })
            });
            (m, at)
        }
        None => (message, None),
    };
    Error::new(ErrorKind::Syntax, at, message).into()
}

fn item(s: ast::Statement) -> Result<Item, Error> {
    let at = pos(s.span());
    match s {
        ast::Statement::Insert(insert) => {
            let at = Position::of(insert.insert_token.0.span).unwrap_or(at);
            let unsupported = |what: &str| {
                Error::at(
                    ErrorKind::Unsupported,
                    at,
                    format!("INSERT INTO takes an output's name and a SELECT, without {what}"),
                )
            };
            if !insert.columns.is_empty() {
                return Err(unsupported(
                    "a list of columns: the SELECT names the output's columns",
                ));
            }
            if insert.on.is_some() {
                return Err(unsupported("ON CONFLICT"));
            }
            if insert.returning.is_some() {
                return Err(unsupported("RETURNING"));
            }
            if insert.table_alias.is_some()
                || insert.or.is_some()
                || insert.ignore
                || insert.overwrite
                || !insert.assignments.is_empty()
                || insert.partitioned.is_some()
                || !insert.after_columns.is_empty()
                || insert.insert_alias.is_some()
                || insert.settings.is_some()
                || insert.format_clause.is_some()
            {
                return Err(unsupported("anything else"));
            }
            let target = match &insert.table {
                ast::TableObject::TableName(n) => idents(n)?,
                _ => return Err(unsupported("a table function")),
            };
            let Some(query) = insert.source else {
                return Err(unsupported("a missing SELECT"));
            };
            Ok(Item::Insert {
                at,
                target,
                query: *query,
            })
        }
        ast::Statement::CreateView(v) => {
            let name_at = pos(v.name.span());
            if v.or_alter
                || v.or_replace
                || v.materialized
                || v.secure
                || v.if_not_exists
                || v.temporary
                || v.copy_grants
                || v.with_no_schema_binding
                || v.comment.is_some()
                || v.to.is_some()
                || v.params.is_some()
                || !v.cluster_by.is_empty()
                || !v.columns.is_empty()
                || !matches!(v.options, ast::CreateTableOptions::None)
            {
                return Err(Error::at(
                    ErrorKind::Unsupported,
                    name_at,
                    "CREATE VIEW takes a name and AS SELECT, and nothing else: \
                     the SELECT names the view's columns",
                ));
            }
            let parts = idents(&v.name)?;
            let [ident] = parts.as_slice() else {
                return Err(Error::at(
                    ErrorKind::Name,
                    name_at,
                    format!(
                        "a view's name is one identifier, never qualified: quote it, as \"{}\", if it has dots",
                        parts
                            .iter()
                            .map(|p| p.value.as_str())
                            .collect::<Vec<_>>()
                            .join(".")
                    ),
                ));
            };
            Ok(Item::View {
                name: fold(ident),
                ident: ident.clone(),
                query: *v.query,
            })
        }
        ast::Statement::Query(_) => Err(Error::at(
            ErrorKind::Unsupported,
            at,
            "a SELECT on its own writes nothing: write INSERT INTO output SELECT …, or name it with CREATE VIEW",
        )),
        other => Err(Error::at(
            ErrorKind::Unsupported,
            at,
            format!(
                "a transform's statements are INSERT INTO … SELECT and CREATE VIEW … AS SELECT, not {}",
                keyword(&other)
            ),
        )),
    }
}

/// The first words of a statement, for messages.
fn keyword(s: &ast::Statement) -> String {
    s.to_string()
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}

fn idents(n: &ast::ObjectName) -> Result<Vec<ast::Ident>, Error> {
    n.0.iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(i) => Ok(i.clone()),
            other => Err(Error::at(
                ErrorKind::Unsupported,
                pos(other.span()),
                "a name is identifiers separated by dots",
            )),
        })
        .collect()
}

/// An identifier as SQL folds it: unquoted ones to lower case.
fn fold(i: &ast::Ident) -> String {
    match i.quote_style {
        Some(_) => i.value.clone(),
        None => i.value.to_lowercase(),
    }
}

fn same(a: &str, b: &str) -> bool {
    a.to_lowercase() == b.to_lowercase()
}

fn pos(span: Span) -> Position {
    Position::of(span).unwrap_or_default()
}

/// The qualifier a name starts with: an unquoted `source` or `destination`
/// before at least one more part.
fn qualifier(parts: &[ast::Ident]) -> Option<&'static str> {
    let [first, _, ..] = parts else {
        return None;
    };
    if first.quote_style.is_some() {
        return None;
    }
    ["source", "destination"]
        .into_iter()
        .find(|q| first.value.eq_ignore_ascii_case(q))
}

/// The output an `INSERT INTO` writes, and whether it replaces a stream.
fn output(
    target: &[ast::Ident],
    catalog: &Catalog,
    at: Position,
    notes: &mut Vec<Note>,
) -> Result<(String, bool), Error> {
    let name_error = |m: String| Error::at(ErrorKind::Name, at, m);
    let rest = match qualifier(target) {
        Some("source") => {
            return Err(name_error(format!(
                "source. names a source's stream, which INSERT INTO doesn't write: write INSERT INTO {} or INSERT INTO destination.{}",
                written(&target[1..]),
                written(&target[1..]),
            )));
        }
        Some(_) => {
            if let [only] = catalog.fitting(target).as_slice() {
                notes.push(Note {
                    at,
                    message: format!(
                        "destination. is a qualifier here, so this writes {}; the source's stream {only} is written as INSERT INTO \"{only}\"",
                        written(&target[1..])
                    ),
                });
            }
            &target[1..]
        }
        None => target,
    };
    match catalog.fitting(rest).as_slice() {
        [stream] => Ok(((*stream).to_owned(), true)),
        [] => match rest {
            [one] => Ok((fold(one), false)),
            _ => Err(name_error(format!(
                "{} isn't a source's stream, and a new output's name is one identifier: quote it, as \"{}\", if it has dots",
                written(rest),
                rest.iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".")
            ))),
        },
        several => Err(name_error(ambiguous(rest, several))),
    }
}

fn ambiguous(parts: &[ast::Ident], streams: &[&str]) -> String {
    format!(
        "{} fits the streams {}, which differ only by case: quote the name to pick one",
        written(parts),
        streams.join(" and ")
    )
}

/// A view defined so far.
#[derive(Debug)]
struct Defined {
    name: String,
    /// The views it reads, directly or not, by index.
    reads: BTreeSet<usize>,
    inputs: Vec<String>,
    nondeterministic: HashSet<String>,
    key: Vec<String>,
    /// What binding gave, which is inlined into each plan that reads it.
    /// `None` if it didn't plan, and then what reads it isn't planned
    /// either.
    plan: Option<LogicalPlan>,
}

/// The names of a query's output columns that a function that isn't
/// deterministic feeds, as SQL folds them.
type Lineage = HashSet<String>;

const WINDOWS: [&str; 3] = ["tumble", "hop", "session"];

/// Aggregates, which a later tier runs.
const AGGREGATES: &[&str] = &[
    "any_value",
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "array_agg",
    "avg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "corr",
    "count",
    "covar",
    "covar_pop",
    "covar_samp",
    "every",
    "grouping",
    "max",
    "mean",
    "median",
    "min",
    "percentile_cont",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "sum",
    "var",
    "var_pop",
    "var_samp",
    "variance",
];

/// Functions that only run over a window, which a later tier runs.
const WINDOW_FUNCTIONS: &[&str] = &[
    "cume_dist",
    "dense_rank",
    "first_value",
    "lag",
    "last_value",
    "lead",
    "nth_value",
    "ntile",
    "percent_rank",
    "rank",
    "row_number",
];

/// Checks a statement's or a view's query before binding, resolves the
/// names it reads, and rewrites them for binding: a stream as its whole name,
/// a view as itself.
struct Check<'a> {
    catalog: &'a Catalog,
    defined: &'a [Defined],
    later: &'a [&'a str],
    outputs: &'a [&'a str],
    at: Position,
    errors: Vec<Error>,
    notes: Vec<Note>,
    later_tiers: HashSet<Class>,
    inputs: Vec<String>,
    views: BTreeSet<usize>,
    /// The outermost SELECT, to say where it gives a column or loses a key.
    top: Option<Top>,
    /// A name FROM read under more than one part, and the alias it was
    /// given, for the SELECT that reads it to qualify its columns by.
    qualified_as: Option<(Vec<ast::Ident>, ast::Ident)>,
}

struct Top {
    at: Position,
    items: Vec<ast::SelectItem>,
    /// The key of the stream or view it reads; `None` for a subquery.
    key: Option<Vec<String>>,
}

impl<'a> Check<'a> {
    fn new(
        catalog: &'a Catalog,
        defined: &'a [Defined],
        later: &'a [&'a str],
        outputs: &'a [&'a str],
        at: Position,
    ) -> Self {
        Check {
            catalog,
            defined,
            later,
            outputs,
            at,
            errors: Vec::new(),
            notes: Vec::new(),
            later_tiers: HashSet::new(),
            inputs: Vec::new(),
            views: BTreeSet::new(),
            top: None,
            qualified_as: None,
        }
    }

    fn at(&self, span: Span) -> Position {
        Position::of(span).unwrap_or(self.at)
    }

    fn unsupported(&mut self, span: Span, message: impl Into<String>) {
        let at = self.at(span);
        self.errors
            .push(Error::at(ErrorKind::Unsupported, at, message));
    }

    /// One error per class a statement needs a later tier for.
    fn later_tier(&mut self, class: Class, span: Span, what: &str) {
        if self.later_tiers.insert(class) {
            let at = self.at(span);
            self.errors.push(Error::at(
                ErrorKind::LaterTier(class),
                at,
                format!(
                    "{what}: transforms don't run {} yet (tier {})",
                    class.description(),
                    class.tier()
                ),
            ));
        }
    }

    fn views_planned(&self) -> bool {
        self.views.iter().all(|&v| self.defined[v].plan.is_some())
    }

    fn query(&mut self, q: &mut ast::Query, top: bool) -> Lineage {
        if let Some(w) = &q.with {
            self.unsupported(
                w.with_token.0.span,
                "WITH isn't supported: name the query with CREATE VIEW, before the statements that read it",
            );
            return Lineage::new();
        }
        if let Some(o) = &q.order_by {
            let span = match &o.kind {
                ast::OrderByKind::Expressions(e) => {
                    e.first().map_or(Span::empty(), |e| e.expr.span())
                }
                ast::OrderByKind::All(_) => Span::empty(),
            };
            self.unsupported(span, "a stream has no order, so ORDER BY is refused");
        }
        if let Some(l) = &q.limit_clause {
            self.unsupported(
                l.span(),
                "a stream has no end, so LIMIT and OFFSET are refused",
            );
        }
        if q.fetch.is_some() {
            self.unsupported(Span::empty(), "a stream has no end, so FETCH is refused");
        }
        if !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            self.unsupported(
                q.span(),
                "a statement is a SELECT, without locks, settings, formats or pipes",
            );
        }
        match q.body.as_mut() {
            ast::SetExpr::Select(s) => self.select(s, top),
            ast::SetExpr::Query(inner) => self.query(inner, top),
            ast::SetExpr::SetOperation { right, .. } => {
                self.unsupported(right.span(), "UNION, INTERSECT and EXCEPT aren't supported");
                Lineage::new()
            }
            ast::SetExpr::Values(v) => {
                self.unsupported(v.span(), "a statement reads a stream or a view, not VALUES");
                Lineage::new()
            }
            other => {
                self.unsupported(other.span(), "a statement is a SELECT");
                Lineage::new()
            }
        }
    }

    fn select(&mut self, s: &mut ast::Select, top: bool) -> Lineage {
        let select_span = s.select_token.0.span;
        if s.top.is_some()
            || s.into.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.connect_by.is_empty()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
        {
            self.unsupported(select_span, "this SELECT has clauses transforms don't run");
        }

        let mut from = Lineage::new();
        let mut from_key = None;
        match s.from.as_mut_slice() {
            [] => self.unsupported(
                select_span,
                "a SELECT reads a stream or a view: it needs a FROM",
            ),
            [first, others @ ..] => {
                if let Some(o) = others.first() {
                    self.later_tier(Class::Join, o.relation.span(), "reading two relations");
                }
                if let Some(j) = first.joins.first() {
                    self.later_tier(Class::Join, j.relation.span(), "a join");
                }
                (from, from_key) = self.relation(&mut first.relation);
                if let Some((written, alias)) = self.qualified_as.take() {
                    unqualify(s, &written, &alias);
                }
            }
        }

        // Aggregates, windows and subqueries, wherever they are.
        let before = self.later_tiers.len();
        let mut aggregate = None;
        let mut over_specs = Vec::new();
        let mut subquery = None;
        let mut window_only = None;
        {
            let mut look = |e: &ast::Expr| {
                match e {
                    ast::Expr::Function(f) => {
                        let name = function_name(f);
                        match &f.over {
                            Some(w) => over_specs.push((f.span(), w.clone())),
                            None if AGGREGATES.contains(&name.as_str()) => {
                                aggregate.get_or_insert(f.span());
                            }
                            None if WINDOW_FUNCTIONS.contains(&name.as_str()) => {
                                window_only.get_or_insert(f.span());
                            }
                            None => {}
                        }
                    }
                    ast::Expr::Subquery(_)
                    | ast::Expr::InSubquery { .. }
                    | ast::Expr::Exists { .. } => {
                        subquery.get_or_insert(e.span());
                    }
                    _ => {}
                }
                ControlFlow::<()>::Continue(())
            };
            let _ = ast::visit_expressions(&s.projection, &mut look);
            let _ = ast::visit_expressions(&s.selection, &mut look);
            let _ = ast::visit_expressions(&s.having, &mut look);
        }
        if let Some(span) = subquery {
            self.later_tier(Class::Join, span, "a subquery");
        }
        for (span, _) in &over_specs {
            self.later_tier(Class::Over, *span, "OVER");
        }
        if let Some(span) = window_only {
            self.later_tier(Class::Over, span, "a window function");
        }
        if let Some(w) = s.named_window.first() {
            self.later_tier(Class::Over, w.0.span, "WINDOW");
        }
        if let Some(q) = &s.qualify {
            self.later_tier(Class::Over, q.span(), "QUALIFY");
        }
        for (_, w) in &over_specs {
            if let ast::WindowType::WindowSpec(spec) = w {
                self.deterministic(&spec.partition_by, &from, "a window's partition");
                let order: Vec<ast::Expr> = spec.order_by.iter().map(|o| o.expr.clone()).collect();
                self.deterministic(&order, &from, "a window's order");
            }
        }
        for w in &s.named_window {
            if let ast::NamedWindowExpr::WindowSpec(spec) = &w.1 {
                self.deterministic(&spec.partition_by, &from, "a window's partition");
            }
        }
        match &s.group_by {
            ast::GroupByExpr::All(_) => {
                self.later_tier(Class::Updating, select_span, "GROUP BY ALL");
            }
            ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                let window = exprs.iter().find(|e| {
                    matches!(e, ast::Expr::Function(f) if WINDOWS.contains(&function_name(f).as_str()))
                });
                match window {
                    Some(w) => self.later_tier(Class::GroupWindow, w.span(), "a group window"),
                    None => self.later_tier(
                        Class::Updating,
                        exprs[0].span(),
                        "GROUP BY without a window",
                    ),
                }
                let exprs = exprs.clone();
                self.deterministic(&exprs, &from, "a group's key");
            }
            ast::GroupByExpr::Expressions(..) => {
                if let Some(span) = aggregate {
                    self.later_tier(Class::Updating, span, "an aggregate without a window");
                } else if let Some(h) = &s.having {
                    self.later_tier(Class::Updating, h.span(), "HAVING without a window");
                }
            }
        }
        if s.distinct.is_some() {
            self.later_tier(Class::Updating, select_span, "DISTINCT");
        }
        let later = self.later_tiers.len() > before;

        let mut lineage = Lineage::new();
        let mut named: Vec<String> = Vec::new();
        for item in &s.projection {
            let name = match item {
                ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(i)) => Some(fold(i)),
                ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(p)) => {
                    p.last().map(fold)
                }
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(fold(alias)),
                _ => None,
            };
            if let Some(name) = name {
                if named.contains(&name) {
                    let at = self.at(item.span());
                    self.errors.push(Error::at(
                        ErrorKind::Bind,
                        at,
                        format!(
                            "there's already a column {name}: give this one another name with AS"
                        ),
                    ));
                }
                named.push(name);
            }
        }
        for item in &s.projection {
            match item {
                ast::SelectItem::UnnamedExpr(e) => match e {
                    ast::Expr::Identifier(i) => {
                        if from.contains(&fold(i)) {
                            lineage.insert(fold(i));
                        }
                    }
                    ast::Expr::CompoundIdentifier(parts) => {
                        if let Some(last) = parts.last()
                            && from.contains(&fold(last))
                        {
                            lineage.insert(fold(last));
                        }
                    }
                    _ if later => {}
                    _ => self.unsupported(e.span(), format!("name this column: {e} AS name")),
                },
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    if self.feeds_nondeterministic(expr, &from).is_some() {
                        lineage.insert(fold(alias));
                    }
                }
                ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..) => {
                    lineage.extend(from.iter().cloned());
                }
                other => self.unsupported(other.span(), "this SELECT item isn't supported"),
            }
        }
        if top {
            self.top = Some(Top {
                at: self.at(select_span),
                items: s.projection.clone(),
                key: from_key,
            });
        }
        lineage
    }

    /// Refuses a function that isn't deterministic in expressions that feed
    /// a key or a window, directly or through a column of what's read.
    fn deterministic(&mut self, exprs: &[ast::Expr], from: &Lineage, what: &str) {
        for e in exprs {
            if let Some((span, name)) = self.feeds_nondeterministic(e, from) {
                let at = self.at(span);
                self.errors.push(Error::at(
                    ErrorKind::NotDeterministic,
                    at,
                    format!("{name} isn't deterministic, so it can't feed {what}"),
                ));
            }
        }
    }

    /// Where an expression calls a function that isn't deterministic, or
    /// reads a column that one feeds.
    fn feeds_nondeterministic(&self, e: &ast::Expr, from: &Lineage) -> Option<(Span, String)> {
        let mut found = None;
        let _ = ast::visit_expressions(e, |e| {
            let hit = match e {
                ast::Expr::Function(f) if !FUNCTIONS.deterministic(&function_name(f)) => {
                    Some(format!("{}()", function_name(f)))
                }
                ast::Expr::Identifier(i) if from.contains(&fold(i)) => Some(i.to_string()),
                ast::Expr::CompoundIdentifier(parts)
                    if parts.last().is_some_and(|l| from.contains(&fold(l))) =>
                {
                    Some(written(parts))
                }
                _ => None,
            };
            match hit {
                Some(name) => {
                    found = Some((e.span(), name));
                    ControlFlow::Break(())
                }
                None => ControlFlow::Continue(()),
            }
        });
        found
    }

    /// Resolves what FROM reads: what feeds its columns that isn't
    /// deterministic, and its key if it's a stream or a view.
    fn relation(&mut self, t: &mut ast::TableFactor) -> (Lineage, Option<Vec<String>>) {
        let span = t.span();
        match t {
            ast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some() {
                    self.unsupported(span, "table functions aren't supported");
                    return (Lineage::new(), None);
                }
                if !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                    || sample.is_some()
                    || !index_hints.is_empty()
                {
                    self.unsupported(span, "FROM takes a stream's or a view's name, and an alias");
                    return (Lineage::new(), None);
                }
                self.table(name, alias)
            }
            ast::TableFactor::Derived {
                lateral, subquery, ..
            } => {
                if *lateral {
                    self.later_tier(Class::Join, span, "LATERAL");
                    return (Lineage::new(), None);
                }
                (self.query(subquery, false), None)
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                if let Some(j) = table_with_joins.joins.first() {
                    self.later_tier(Class::Join, j.relation.span(), "a join");
                    return (Lineage::new(), None);
                }
                self.relation(&mut table_with_joins.relation)
            }
            ast::TableFactor::UNNEST { .. } => {
                self.unsupported(
                    span,
                    "UNNEST goes in the SELECT list: SELECT unnest(list) AS item FROM …",
                );
                (Lineage::new(), None)
            }
            _ => {
                self.unsupported(span, "FROM takes a stream's or a view's name");
                (Lineage::new(), None)
            }
        }
    }

    fn table(
        &mut self,
        name: &mut ast::ObjectName,
        alias: &mut Option<ast::TableAlias>,
    ) -> (Lineage, Option<Vec<String>>) {
        let span = name.span();
        let at = self.at(span);
        let parts = match idents(name) {
            Ok(p) => p,
            Err(e) => {
                self.errors.push(e);
                return (Lineage::new(), None);
            }
        };
        let Some(written_as) = parts.last().cloned() else {
            return (Lineage::new(), None);
        };
        let name_error = |m: String| Error::at(ErrorKind::Name, at, m);
        let rest = match qualifier(&parts) {
            Some("destination") => {
                let mut m = format!(
                    "destination. names an output, and FROM reads a source's stream or a view: write FROM {}",
                    written(&parts[1..])
                );
                if let [only] = self.catalog.fitting(&parts).as_slice() {
                    m.push_str(&format!(", or FROM \"{only}\" for the stream {only}"));
                }
                self.errors.push(name_error(m));
                return (Lineage::new(), None);
            }
            Some(_) => &parts[1..],
            None => &parts[..],
        };
        let qualified = rest.len() < parts.len();

        if !qualified && let [one] = rest {
            let folded = fold(one);
            if let Some(v) = self.defined.iter().position(|d| d.name == folded) {
                let d = &self.defined[v];
                self.views.insert(v);
                self.views.extend(d.reads.iter().copied());
                for i in &d.inputs {
                    if !self.inputs.contains(i) {
                        self.inputs.push(i.clone());
                    }
                }
                rewrite(name, alias, &d.name, &written_as);
                let key = d.plan.is_some().then(|| d.key.clone());
                return (d.nondeterministic.clone(), key);
            }
            if self.later.contains(&folded.as_str()) {
                self.errors.push(name_error(format!(
                    "view {folded} is defined after this statement: define a view before the statements that read it"
                )));
                return (Lineage::new(), None);
            }
        }

        match self.catalog.fitting(rest).as_slice() {
            [stream] => {
                let stream = (*stream).to_owned();
                if qualified
                    && let [other] = self.catalog.fitting(&parts).as_slice()
                    && *other != stream
                {
                    self.notes.push(Note {
                        at,
                        message: format!(
                            "source. is a qualifier here, so this reads the stream {stream}; the stream {other} is read as FROM \"{other}\""
                        ),
                    });
                }
                if !self.inputs.contains(&stream) {
                    self.inputs.push(stream.clone());
                }
                if alias.is_none() && parts.len() > 1 {
                    self.qualified_as = Some((parts.clone(), written_as.clone()));
                }
                rewrite(name, alias, &stream, &written_as);
                let key = self.catalog.get(&stream).map(|s| s.key.clone());
                (Lineage::new(), key)
            }
            [] => {
                let w = written(rest);
                let m = if qualified
                    && rest.len() == 1
                    && self.defined.iter().any(|d| d.name == fold(&rest[0]))
                {
                    format!("{w} is a view, and views take no qualifier: write FROM {w}")
                } else if qualified && let [only] = self.catalog.fitting(&parts).as_slice() {
                    format!(
                        "source. is a qualifier here, and the source has no stream {w}: write FROM \"{only}\" for the stream {only}"
                    )
                } else if !qualified && self.outputs.iter().any(|o| same(o, &w)) {
                    format!(
                        "{w} is an output, and FROM reads a source's stream or a view: name the SELECT that writes it with CREATE VIEW, and read that"
                    )
                } else {
                    format!("there's no stream or view {w}")
                };
                self.errors.push(name_error(m));
                (Lineage::new(), None)
            }
            several => {
                let m = ambiguous(rest, several);
                self.errors.push(name_error(m));
                (Lineage::new(), None)
            }
        }
    }

    /// Binds the query with the views it reads inlined, lowers it, and
    /// refuses it if it loses a changelog's key.
    fn lower(
        &mut self,
        query: &ast::Query,
        at: Position,
    ) -> Result<(lower::Lowered, LogicalPlan), Error> {
        let views: Vec<(&str, &LogicalPlan)> = self
            .views
            .iter()
            .filter_map(|&v| {
                let d = &self.defined[v];
                d.plan.as_ref().map(|p| (d.name.as_str(), p))
            })
            .collect();
        let column = |name: &str| self.column(name).unwrap_or(at);
        let plan = bind::plan(query, self.catalog, &views, at, &column)?;
        let lowered = lower::row_wise(&plan, self.catalog, at)?;
        if lowered.loses_changelog_key {
            return Err(self.changelog_key(&lowered));
        }
        Ok((lowered, plan))
    }

    /// Where the outermost SELECT gives a column, by name.
    fn column(&self, name: &str) -> Option<Position> {
        let top = self.top.as_ref()?;
        top.items.iter().find_map(|item| {
            let (span, given) = match item {
                ast::SelectItem::ExprWithAlias { expr, alias } => (expr.span(), fold(alias)),
                ast::SelectItem::UnnamedExpr(e @ ast::Expr::Identifier(i)) => (e.span(), fold(i)),
                ast::SelectItem::UnnamedExpr(e @ ast::Expr::CompoundIdentifier(p)) => {
                    (e.span(), p.last().map(fold).unwrap_or_default())
                }
                _ => return None,
            };
            (given == name).then(|| Position::of(span)).flatten()
        })
    }

    /// Where the outermost SELECT drops or changes a changelog's key.
    fn changelog_key(&self, lowered: &lower::Lowered) -> Error {
        let input = &lowered.operator.input;
        let key = self
            .catalog
            .get(input)
            .map(|s| s.key.join(", "))
            .unwrap_or_default();
        let mut at = self.at;
        let mut how = "drops or changes";
        if let Some(top) = &self.top {
            at = top.at;
            how = "drops";
            let key = top.key.clone().unwrap_or_default();
            let names: Vec<String> = key.iter().map(|k| k.to_lowercase()).collect();
            let is_key = |i: &ast::Ident| names.contains(&fold(i).to_lowercase());
            for item in &top.items {
                let (ast::SelectItem::UnnamedExpr(e)
                | ast::SelectItem::ExprWithAlias { expr: e, .. }) = item
                else {
                    continue;
                };
                let bare = match e {
                    ast::Expr::Identifier(i) => is_key(i),
                    ast::Expr::CompoundIdentifier(p) => p.last().is_some_and(is_key),
                    _ => false,
                };
                let mut unnest = false;
                let mut reads_key = false;
                let _ = ast::visit_expressions(e, |e| {
                    match e {
                        ast::Expr::Function(f) if function_name(f).starts_with("unnest") => {
                            unnest = true
                        }
                        ast::Expr::Identifier(i) if is_key(i) => reads_key = true,
                        ast::Expr::CompoundIdentifier(p) if p.last().is_some_and(is_key) => {
                            reads_key = true
                        }
                        _ => {}
                    }
                    ControlFlow::<()>::Continue(())
                });
                if unnest {
                    at = Position::of(e.span()).unwrap_or(at);
                    how = "multiplies the rows of";
                    break;
                }
                if reads_key && !bare {
                    at = Position::of(e.span()).unwrap_or(at);
                    how = "changes";
                    break;
                }
            }
        }
        Error::at(
            ErrorKind::ChangelogKey,
            at,
            format!(
                "{input} is a changelog, so what reads it keeps its key ({key}) unchanged, renamed or not; this {how} it"
            ),
        )
    }
}

/// Qualifies a SELECT's columns and wildcards by `alias` where they're
/// qualified by the whole name FROM read, as `public.orders.id` or
/// `source.customers.*`.
fn unqualify(s: &mut ast::Select, written: &[ast::Ident], alias: &ast::Ident) {
    let fits = |p: &[ast::Ident]| {
        p.len() == written.len() && p.iter().zip(written).all(|(a, b)| same(&a.value, &b.value))
    };
    for item in &mut s.projection {
        if let ast::SelectItem::QualifiedWildcard(
            ast::SelectItemQualifiedWildcardKind::ObjectName(n),
            _,
        ) = item
            && idents(n).is_ok_and(|p| fits(&p))
        {
            *n = ast::ObjectName(vec![ast::ObjectNamePart::Identifier(alias.clone())]);
        }
    }
    let mut replace = |e: &mut ast::Expr| {
        if let ast::Expr::CompoundIdentifier(p) = e
            && p.len() > written.len()
            && fits(&p[..written.len()])
        {
            let rest = p.split_off(written.len());
            *p = std::iter::once(alias.clone()).chain(rest).collect();
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = ast::visit_expressions_mut(&mut s.projection, &mut replace);
    let _ = ast::visit_expressions_mut(&mut s.selection, &mut replace);
}

/// Points a name at what it resolved to, keeping the name it was written
/// with as its alias.
fn rewrite(
    name: &mut ast::ObjectName,
    alias: &mut Option<ast::TableAlias>,
    to: &str,
    written_as: &ast::Ident,
) {
    let span = name.span();
    *name = ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident {
        value: to.to_owned(),
        quote_style: Some('"'),
        span,
    })]);
    if alias.is_none() {
        *alias = Some(ast::TableAlias {
            explicit: true,
            name: written_as.clone(),
            columns: Vec::new(),
            at: None,
        });
    }
}

fn function_name(f: &ast::Function) -> String {
    f.name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(fold)
        .unwrap_or_default()
}
