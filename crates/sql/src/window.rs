//! Group windows. `GROUP BY tumble(time, width)` names a window, and
//! `window_start` and `window_end` in the statement name its bounds.
//! DataFusion binds neither name, so the statement is rewritten before
//! binding: each becomes the call, which binds to [`Tumble`], a scalar
//! function giving a row's window start.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray, cast::AsArray};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_sql::sqlparser::ast::{self, Spanned as _};

use crate::{Error, Position};

pub(crate) const TUMBLE: &str = "tumble";
const START: &str = "window_start";
const END: &str = "window_end";

/// Rewrites a windowed statement's `window_start` and `window_end` as calls
/// of its window's function.
pub(crate) fn rewrite(query: &mut ast::Query) -> Result<(), Error> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Ok(());
    };
    let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by else {
        return Ok(());
    };
    let mut window: Option<(ast::Expr, ast::Expr)> = None;
    for e in exprs {
        let ast::Expr::Function(f) = e else { continue };
        let at = position(e);
        match crate::object_name(&f.name).as_str() {
            TUMBLE if window.is_some() => {
                return Err(Error::refused(at, "a statement groups by one window"));
            }
            TUMBLE => {
                let width = match &f.args {
                    ast::FunctionArguments::List(l) if l.args.len() == 2 => match &l.args[1] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(w)) => w.clone(),
                        _ => return Err(Error::refused(at, "tumble(time, width)")),
                    },
                    _ => return Err(Error::refused(at, "tumble(time, width)")),
                };
                window = Some((e.clone(), width));
            }
            "hop" | "session" => {
                return Err(Error::refused(
                    at,
                    "hop and session windows aren't supported yet",
                ));
            }
            _ => {}
        }
    }
    let Some((start, width)) = window else {
        return Ok(());
    };
    let end = ast::Expr::BinaryOp {
        left: Box::new(start.clone()),
        op: ast::BinaryOperator::Plus,
        right: Box::new(width),
    };
    let bound = |id: &ast::Ident| match id.value.to_lowercase().as_str() {
        START if id.quote_style.is_none() => Some(&start),
        END if id.quote_style.is_none() => Some(&end),
        _ => None,
    };
    for item in &mut select.projection {
        if let ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(id)) = item
            && let Some(e) = bound(id)
        {
            *item = ast::SelectItem::ExprWithAlias {
                expr: e.clone(),
                alias: ast::Ident::new(id.value.to_lowercase()),
            };
        }
    }
    let replace = |e: &mut ast::Expr| {
        if let ast::Expr::Identifier(id) = e
            && let Some(b) = bound(id)
        {
            *e = b.clone();
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = ast::visit_expressions_mut(&mut select.projection, replace);
    let _ = ast::visit_expressions_mut(&mut select.having, replace);
    if let Some(o) = &mut query.order_by {
        let _ = ast::visit_expressions_mut(o, replace);
    }
    Ok(())
}

pub(crate) fn position(e: &ast::Expr) -> Position {
    let s = e.span().start;
    Position {
        line: s.line,
        column: s.column,
    }
}

/// `tumble(time, width)`: the start of the window of `width` holding `time`,
/// counted from the Unix epoch.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Tumble {
    signature: Signature,
}

pub(crate) fn tumble() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Tumble {
        signature: Signature::any(2, Volatility::Immutable),
    }))
}

/// A window's width in microseconds, from an interval without months.
pub(crate) fn width(v: &ScalarValue) -> Result<i64> {
    let ScalarValue::IntervalMonthDayNano(Some(i)) = v else {
        return plan_err!("a window's width is an interval, such as INTERVAL '1 minute'");
    };
    if i.months != 0 {
        return plan_err!("a window's width can't be in months");
    }
    let micros = i64::from(i.days)
        .checked_mul(86_400_000_000)
        .and_then(|d| d.checked_add(i.nanoseconds / 1000));
    match micros {
        Some(m) if m > 0 && i.nanoseconds % 1000 == 0 => Ok(m),
        _ => plan_err!("a window's width is a positive number of microseconds"),
    }
}

fn starts<T: ArrowTimestampType>(time: &ArrayRef, width: i64) -> ArrayRef {
    let t: &PrimitiveArray<T> = time.as_primitive();
    let tz = match t.data_type() {
        DataType::Timestamp(_, tz) => tz.clone(),
        _ => None,
    };
    Arc::new(
        t.unary::<_, T>(|v| v - v.rem_euclid(width))
            .with_timezone_opt(tz),
    )
}

impl ScalarUDFImpl for Tumble {
    fn name(&self) -> &str {
        TUMBLE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [
                t @ DataType::Timestamp(..),
                DataType::Interval(IntervalUnit::MonthDayNano),
            ] => Ok(t.clone()),
            _ => plan_err!("tumble takes a timestamp and an interval"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [time, ColumnarValue::Scalar(w)] = args.args.as_slice() else {
            return exec_err!("tumble's width is a constant");
        };
        let micros = width(w)?;
        let time = time.to_array(args.number_rows)?;
        let out = match time.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) if micros % 1_000_000 == 0 => {
                starts::<TimestampSecondType>(&time, micros / 1_000_000)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) if micros % 1000 == 0 => {
                starts::<TimestampMillisecondType>(&time, micros / 1000)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                starts::<TimestampMicrosecondType>(&time, micros)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => match micros.checked_mul(1000) {
                Some(n) => starts::<TimestampNanosecondType>(&time, n),
                None => return exec_err!("the window is too wide for nanoseconds"),
            },
            t => return exec_err!("tumble can't take {t} with this width"),
        };
        Ok(ColumnarValue::Array(out))
    }
}
